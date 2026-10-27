"""Entry point:  schema_pick -> pick_mysql_changes -> upsert

    python main.py
"""
import logging
import os

_HERE = os.path.dirname(os.path.abspath(__file__))

REQUIRED_ENV = (
    "MYSQL_HOST",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_DATABASE",
    "PG_HOST",
    "PG_USER",
    "PG_PASSWORD",
    "PG_DATABASE",
)


def load_dotenv_file(path=None):
    """Load KEY=VALUE lines into os.environ without overriding existing vars.

    Docker Compose injects env_file already. `docker run` / `docker exec` +
    a copied .env file does not, which is why MYSQL_HOST was missing.
    """
    path = path or os.path.join(_HERE, ".env")
    if not os.path.isfile(path):
        return path
    with open(path, encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            if key:
                os.environ.setdefault(key, value)
    return path


def _normalize_tz(tz):
    """Flink/Java ZoneId: UTC+5 -> UTC+05:00."""
    tz = (tz or "UTC").strip()
    if tz.upper().startswith("UTC") and len(tz) > 3 and tz[3] in "+-":
        sign = tz[3]
        rest = tz[4:].strip()
        if rest.isdigit():
            return f"UTC{sign}{int(rest):02d}:00"
        if len(rest) == 4 and rest.isdigit():
            return f"UTC{sign}{rest[:2]}:{rest[2:]}"
    return tz


def load_config():
    load_dotenv_file()
    e = os.environ
    missing = [k for k in REQUIRED_ENV if not e.get(k)]
    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
            + ". Set them in the container environment or in .env next to main.py."
        )
    cdc_jar = e.get("CDC_JAR") or "/opt/flink/usrlib/flink-sql-connector-mysql-cdc.jar"
    return {
        "mysql": {
            "host": e["MYSQL_HOST"],
            "port": int(e.get("MYSQL_PORT", "3306")),
            "user": e["MYSQL_USER"],
            "password": e["MYSQL_PASSWORD"],
            "database": e["MYSQL_DATABASE"],
            # Must match the MySQL/RDS server time zone, otherwise TIMESTAMP values shift.
            "timezone": _normalize_tz(e.get("MYSQL_SERVER_TZ", "UTC")),
        },
        "pg": {
            "host": e["PG_HOST"],
            "port": int(e.get("PG_PORT", "5432")),
            "user": e["PG_USER"],
            "password": e["PG_PASSWORD"],
            "dbname": e["PG_DATABASE"],
            "schema": e.get("PG_SCHEMA", "warehouse"),
        },
        "db_id": int(e.get("DB_ID", "2")),
        # Empty = every base table that has a primary key in MYSQL_DATABASE.
        "tables": [t.strip() for t in e.get("MYSQL_TABLES", "").split(",") if t.strip()],
        "cdc_jar": cdc_jar,
        "mysql_jdbc_jar": e.get("MYSQL_JDBC_JAR") or "/opt/flink/usrlib/mysql-connector-j.jar",
        # latest-offset only: no snapshot, so MySQL tables are not locked.
        "startup_mode": "latest-offset",
        # Each table gets its own binlog client -> needs its own server-id (base, base+1, ...).
        "server_id_base": int(e.get("MYSQL_SERVER_ID_BASE", "5400")),
        "snapshot_path": e.get("SCHEMA_SNAPSHOT_PATH", "schema_snapshot.json"),
        "checkpoint_dir": e.get("CHECKPOINT_DIR"),  # e.g. file:///var/flink/checkpoints
    }


def _require_table_list(cfg):
    if cfg["tables"]:
        return
    if os.environ.get("ALLOW_ALL_TABLES", "").lower() in ("1", "true", "yes"):
        return
    raise RuntimeError(
        "MYSQL_TABLES is empty. Set it to a comma-separated list "
        "(e.g. transaction,operator,product) or ALLOW_ALL_TABLES=true."
    )


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def run():
    import schema_pick
    import pick_mysql_changes
    import upsert

    setup_logging()
    cfg = load_config()
    _require_table_list(cfg)

    # 1) Runs on EVERY start: read current MySQL columns/types (for Flink), log diffs. No Postgres DDL.
    schema = schema_pick.pick_schema(cfg["mysql"], cfg["tables"])
    schema_pick.diff_and_save(schema, cfg["snapshot_path"])

    # 2) Keep tables that already exist in the warehouse with dbId + MySQL PK columns. No DDL.
    upsert.filter_targets(cfg["pg"], schema)

    # 3) Flink reads binlog from now (latest-offset) -> 4) fill overlapping columns; match on MySQL PK + dbId.
    env, changes = pick_mysql_changes.build_change_stream(cfg, schema)
    keys = {t: upsert.conflict_key(info) for t, info in schema["tables"].items()}
    changes.flat_map(upsert.PostgresUpserter(cfg["pg"], cfg["db_id"], keys)).name("postgres-upsert")
    env.execute("mysql-flink-cdc-to-postgres")


if __name__ == "__main__":
    run()

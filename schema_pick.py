"""Reads table/column/type info from MySQL on every start and logs schema changes
compared with the previous run.

    python schema_pick.py        # prints the schema it picked
"""
import json
import logging
import os

import pymysql

log = logging.getLogger("schema_pick")

COLUMNS_SQL = """
SELECT c.TABLE_NAME        AS table_name,
       c.COLUMN_NAME       AS column_name,
       c.DATA_TYPE         AS data_type,
       c.COLUMN_TYPE       AS column_type,
       c.IS_NULLABLE       AS is_nullable,
       c.NUMERIC_PRECISION AS num_precision,
       c.NUMERIC_SCALE     AS num_scale,
       c.DATETIME_PRECISION AS dt_precision
FROM information_schema.COLUMNS c
JOIN information_schema.TABLES t
  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_SCHEMA = %s AND t.TABLE_TYPE = 'BASE TABLE'
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
"""

PK_SQL = """
SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = %s AND CONSTRAINT_NAME = 'PRIMARY'
ORDER BY TABLE_NAME, ORDINAL_POSITION
"""


def to_flink_type(r):
    """MySQL column -> Flink SQL type."""
    dt = r["data_type"].lower()
    ct = r["column_type"].lower()
    unsigned = "unsigned" in ct
    prec = r["num_precision"]
    scale = r["num_scale"] or 0
    dtp = r["dt_precision"] or 0

    if dt == "tinyint":
        if ct.startswith("tinyint(1)") and not unsigned:
            return "BOOLEAN"
        return "SMALLINT" if unsigned else "TINYINT"
    if dt == "smallint":
        return "INT" if unsigned else "SMALLINT"
    if dt == "mediumint":
        return "INT"
    if dt in ("int", "integer"):
        return "BIGINT" if unsigned else "INT"
    if dt == "bigint":
        return "DECIMAL(20, 0)" if unsigned else "BIGINT"
    if dt in ("decimal", "numeric"):
        p = min(int(prec or 38), 38)
        return f"DECIMAL({p}, {min(int(scale), p)})"
    if dt == "float":
        return "FLOAT"
    if dt in ("double", "real"):
        return "DOUBLE"
    if dt == "year":
        return "INT"
    if dt == "date":
        return "DATE"
    if dt == "datetime":
        return f"TIMESTAMP({dtp})"
    if dt == "timestamp":
        return f"TIMESTAMP_LTZ({dtp})"
    if dt == "time":
        return f"TIME({min(int(dtp), 3)})"
    if dt == "bit":
        return "BOOLEAN" if prec == 1 else "BYTES"
    if dt in ("binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"):
        return "BYTES"
    # char, varchar, *text, enum, set, json, geometry ... -> STRING
    return "STRING"


def pick_schema(mysql_cfg, only_tables=None):
    """Returns {"tables": {name: {"columns": [...], "primary_key": [...]}}, "skipped": [...]}."""
    conn = pymysql.connect(
        host=mysql_cfg["host"],
        port=mysql_cfg["port"],
        user=mysql_cfg["user"],
        password=mysql_cfg["password"],
        database=mysql_cfg["database"],
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(COLUMNS_SQL, (mysql_cfg["database"],))
            col_rows = cur.fetchall()
            cur.execute(PK_SQL, (mysql_cfg["database"],))
            pk_rows = cur.fetchall()
    finally:
        conn.close()

    tables = {}
    for r in col_rows:
        t = tables.setdefault(r["table_name"], {"columns": [], "primary_key": []})
        t["columns"].append(
            {
                "name": r["column_name"],
                "mysql_type": r["column_type"],
                "flink_type": to_flink_type(r),
                "nullable": r["is_nullable"] == "YES",
            }
        )
    for r in pk_rows:
        if r["table_name"] in tables:
            tables[r["table_name"]]["primary_key"].append(r["column_name"])

    if only_tables:
        missing = [t for t in only_tables if t not in tables]
        if missing:
            log.warning("Requested tables not found in MySQL: %s", missing)
        tables = {t: v for t, v in tables.items() if t in only_tables}

    skipped = [t for t, v in tables.items() if not v["primary_key"]]
    for t in skipped:
        log.warning("Skipping %s: no primary key (cannot UPSERT)", t)
        del tables[t]

    log.info("Picked schema for %d table(s): %s", len(tables), sorted(tables))
    return {"tables": tables, "skipped": skipped}


def diff_and_save(schema, path):
    """Compare with the snapshot from the previous start, log differences, store the new snapshot."""
    changes = []
    new = schema["tables"]
    if os.path.exists(path):
        with open(path) as f:
            old = json.load(f)["tables"]
        for t in sorted(new.keys() - old.keys()):
            changes.append(f"NEW TABLE {t}")
        for t in sorted(old.keys() - new.keys()):
            changes.append(f"DROPPED TABLE {t}")
        for t in sorted(new.keys() & old.keys()):
            oc = {c["name"]: c["mysql_type"] for c in old[t]["columns"]}
            nc = {c["name"]: c["mysql_type"] for c in new[t]["columns"]}
            for c in sorted(nc.keys() - oc.keys()):
                changes.append(f"{t}: ADDED COLUMN {c} {nc[c]}")
            for c in sorted(oc.keys() - nc.keys()):
                changes.append(f"{t}: DROPPED COLUMN {c}")
            for c in sorted(nc.keys() & oc.keys()):
                if oc[c] != nc[c]:
                    changes.append(f"{t}: TYPE CHANGED {c} {oc[c]} -> {nc[c]}")
            if old[t]["primary_key"] != new[t]["primary_key"]:
                changes.append(f"{t}: PRIMARY KEY CHANGED {old[t]['primary_key']} -> {new[t]['primary_key']}")
        if not changes:
            log.info("No schema changes since last start")
    else:
        log.info("No previous snapshot at %s (first run)", path)

    for line in changes:
        log.warning("SCHEMA CHANGE: %s", line)

    with open(path, "w") as f:
        json.dump(schema, f, indent=2)
    return changes


if __name__ == "__main__":
    from main import load_config, setup_logging

    setup_logging()
    cfg = load_config()
    s = pick_schema(cfg["mysql"], cfg["tables"])
    diff_and_save(s, cfg["snapshot_path"])
    for tname, info in s["tables"].items():
        print(f"\n{tname}  (PK: {info['primary_key']})")
        for c in info["columns"]:
            print(f"  {c['name']:<30} {c['mysql_type']:<25} -> {c['flink_type']}")

"""Uses Flink (mysql-cdc connector, scan.startup.mode = latest-offset) to pick MySQL changes.

Produces a DataStream of (table_name, {column: value}) for INSERT and UPDATE_AFTER only.
DELETE and UPDATE_BEFORE are dropped here (UPSERT-only, no deletes).

    python pick_mysql_changes.py     # debug: prints changes, does NOT write to Postgres
"""
import logging
import os
import sys

from pyflink.common import RowKind, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction
from pyflink.table import StreamTableEnvironment

log = logging.getLogger("pick_mysql_changes")

_HERE = os.path.dirname(os.path.abspath(__file__))


def _q(name):
    """Flink identifier quoting (backticks)."""
    return "`" + name.replace("`", "``") + "`"


def _lit(value):
    """Flink string literal."""
    return str(value).replace("'", "''")


def build_source_ddl(cfg, table, info, index):
    m = cfg["mysql"]
    pk = set(info["primary_key"])
    col_defs = []
    for c in info["columns"]:
        not_null = " NOT NULL" if c["name"] in pk else ""
        col_defs.append(f"  {_q(c['name'])} {c['flink_type']}{not_null}")
    pk_cols = ", ".join(_q(c) for c in info["primary_key"])
    cols_sql = ",\n".join(col_defs)
    return f"""
CREATE TEMPORARY TABLE {_q('src_' + table)} (
{cols_sql},
  PRIMARY KEY ({pk_cols}) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '{_lit(m['host'])}',
  'port' = '{m['port']}',
  'username' = '{_lit(m['user'])}',
  'password' = '{_lit(m['password'])}',
  'database-name' = '{_lit(m['database'])}',
  'table-name' = '{_lit(table)}',
  'scan.startup.mode' = 'latest-offset',
  'server-time-zone' = '{_lit(m['timezone'])}',
  'server-id' = '{cfg['server_id_base'] + index}'
)"""


class ToChangeEvent(FlatMapFunction):
    """Row (with RowKind) -> (table, {col: value}); drops DELETE / UPDATE_BEFORE."""

    def __init__(self, table, columns):
        self.table = table
        self.columns = columns

    def flat_map(self, row):
        kind = row.get_row_kind()
        if kind in (RowKind.INSERT, RowKind.UPDATE_AFTER):
            yield (self.table, {c: row[i] for i, c in enumerate(self.columns)})


def build_change_stream(cfg, schema):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # binlog is a single ordered stream; keeps per-row order
    env.enable_checkpointing(60_000)
    if cfg.get("checkpoint_dir"):
        env.get_checkpoint_config().set_checkpoint_storage_dir(cfg["checkpoint_dir"])

    jars = []
    for key, label in (("cdc_jar", "MySQL CDC"), ("mysql_jdbc_jar", "MySQL JDBC")):
        path = cfg.get(key)
        if not path:
            raise RuntimeError(f"{label} jar path is not set")
        jar = os.path.abspath(path)
        if not os.path.isfile(jar):
            raise RuntimeError(f"{label} jar not found: {jar}")
        jars.append("file://" + jar)
    env.add_jars(*jars)
    # Make the Python workers able to import our modules and use this interpreter.
    for f in ("pick_mysql_changes.py", "upsert.py", "schema_pick.py"):
        env.add_python_file(os.path.join(_HERE, f))

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.local-time-zone", "UTC")
    t_env.get_config().set("python.executable", sys.executable)
    t_env.get_config().set("python.client.executable", sys.executable)

    streams = []
    for i, (table, info) in enumerate(sorted(schema["tables"].items())):
        t_env.execute_sql(build_source_ddl(cfg, table, info, i))
        changelog = t_env.to_changelog_stream(
            t_env.sql_query(f"SELECT * FROM {_q('src_' + table)}")
        )
        cols = [c["name"] for c in info["columns"]]
        streams.append(
            changelog.flat_map(
                ToChangeEvent(table, cols), output_type=Types.PICKLED_BYTE_ARRAY()
            ).name(f"cdc-{table}")
        )
        log.info("Source registered for %s (server-id %d)", table, cfg["server_id_base"] + i)

    if not streams:
        raise RuntimeError("No tables to capture (none found, or none have a primary key).")
    merged = streams[0] if len(streams) == 1 else streams[0].union(*streams[1:])
    return env, merged


if __name__ == "__main__":
    import schema_pick
    from main import load_config, setup_logging

    setup_logging()
    conf = load_config()
    sch = schema_pick.pick_schema(conf["mysql"], conf["tables"])
    environment, stream = build_change_stream(conf, sch)
    stream.print()
    environment.execute("debug-print-mysql-changes")

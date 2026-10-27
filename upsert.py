"""Applies change events to PostgreSQL as UPSERTs.

Never deletes (DELETE / UPDATE_BEFORE are dropped in pick_mysql_changes.py).
Never issues DDL. Warehouse-only columns (including serial "whId") are left alone.

Write rule: only columns that exist on both the MySQL row and the warehouse table,
plus compulsory "dbId" = DB_ID. Match existing rows by (MySQL primary key + "dbId"),
not by the warehouse primary key.

    python upsert.py     # prints which MySQL tables overlap the warehouse
"""
import logging

import psycopg2
from psycopg2 import sql
from pyflink.datastream.functions import FlatMapFunction

log = logging.getLogger("upsert")

DB_ID_COLUMN = "dbId"
WAREHOUSE_OWNED = {"whId"}
INT_TYPES = {"smallint", "integer", "bigint"}


def _connect(pg):
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        dbname=pg["dbname"],
    )
    conn.autocommit = True
    return conn


def load_table_meta(conn, schema_name, table):
    """Warehouse columns, plus names we must never write (serial / identity / whId)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (schema_name, table),
        )
        columns = dict(cur.fetchall())
        if not columns:
            return None
    readonly = WAREHOUSE_OWNED & set(columns)
    return {"columns": columns, "readonly": readonly}


def conflict_key(info):
    return list(info["primary_key"]) + [DB_ID_COLUMN]


def filter_targets(pg, schema):
    """Keep MySQL tables that already exist in the warehouse and have dbId + MySQL PK columns.

    No DDL. Extra MySQL columns are omitted on write; extra warehouse columns are left as-is.
    """
    conn = _connect(pg)
    kept = {}
    try:
        for table, info in schema["tables"].items():
            reason = _skip_reason(conn, pg["schema"], table, info)
            if reason:
                log.warning("Skipping %s: %s", table, reason)
                continue
            meta = load_table_meta(conn, pg["schema"], table)
            mysql_cols = {c["name"] for c in info["columns"]}
            extra = sorted(mysql_cols - set(meta["columns"]))
            if extra:
                log.info(
                    '"%s": warehouse is missing columns %s; those values will not be written',
                    table,
                    extra,
                )
            if meta["readonly"]:
                log.info('"%s": warehouse-owned columns left untouched: %s', table, sorted(meta["readonly"]))
            kept[table] = info
    finally:
        conn.close()
    if not kept:
        raise RuntimeError(
            "No MySQL tables overlap the warehouse. Each target table needs "
            f'column "{DB_ID_COLUMN}" and the MySQL primary-key column(s).'
        )
    schema["tables"] = kept
    log.info("Warehouse overlap: %d table(s) %s", len(kept), sorted(kept))
    return schema


def _skip_reason(conn, schema_name, table, info):
    meta = load_table_meta(conn, schema_name, table)
    if meta is None:
        return f"not in {schema_name} (create it yourself if you want it synced)"
    if DB_ID_COLUMN not in meta["columns"]:
        return f'missing compulsory column "{DB_ID_COLUMN}"'
    missing_pk = [c for c in info["primary_key"] if c not in meta["columns"]]
    if missing_pk:
        return f"MySQL PK columns {missing_pk} are not on the warehouse table"
    return None


class PostgresUpserter(FlatMapFunction):
    """(table, {col: value}) -> UPDATE by (MySQL PK + dbId), else INSERT those columns."""

    def __init__(self, pg_cfg, db_id, keys):
        self.pg_cfg = pg_cfg
        self.db_id = db_id
        self.keys = keys  # table -> [mysql pk columns..., "dbId"]
        self.conn = None
        self.meta = {}
        self.stmts = {}

    def open(self, runtime_context):
        self.conn = _connect(self.pg_cfg)
        self.meta = {}
        self.stmts = {}

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def flat_map(self, event):
        table, values = event
        values = dict(values)
        values[DB_ID_COLUMN] = self.db_id

        meta = self._meta(table)
        key_cols = self.keys[table]
        cols = [c for c in values if c in meta["columns"] and c not in meta["readonly"]]

        missing_key = [c for c in key_cols if c not in cols]
        if missing_key:
            raise RuntimeError(f'"{table}": event is missing upsert key column(s) {missing_key}')

        data_cols = [c for c in cols if c not in key_cols]
        update_sql, insert_sql = self._statements(table, data_cols, key_cols)
        coerce = meta["columns"]
        update_params = [self._coerce(values[c], coerce[c]) for c in data_cols + key_cols]
        insert_params = [self._coerce(values[c], coerce[c]) for c in data_cols + key_cols]

        def work():
            with self.conn.cursor() as cur:
                if data_cols:
                    cur.execute(update_sql, update_params)
                    if cur.rowcount:
                        return
                else:
                    cur.execute(update_sql, [self._coerce(values[c], coerce[c]) for c in key_cols])
                    if cur.rowcount:
                        return
                cur.execute(insert_sql, insert_params)

        self._execute(work)
        return iter(())

    def _meta(self, table):
        if table not in self.meta:
            meta = load_table_meta(self.conn, self.pg_cfg["schema"], table)
            if meta is None:
                raise RuntimeError(f'Postgres table "{table}" is missing')
            if DB_ID_COLUMN not in meta["columns"]:
                raise RuntimeError(f'Postgres table "{table}" has no "{DB_ID_COLUMN}" column')
            self.meta[table] = meta
        return self.meta[table]

    def _statements(self, table, data_cols, key_cols):
        cache_key = (table, tuple(data_cols), tuple(key_cols))
        if cache_key in self.stmts:
            return self.stmts[cache_key]
        tbl = sql.Identifier(self.pg_cfg["schema"], table)
        where = sql.SQL(" AND ").join(
            sql.SQL("{c} = %s").format(c=sql.Identifier(c)) for c in key_cols
        )
        if data_cols:
            set_clause = sql.SQL(", ").join(
                sql.SQL("{c} = %s").format(c=sql.Identifier(c)) for c in data_cols
            )
            update_sql = sql.SQL("UPDATE {} SET {} WHERE {}").format(tbl, set_clause, where)
        else:
            update_sql = sql.SQL("UPDATE {} SET {} = {} WHERE {}").format(
                tbl,
                sql.Identifier(DB_ID_COLUMN),
                sql.Identifier(DB_ID_COLUMN),
                where,
            )
        insert_cols = data_cols + key_cols
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            tbl,
            sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols),
            sql.SQL(", ").join(sql.Placeholder() for _ in insert_cols),
        )
        self.stmts[cache_key] = (update_sql, insert_sql)
        return self.stmts[cache_key]

    @staticmethod
    def _coerce(value, pg_type):
        if isinstance(value, bool) and pg_type in INT_TYPES:
            return int(value)
        return value

    def _execute(self, work):
        for attempt in (1, 2):
            try:
                work()
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                if attempt == 2:
                    raise
                log.warning("Postgres connection lost, reconnecting")
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = _connect(self.pg_cfg)


if __name__ == "__main__":
    import schema_pick
    from main import load_config, setup_logging

    setup_logging()
    conf = load_config()
    sch = schema_pick.pick_schema(conf["mysql"], conf["tables"])
    filter_targets(conf["pg"], sch)
    print("Warehouse overlap:", ", ".join(sorted(sch["tables"])))

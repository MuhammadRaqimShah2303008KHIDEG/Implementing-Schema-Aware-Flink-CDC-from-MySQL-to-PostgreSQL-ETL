# Implementing Schema Aware Flink CDC from MySQL-to-PostgreSQL ETL
# MySQL → PostgreSQL CDC Pipeline

Real-time, **UPSERT-only** change data capture from a MySQL (AWS RDS) binlog into an existing PostgreSQL warehouse, built with **Apache Flink CDC** and **PyFlink**, packaged as a single Docker container.

<img width="1573" height="1109" alt="Architecture" src="https://github.com/user-attachments/assets/067ce2be-a8e9-4e31-9ff0-5012bbc70661" />


## Highlights

- **Binlog based, no locks.** Uses the Flink `mysql-cdc` connector with `scan.startup.mode = latest-offset`, so there is no initial snapshot and the source tables are never locked.
- **Upsert only.** `DELETE` and `UPDATE_BEFORE` events are dropped. Rows are never deleted from the warehouse.
- **No DDL, ever.** The pipeline only writes into warehouse tables that already exist. Extra MySQL columns are ignored; extra warehouse columns (including a serial `"whId"`) are left untouched.
- **Multi-source friendly.** Every row is stamped with a compulsory `"dbId"` column (`DB_ID`), and rows are matched on **MySQL primary key + `"dbId"`**, so several source databases can feed one warehouse table.
- **Schema aware.** On every start the MySQL schema is read, converted to Flink types, and compared with the previous run. Added/dropped columns, type changes and PK changes are logged as warnings.
- **Fault tolerant by design.** A bad value never stops the job (see [Value handling](#value-handling)).
- **Offline driver install.** Connector JARs are copied from `./drivers`; nothing is downloaded for them at build time.

## How it works

| Step | Module | What happens |
|------|--------|--------------|
| 1 | `schema_pick.py` | Reads tables, columns and primary keys from `information_schema`, maps MySQL types to Flink SQL types, skips tables without a primary key, logs schema differences against `schema_snapshot.json`. |
| 2 | `upsert.filter_targets()` | Keeps only tables that exist in the warehouse schema and have `"dbId"` plus the MySQL PK column(s). |
| 3 | `pick_mysql_changes.py` | Registers one `mysql-cdc` source per table (each with its own `server-id`), keeps `INSERT` and `UPDATE_AFTER`, converts them to `(table, {column: value})` and logs one line per change. |
| 4 | `upsert.py` | `UPDATE ... WHERE <mysql pk> AND "dbId"`; if no row matched, `INSERT`. Values are coerced to fit the target column types. |
| – | `main.py` | Entry point. Loads configuration from environment / `.env` and wires the steps together. |

## Requirements

**MySQL / RDS**
- Binary logging enabled with `binlog_format = ROW` and `binlog_row_image = FULL`.
- Sufficient binlog retention (on RDS: `CALL mysql.rds_set_configuration('binlog retention hours', 24);`).
- A user with `SELECT`, `REPLICATION SLAVE` and `REPLICATION CLIENT`.
- Tables must have a primary key (tables without one are skipped).

**PostgreSQL**
- Target tables already created in `PG_SCHEMA` (default `warehouse`), each with a `"dbId"` column and the MySQL primary-key column(s).
- The table's primary/unique constraints must allow rows from different `"dbId"` values to coexist if you load several sources (for example a surrogate `"whId"` serial key plus a unique index on `(<mysql pk>, "dbId")`).

**Host**
- Docker and Docker Compose.
- The two connector JARs in `./drivers` (see below).

## Quick start

```bash
git clone Implementing-Schema-Aware-Flink-CDC-from-MySQL-to-PostgreSQL-ETL
cd Implementing-Schema-Aware-Flink-CDC-from-MySQL-to-PostgreSQL-ETL

# 1. Put the drivers in ./drivers (any version suffix works)
#    flink-sql-connector-mysql-cdc-<ver>.jar
#    mysql-connector-j-<ver>.jar

# 2. Configure
cp .env.example .env
# edit .env with your MySQL / PostgreSQL details

# 3. Build and run
docker compose up -d --build

# 4. Follow the pipeline log lines
docker exec mysql-flink-cdc sh -c "grep -h -E ' (INFO|WARNING|ERROR) (pick_mysql_changes|upsert|schema_pick):' /opt/flink/log/*.log | tail -n 50"
```

The base image is `flink:1.20-java17` and the CDC connector is the Flink 1.20 build (3.6.x-1.20). Keep the connector version in line with the Flink version.

## Configuration

All settings are environment variables (from `.env`, which `docker-compose.yml` passes through `env_file`).

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | yes | – | Source database. |
| `MYSQL_PORT` | no | `3306` | |
| `MYSQL_SERVER_TZ` | no | `UTC` | Must match the MySQL/RDS server time zone, otherwise `TIMESTAMP` values shift. Formats like `UTC+5` are normalised. |
| `MYSQL_TABLES` | one of | – | Comma-separated tables to capture. |
| `ALLOW_ALL_TABLES` | one of | `false` | Set `true` to capture every base table with a primary key when `MYSQL_TABLES` is empty. |
| `MYSQL_SERVER_ID_BASE` | no | `5400` | First binlog `server-id`; each table uses base, base+1, ... Must be unique across all binlog clients of the server.(But it's optional) |
| `PG_HOST`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE` | yes | – | Warehouse database. |
| `PG_PORT` | no | `5432` | |
| `PG_SCHEMA` | no | `warehouse` | Schema containing the target tables. |
| `DB_ID` | no | `2` | Value written to `"dbId"` for every row from this source. |
| `CDC_JAR`, `MYSQL_JDBC_JAR` | no | `/opt/flink/usrlib/...` | Set in the image and compose file; override only if you move the JARs.(But it's optional) |
| `SCHEMA_SNAPSHOT_PATH`, `CHECKPOINT_DIR` | no | `/data/...` | Persisted in the `cdc-data` volume.(But it's optional) |

## Value handling

A single odd value must never take the pipeline down.

| Target column in PostgreSQL | Behaviour |
|-----------------------------|-----------|
| `varchar` / `char` / `text` | Accepts anything. Clean printable bytes become text; other bytes (e.g. `BINARY(16)` ids) become a hex string; other types use `str()`. NUL (`0x00`) characters are removed. If the value is longer than `varchar(n)`, that column is skipped. |
| `bytea` | Bytes are written as-is; text is UTF-8 encoded. |
| `integer` types / `boolean` | `bool` ↔ `int` is converted automatically. |
| Anything else | If PostgreSQL rejects the value (type mismatch, out of range), **only that column is skipped** for that row and the rest of the row is still written. |
| Row cannot be written at all | Logged as an error with its key and skipped; processing continues. |
| Connection lost | Retried once, then the job fails so Flink restarts it. |

MySQL `TIMESTAMP` columns are converted to UTC wall-clock datetimes and the PostgreSQL session runs in UTC, so they are exact for `timestamptz` columns. `DATETIME` columns are copied as plain wall-clock values.

## Logging

Only meaningful events are logged, with no debug noise:

```
INFO    pick_mysql_changes: INSERT transaction pk={id: 1042}
INFO    pick_mysql_changes: UPDATE operator pk={id: 7}
WARNING upsert: orders: column "meta" skipped, value does not fit Postgres type integer
ERROR   upsert: reinitiated_disbursements: row skipped, key={'id': '8873', 'dbId': '4'}: UniqueViolation: ... | Key (...)=(...) already exists.
WARNING schema_pick: SCHEMA CHANGE: orders: ADDED COLUMN note varchar(100)
```

## Project structure

```
.
├── main.py                  # entry point and configuration
├── schema_pick.py           # MySQL schema discovery, type mapping, schema diff
├── pick_mysql_changes.py    # Flink CDC sources and change filtering
├── upsert.py                # PostgreSQL upsert sink and value coercion
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── drivers/                 # connector JARs (not downloaded at build time)
├── docs/architecture.svg
└── .env.example
```

Each module can also be run on its own for debugging: `schema_pick.py` prints the picked schema, `upsert.py` prints the warehouse overlap, and `pick_mysql_changes.py` prints changes without writing to PostgreSQL.

## Limitations

- **No history load.** `latest-offset` means only changes made after the job starts are captured. Load existing data separately.
- **No deletes.** Deleted MySQL rows stay in the warehouse. A changed MySQL primary key creates a new warehouse row.
- **Schema changes are reported, not applied.** Add new columns to the warehouse tables yourself. After a MySQL schema change, restart the container so the new schema is picked up.
- **Single ordered stream.** Parallelism is 1 to preserve per-row ordering, so throughput is bounded by one worker.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| `Unsupported conversion from data type 'TIMESTAMP_LTZ(n)'` | PyFlink cannot pass `Instant` to Python. Handled by casting those columns to `TIMESTAMP(n)` in `pick_mysql_changes.py`. |
| `A string literal cannot contain NUL (0x00) characters` | A binary value reached a text parameter. Handled by the value coercion in `upsert.py`. |
| `duplicate key value violates unique constraint "<table>_pkey"` | The warehouse primary key does not allow rows from different `"dbId"` values, or the `"whId"` sequence is behind `MAX("whId")`. Check the constraint and reset the sequence with `setval`. |
| `Missing environment variables` | Fill in `.env`, or set the variables in the container. |
| `no such host` while building | Docker cannot reach Docker Hub (DNS, VPN or proxy issue), not a code problem. |
| `Skipping <table>: ...` at start | The table is missing from the warehouse, lacks `"dbId"`, or lacks the MySQL PK column(s). Create or fix it and restart. |

## Security notes

- Never commit `.env`. It is listed in `.gitignore`; commit only `.env.example`.
- The `Dockerfile` currently runs `COPY .env .`, which bakes credentials into the image. Do not push that image to a public registry. Prefer relying on `env_file` in `docker-compose.yml` and removing that line.
- Use a dedicated MySQL user with only the privileges above, and a PostgreSQL user limited to the warehouse schema.

# InfDB Python Wrapper

A tiny helper layer around PostgreSQL for “InfDB” projects. It gives you:

- `InfDB`: one entry point for config + logging + DB connections
- `InfdbClient`: thin psycopg2 client (queries, run SQL files)
- `InfdbConfig`: load/merge YAML config and resolve placeholders
- `InfdbLogger`: stdout + file logging, with worker logger support
- `utils`: misc helpers (env → DSN, atomic writes, quick SQL)

## Install

```bash
uv pip install
```

## Basic idea

1. You write a YAML like `configs/choose-a-name.yml`.
2. You create `InfDB("mytool")`.
3. You call `.connect()` or `.get_db_engine()`.

## Example

```python
from infdb.infdb import InfDB

infdb = InfDB("mytool", config_path="configs")

# psycopg2 client
with infdb.connect("postgres") as db:
    rows = db.execute_query("SELECT now()")
    print(rows)

# SQLAlchemy engine
engine = infdb.get_db_engine("postgres")
with engine.connect() as conn:
    print(conn.execute("SELECT 1").scalar())
```

## Config shape

`configs/config-mytool.yml`:

```yaml
mytool:
  logging:
    path: logs/mytool/infdb.log
    level: INFO
  hosts:
    postgres:
      host: localhost
      exposed_port: 5432
      db: appdb
      user: appuser
      password: secret

services:  # optional global defaults
  postgres:
    host: host.docker.internal
    exposed_port: 5432
    db: appdb
    user: appuser
    password: secret
```

How it picks DB params:
- start with `services.<name>` (if present)
- override with `<tool>.hosts.<name>`
- if tool host is set → force to `host.docker.internal`

## Running SQL files

```python
with infdb.connect() as db:
    db.execute_sql_files("sql/boot", format_params={"schema": "public"})
```
- runs `*.sql` in order
- empty files are skipped
- on error: rollback + re-raise

## Logging

```python
log = infdb.get_log()
log.info("hi")
worker_log = infdb.get_worker_logger()
worker_log.info("from worker")
```

## Utilities (quick)

```python
from infdb.utils import build_dsn_from_env, atomic_write_text, do_sql_query
from infdb.config import InfdbConfig

cfg = InfdbConfig("mytool")
do_sql_query("SELECT 1", cfg)  # one-off query
```

That’s it. Keep configs small, call `InfDB` everywhere, and let the client do the DB wiring.


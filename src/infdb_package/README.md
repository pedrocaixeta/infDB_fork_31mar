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
---
## Functionality documentation

The core package documentation is generated automatically as Markdown pages.  
These pages describe the public API and behavior of each main module in the
`infdb` package.

- **InfDB facade**: [documentation/api/infdb.md](documentation/api/infdb.md)
- **Client**: [documentation/api/client.md](documentation/api/client.md)
- **Config**: [documentation/api/config.md](documentation/api/config.md)
- **Logger**: [documentation/api/logger.md](documentation/api/logger.md)
- **Utils**: [documentation/api/utils.md](documentation/api/utils.md)

### How this documentation is generated

These “functionality” pages are not maintained manually. They are produced by
the documentation automation scripts (API markdown generation → MkDocs build →
export).

The steps and commands to generate/update them are documented here:

- [scripts/README.md](scripts/README.md)
# infDB Python Package

This guide explains how to use the `pyinfdb` python package to interact with your database in a standardized, configuration-driven way.

## Overview

The `pyinfdb` package is a wrapper around postgresql database connections. It provides a single "Facade" class (`InfDB`) that handles:

1. **Configuration**: Loading database credentials and settings from YAML and environment files.
2. **Connections**: Providing both raw `psycopg2` clients and `SQLAlchemy` engines.
3. **Logging**: Standardized logging for your tools.
4. **Utility Functions**: Utilities for common tasks.

## Installation

```bash
pip install pyinfdb
```

## Quick Start

The typical workflow involves three steps:

1.  **Setup Configuration**: Ensure you have a YAML config file.
2.  **Initialize InfDB**: Create an instance of the `InfDB` class.
3.  **Connect**: Use the instance to get a database connection.

### 1. Configuration

InfDB looks for configuration files in a specific directory (default: `../configs`).
A typical config file (e.g., `configs/config-my-tool.yml`) looks like this:

```yaml
# configs/config-my-tool.yml
db:
  host: localhost
  port: 5432
  user: my_user
  password: my_password
  dbname: my_database

logging:
  level: INFO
  path: logs/my_tool.log
```

### 2. Initialization

Import and initialize the `InfDB` class. You must provide a `tool_name` which usually corresponds to your configuration setup (though specific behavior depends on your config structure).

```python
from infdb import InfDB

# Initialize for a tool named "my-tool"
# It will look for configs in the default directory ("../configs")
infdb = InfDB(tool_name="my-tool")
```

### 3. Usage Patterns

You can interact with the database in two main ways: using the internal client (wrapper around `psycopg2`) or getting a standard `SQLAlchemy` engine.

#### Option A: Using `InfdbClient` (Direct SQL)

Best for running raw SQL queries or executing SQL scripts.

```python
# Connect to the database defined in your config
# The argument "postgres" is typically the service name or db identifier in your config
with infdb.connect() as client:
    
    # Execute a simple query
    rows = client.execute_query("SELECT * FROM my_table LIMIT 5")
    for row in rows:
        print(row)
        
    # Execute a SQL file
    client.execute_file("scripts/setup_schema.sql")
```

#### Option B: Using SQLAlchemy (Dataframes, ORM)

Best for use with `pandas`, `geopandas`, or when you need a standard SQLAlchemy engine.

```python
import pandas as pd

# Get a standard SQLAlchemy engine
engine = infdb.get_db_engine()

# Use with pandas
df = pd.read_sql("SELECT * FROM my_table", engine)
print(df.head())
```

## Logging

`InfDB` automatically sets up logging based on your configuration.

```python
# Get the configured logger
logger = infdb.get_logger()

logger.info("Starting processing...")
try:
    # Do work...
    logger.info("Finished successfully.")
except Exception as e:
    logger.error(f"An error occurred: {e}")
```

## API Summary

### `class InfDB(tool_name: str, config_path: str)`

The main entry point.

*   **`connect() -> InfdbClient`**: Returns a context manager for a database client. usage: `with infdb.connect() as client: ...`
*   **`get_db_engine()`**: Returns a SQLAlchemy `Engine` object.
*   **`get_logger() -> logging.Logger`**: Returns the standard logger.
*   **`get_config_dict() -> dict`**: Returns the full loaded configuration as a dictionary.

### `class InfdbClient`

Returned by `connect()`.

*   **`execute_query(query: str, params: tuple)`**: Execute a raw SQL query.
*   **`execute_file(filepath: str)`**: Execute all SQL commands in a file.

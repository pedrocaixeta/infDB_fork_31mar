# Python Package pyinfdb
This guide explains how to use the `pyinfdb` python package to interact with your infDB instance.

## Structure
The `pyinfdb` package consists of a superior class InfDB based on the internal classes InDBConfig, InfDBClient, InfDBLogger and InfDBIO as shown in the following figure:
![alt text](pyinfdb.png)
The user only interacts with the superior InfDB class, the internal classes are not directly accessible. This abstraction ensures the python interface is consistent despite changes in the internal structure of the package.
It provides functions for database access, configuration management, logging and data handling. The central idea is to provide standard methods to interact with infDB in order to simplify the interaction with infDB.

## Installation
pyinfdb is available on [PyPI](https://pypi.org/project/infdb/) and can be installed via pip:
```bash
uv pip install pyinfdb
```

## Quick Start
The typical workflow involves three steps:

1.  **Setup Configuration**: Ensure you have a YAML config file.
2.  **Initialize InfDB**: Create an instance of the `InfDB` class.
3.  **Database Query**: Use the instance to get a database connection.

### 1. Configuration
The infDB class use the environment variables and YAML configuration files to load the database credentials and settings. The configuration files path needs to be given as the argument `config_path` when initializing the InfDB class. If no path is given, the no configuration will be loaded and the package will rely solely on environment variables. A typical config file (e.g., `configs/config-my-tool.yml`) looks like this:
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
Configuration paramters that should be not changed can be set to "None" in the config file. This ensures that these parameters are not accidentally changed and that the default values are used instead.

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

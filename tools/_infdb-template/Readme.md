# InfDB Dev Container Template

A reusable template for creating Docker-based development containers that interact with the InfDB infrastructure. This template provides a standardized structure for building data processing tools, loaders, and analysis scripts that work with the InfDB PostgreSQL/PostGIS database.

## Overview

This template enables you to:
- Quickly scaffold new InfDB tools with consistent structure
- Access InfDB database connections with pre-configured clients
- Run Python scripts with geospatial capabilities (GeoPandas, SQLAlchemy)
- Execute SQL scripts for database operations
- Develop with VS Code dev containers for debugging

## Getting Started

### Development Workflow

0. **Define tool name:**
   - think of tool name
   - use kebab-case naming convention as for example "your-tool-name" 

1. **Copy and rename the template:**
   ```bash
   cp -r tools/_infdb-template tools/your-tool-name
   ```

2. **Customize configuration:**
   - Rename `configs/config-your-tool-name.yml` to match your tool name
   - Update configuration values (database, schemas, logging)
   - `None` will be automatically adopted by settings defined in config-infdb.yml of infDB. Replace `None` by actual parameters if you want to connect to remote database
   - for `output_schema` you need to use snake_case like "your_tool_name" since postgresql database naming convention does not accept kebab-case. 

3. **Replace placeholder "your-tool-name":**
    - replace all remaining occurances of "your-tool-name" in the new copied folder by the name of your tool.

4. **Add dependencies:**
   - Edit `pyproject.toml` under `dependencies` section
   <!-- - Rebuild after changes: `docker compose up --build` -->

5. **Implement your code:**
   - **Python:** Add modules to `src/`, implement logic in `main.py`
   - **SQL:** Add scripts to `sql/` (executed in alphabetical order)

6. **Run your tool:**
   ```bash
   # Standard run (from project root)
   docker compose -f tools/your-tool-name/compose.yml up
   ```

    Develop with preconfigured VS Code as development container:

    1. Open the tool folder: `code tools/your-tool-name`
    2. Install "Dev Containers" extension
    3. Press `F1` → "Dev Containers: Reopen in Container"
    4. Set breakpoints and debug with `F5`


## Project Structure

```
your-tool-name/
├── src/                                    # Python source modules
│   └── demo.py                             # Example database operations
├── sql/                                    # SQL scripts (alphabetical order)
│   └── 00_cleanup.sql                      # Schema initialization
├── configs/                                # Configuration files
│   └── config-your-tool-name.yml          # Tool-specific config
├── main.py                                 # Entry point - starts here
├── pyproject.toml                          # Python dependencies
├── compose.yml                             # Docker Compose definition
├── Dockerfile                              # Docker image build
├── .env                                    # Environment variables
└── Readme.md                               # This file
```

### Key Files

#### `main.py`
Entry point that:
- Initializes InfDB client (Sets up logging, database connections, etc.)
- Executes your business logic

#### `src/demo.py`
Example functions showing:
- SQL script execution with InfDB
- Database queries with InfDB client
- Direct SQLAlchemy connections
- GeoPandas spatial data integration

#### `sql/*.sql`
SQL scripts for database operations:
- Execute in alphabetical order (use prefixes: `01_`, `02_`)
- Support template variables like `{output_schema}`, `{input_schema}`
- Useful for schema setup, transformations, indexes

#### `pyproject.toml`
Python project configuration:
- Package dependencies
- Python version requirements
- Build system configuration

#### `compose.yml`
Docker Compose service definition:
- Container configuration
- Volume mounts
- Network settings
- Environment variables

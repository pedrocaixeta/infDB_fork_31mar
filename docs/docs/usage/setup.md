
## Prequisites

!!! info 
    You can either use [Docker Engine](https://docs.docker.com/engine/install/) or [Docker Desktop ](https://docs.docker.com/desktop/) (for a Graphical User Interface).

## Steps
If you are happy with the preconfiguration and default passwords, then just follow these four steps (see detailed instructions in the corresponding sections below):

1. [Prepare folder structure](#Suggested-folder-structure-for-infDB)   
2. [Clone infDB](#clone-infdb)
3. [Startup infDB](#startup-script)
4. [Import data and run toolchain](#setup-infdb-loader)

!!! warning
    All commands need to be executed on **macOS or Linux**. 

!!! tip "Tip - Windows Users"
    Install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) **and** [Ubuntu as Windows Subsystem for Linux (WSL)](https://documentation.ubuntu.com/wsl/stable/howto/install-ubuntu-wsl2/). After installation, launch the Linux terminal by searching for "Ubuntu" in your applications.

## Folder Structure of infDB
The infDB provides a modular folder structure that allows managing multiple database instances independently. Each instance represents a separate deployment with its own data, configuration, and services—ideal for handling different regions, projects, or environments.


!!! example
        infdb/
        ├── infdb-demo/
        ├── sonthofen/
        ├── ...
        └── muenchen/
    The recommended structure places all instance data in docker managed volumes while keeping each instance's configuration and tools in separate directories (e.g., `infdb-demo/`, `sonthofen/`, `muenchen/`). This approach simplifies backups, migrations, and multi-instance management.

First of all, create the main `infdb` directory and navigate into it:
```bash
mkdir infdb
cd infdb
```

## Clone infDB
Then, you can access the repository either with SSH or HTTPS as you like:


!!! warning "Windows Users"
    Clone the repository to your Ubuntu home directory:
    ````
    \\wsl.localhost\Ubuntu\home\[PC username]
    ````
    (in file explorer Windows shows \\wsl.localhost as Linux) and execute scripts from Linux terminal (search for Ubuntu in applications)

You can either use **SSH** or **HTTPS**:

- **SSH** (Secure Shell) uses cryptographic key pairs for authentication. Once set up, you won't need to enter credentials for each operation. Recommended for frequent Git operations. 
- **HTTPS** uses username and password (or personal access token) for authentication. Simpler to set up initially but may require credentials for each operation unless you configure credential caching.

=== "SSH"
    ``` bash
    # Replace "infdb-demo" by name of instance 
    git clone git@git-ce.rwth-aachen.de:need/NEED-infdb.git infdb-demo 
    ```

=== "HTTPS"
    ```bash
    # Replace "infdb-demo" by name of instance
    git clone https://git-ce.rwth-aachen.de/need/NEED-infdb.git infdb-demo
    ```

Both methods are secure and work identically for cloning, pushing, and pulling. Your choice depends on your workflow preferences and environment constraints.

Navigate to the instance directory:
```bash
cd infdb-demo
```


## Setup infDB Configuration

!!! note
    If you're using the default configuration, you can skip editing `.env` configuration file.

Before starting infDB, you need to configure the infDB, you need to create `.env` configuration file by copying from the template `.env.template`
```bash
cp .env.template .env
```

Edit the environment file `.env` to customize your infDB instance settings (database credentials, ports, paths, etc.):
    

``` bash
# ==============================================================================
# InfDB Docker Compose Configuration
# ==============================================================================
# This file contains all configuration parameters for the InfDB Docker setup.
# Copy this file to .env and customize the values as needed.
# ==============================================================================

# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate

# Base profiles
COMPOSE_PROFILES=core

# All profiles
# COMPOSE_PROFILES=core,admin,notebook,qwc,api

# ==============================================================================
# BASE CONFIGURATION
# ==============================================================================
# Base name for the project (used in network names and data paths)
BASE_NAME=infdb-demo

# ==============================================================================
# POSTGRESQL DATABASE (Core Service)
# ==============================================================================
# Profile: core

# Database name
SERVICES_POSTGRES_DB=infdb

# Database credentials
SERVICES_POSTGRES_USER=infdb_user
SERVICES_POSTGRES_PASSWORD=infdb

# Host:Port address from which a container is able to reach the Postgres database
SERVICES_POSTGRES_HOST=host.docker.internal
SERVICES_POSTGRES_EXPOSED_PORT=54328

# EPSG code for spatial reference system (25832 = ETRS89 / UTM zone 32N)
SERVICES_POSTGRES_EPSG=25832


# ==============================================================================
# PGADMIN (Database Administration Interface)
# ==============================================================================
# Profile: admin

# Default login credentials for pgAdmin
SERVICES_PGADMIN_DEFAULT_EMAIL=admin@need.energy
SERVICES_PGADMIN_DEFAULT_PASSWORD=infdb

# Port to expose pgAdmin on the host machine
SERVICES_PGADMIN_EXPOSED_PORT=82


# ==============================================================================
# FASTAPI (REST API Service)
# ==============================================================================
# Profile: api

# Port for the FastAPI service
SERVICES_API_PORT=8000


# ==============================================================================
# PYGEOAPI (OGC API Service)
# ==============================================================================
# Profile: api

# Port for the PyGeoAPI service
SERVICES_PYGEOAPI_PORT=8001

# Host IP to run PyGeoAPI on (e.g., localhost or 10.162.28.144)
SERVICES_PYGEOAPI_BASE_HOST=localhost


# ==============================================================================
# POSTGREST (PostgreSQL REST API)
# ==============================================================================
# Profile: api

# Port for the PostgREST service
SERVICES_POSTGREST_PORT=8002


# ==============================================================================
# JUPYTER NOTEBOOK (Development Environment)
# ==============================================================================
# Profile: notebook

# Port to expose Jupyter on the host machine
SERVICES_JUPYTER_EXPOSED_PORT=8888

# Enable Jupyter Lab interface (yes/no)
SERVICES_JUPYTER_ENABLE_LAB=yes

# Authentication token for Jupyter
SERVICES_JUPYTER_TOKEN=infdb

# Path to notebook files
SERVICES_JUPYTER_PATH_BASE=..src/notebooks/


# ==============================================================================
# QGIS WEB CLIENT (QWC)
# ==============================================================================
# Profile: qwc

# Port for QWC web interface
SERVICES_QWC_EXPOSED_PORT_GUI=80

# Port for QWC internal database
SERVICES_QWC_EXPOSED_PORT_DB=5434

# Password for QWC PostgreSQL database
SERVICES_QWC_POSTGRES_PASSWORD=infdb

# JWT secret key for QWC (change this for production!)
JWT_SECRET_KEY=change-me-in-production
```
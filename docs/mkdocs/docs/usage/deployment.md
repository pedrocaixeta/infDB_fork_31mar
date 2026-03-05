---
icon: material/rocket-launch
---
# Deployment :material-rocket-launch:

The infDB platform is designed for easy deployment using provided bash scripts that abstract complex Docker Compose commands.

!!! warning "Prerequisite"
    Ensure that **Docker** and **Docker Compose** are installed and running on your system.

## Management Commands

### Start infDB
To start the configured infDB services:

```bash
bash infdb.sh start
```

!!! info "Persistence"
    infDB services will continue running in the background until manually stopped, even if the terminal is closed.

### Stop infDB
To stop all running services **without** deleting data:

```bash
bash infdb.sh stop
```

### Remove infDB
To stop services **and** delete all stored data (reset):

```bash
bash infdb.sh remove
```

!!! danger "Data Loss"
    This command will permanently remove all data stored in the database volumes.

## Data Import

The **infdb-import** service usually runs automatically on startup if configured. To trigger a manual import run without restarting the entire stack:

```bash
bash infdb.sh import
```

### Removing Docker Volumes
To manually remove the Docker volumes used by infDB, you can run:
```bash
docker volume rm infdb-import-data
docker volume rm infdb-db-data
```
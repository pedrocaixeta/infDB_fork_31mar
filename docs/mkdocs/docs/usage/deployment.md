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
bash infdb-start.sh up -d --build
```

!!! info "Persistence"
    infDB services will continue running in the background until manually stopped, even if the terminal is closed.

### Stop infDB
To stop all running services **without** deleting data:

```bash
bash infdb-stop.sh
```

### Remove infDB
To stop services **and** delete all stored data (reset):

```bash
bash infdb-remove.sh
```

!!! danger "Data Loss"
    This command will permanently remove all data stored in the database volumes.

## Data Import

The **infdb-import** service usually runs automatically on startup if configured. To trigger a manual import run without restarting the entire stack:

```bash
bash infdb-import.sh
```

### Cleaning Import Data
Downloaded raw data files are stored in a persistent Docker volume (`infdb-import-data`). To reclaim space:

```bash
docker volume rm infdb-import-data
```
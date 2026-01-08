---
icon: material/docker
---

The infDB can be easily deployed using the provided bash scripts, which simplify the startup, stop and removing process.
!!! warning
    Ensure that Docker is running. If you use Docker Desktop, start the application first.

## Start infDB
To start the configured infDB services, use the following startup script:
```bash
bash infdb-startup.sh
```
!!! info
    infDB will continue running until you stop it manually as described below, even if the machine is restarted.

## Stop infDB
To stop all running infDB services without deleting any data, execute:

```bash
bash infdb-stop.sh
```

## Remove infDB
To stop all running infDB services and delete all stored data, execute:

```bash
bash infdb-remove.sh
```
!!! danger
    All stored data gets removed

## Import Open Data
To import open data into the infDB, execute:

```bash
bash infdb-import.sh
```

!!! info
    Downloaded data is stored centrally on each host as a persistent docker volume. This data persists even if the `infdb-importer` container is removed.

## Remove Infdb-importer Data
To remove the downloaded open data, execute:

```bash
docker volume rm infdb-loader-data
```
!!! danger
    All downloaded data gets removed
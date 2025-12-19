---
icon: material/docker
---

The infDB can be easily deployed using the provided bash scripts, which simplify the startup, stop and removing process.
!!! warning
    Ensure that Docker is running. If you use Docker Desktop, start the application first.

## Start infDB

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

## Run infdb-loader
To run the data loader, execute:

```bash
bash infdb-loader-run.sh
```

!!! info
    Downloaded data is stored centrally on each host as a persistent docker volume. This data persists even if the `infdb-loader` container is removed.

## Remove infdb-loader data
To remove the downloaded `infdb-loader` data, execute:

```bash
bash infdb-loader-remove.sh
```
!!! danger
    All downloaded data gets removed
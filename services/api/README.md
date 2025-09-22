# INFDB API Stack

A small, dockerized stack that exposes your PostGIS-backed “CityDB” through two APIs:

- **PostgREST** — an instant REST API over PostgreSQL schemas/tables
- **pygeoapi** — an OGC API - Features for geospatial collections

…and a **FastAPI** service that can talk to both via internal URLs.

This README documents how each service is built and wired, what the watchdog containers do, required environment, and how to run/test/troubleshoot.

---

## Table of contents

1. [Architecture](#architecture)
2. [Files in this directory](#files-in-this-repo)
3. [Quick start](#quick-start)


---

## Architecture

                         +--------------------------------------------+
                         |       FastAPI                              |
                         |Host:${SERVICES_API_PORT} ->  container:8000|
                         |  talks to ↓   ↓                            |
                         | POSTGREST_INTERNAL                         |
                         | PYGEOAPI_INTERNAL                          |
                         +---------------------+----------------------+
                                               |
                ┌──────────────────────────────┴─────────────────────────────┐
                ▼                                                            ▼
               +-----------------------------+   +----------------------------+
               |        PostgREST            |   |         pygeoapi           |
               | exposes DB schemas/tables   |   | exposes Geo* collections   |
               | server-port: ${SERVICES_...}|   | port: ${SERVICES_PYGEO...} |
               +---------------+-------------+   +--------------+-------------+
                                ▲                               ▲
                                | config via watcher            | config via watcher
                                | (writes postgrest.conf)       | (writes pygeoapi-config.yml)
                                ▼                               ▼
                         pgrstwatch (python)              configwatch (python)
                                │                               │
                                └──── connects to Postgres ─────┘
                                    (your CityDB / PostGIS)

All services are joined to a shared Docker network `${BASE_NETWORK_NAME}` so they can resolve each other by container name.

---

## Files in this directory

- **`fastapi/api.yml`** — Compose file for the FastAPI service.
- **`fastapi/Dockerfile`** — Container image for the FastAPI service (uvicorn).
- **`postgrest/postgrest.yml`** — Compose file for PostgREST and its config watcher.
- **`postgrest/watch_and_update_postgrest_conf.py`** — Watches DB schemas, generates `postgrest/postgrest.conf`, and notifies PostgREST to reload.
- **`pygeoapi/pygeoapi.yml`** — Compose file for pygeoapi and its config watcher.
- **`pygeoapi/watch_and_generate_pygeoapi_config.py`** — Watches DB schema/data changes, generates `pygeoapi/pygeoapi-config.yml`.

---

## Quick start

1. **Generate the startup files.**

   ```bash
   docker compose -f services/setup/compose.yml up
   ```
    This command:

    - **Generates** the main `compose.yml` file
    - **Includes** configurations from:
    - `fastapi/api.yml`
    - `postgrest/postgrest.yml`
    - `pygeoapi/pygeoapi.yml`
    - **Creates** the `.env` file containing environment variables
<br>

2. **Bring up the stack in detached mode.**

   ```bash
   docker compose -f compose.yml up -d
   ````
    This starts the services defined in `fastapi/api.yml`, `postgrest/postgrest.yml`, and `pygeoapi/pygeoapi.yml` in the background.
<br>

3. **Access the APIs.**

   - FastAPI docs: http://localhost:8000/docs
   - pygeoapi root: http://localhost:5000/
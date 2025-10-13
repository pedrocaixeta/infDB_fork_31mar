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
4. [FastAPI Endpoints Usage](#fastapi-endpoints-usage)

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
<br>

4. **(Optional) View logs.**

   To view the logs of all services:

   ```bash
   docker compose logs -f
   ```

---

## FastAPI Endpoints Usage

The FastAPI service provides endpoints to interact with your PostgREST API.  
Below are the available endpoints and how to use them:

---

## 1. Get Data from PostgREST

**Endpoint:**  
```
GET /get-postgrest/{schema}/{table}
```

**Parameters:**
- `schema` (path): The database schema name (e.g., `public`)
- `table` (path): The table name (e.g., `buildings`)
- `limit` (query, optional): Limit the number of records returned (default: 100)
- `tolerance` (query, optional): Geometry simplification tolerance (default: 100; higher values mean more simplification)

**Example:**  
```
GET /get-postgrest/public/buildings?limit=10&tolerance=100
```

---

## 2. Create a New Row (POST)

**Endpoint:**  
```
POST /postgrest/{schema}/{table}
```

**Parameters:**
- `schema` (path): The database schema name
- `table` (path): The table name
- Request body: JSON object representing the new row to insert

**Example:**  
```
POST /postgrest/public/buildings
Content-Type: application/json

{
  "name": "New Building",
  "height": 50
}
```

---

## 3. Update an Existing Row (PUT/PATCH)

**Endpoint:**  
```
PUT /postgrest/{schema}/{table}/{item_id}
```

**Parameters:**
- `schema` (path): The database schema name
- `table` (path): The table name
- `item_id` (path): The primary key value of the row to update
- `key_column` (query, optional): The primary key column name (default: `id`)
- Request body: JSON object with updated fields

**Example:**  
```
PUT /postgrest/public/buildings/1?key_column=id
Content-Type: application/json

{
  "height": 60
}
```

---

## 4. Delete a Row

**Endpoint:**  
```
DELETE /postgrest/{schema}/{table}/{item_id}
```

**Parameters:**
- `schema` (path): The database schema name
- `table` (path): The table name
- `item_id` (path): The primary key value of the row to delete
- `key_column` (query, optional): The primary key column name (default: `id`)

**Example:**  
```
DELETE /postgrest/public/buildings/1?key_column=id
```

---

## 5. Health Check

**Endpoint:**  
```
GET /health
```

**Description:**  
Checks the health of the FastAPI service.

---

## Notes

- All endpoints are accessible via the FastAPI docs at `/docs`.
- For endpoints that modify data (POST, PUT, DELETE), ensure you provide the correct schema, table, and primary key information.
- The `tolerance` parameter in GET requests allows you to control geometry simplification for large geometry columns.
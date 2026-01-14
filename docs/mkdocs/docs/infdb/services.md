---
icon: material/cogs
---
# Managment Interfaces

The infDB platform provides a suite of essential services designed to facilitate database operation and administration, data handling and visualization, and connectivity. Each preconfigured service can be activated individually to tailor the environment to your specific requirements. This section provides a brief description and configuration options for each available service.

![alt text](../usage/services.png)

## Overview:

- [infdb-db](#infdb-db-material-database): Core PostgreSQL database with PostGIS, timescaledb, and pgrouting extensions; handles all central storage and queries.
- [infdb-importer](#infdb-importer-material-cloud-download): Automates the ingestion, structuring, and integration of external open data formats into the platform.
- [pgAdmin](https://www.pgadmin.org/): Web UI for inspecting schemas, running SQL, managing roles; auto-configured credentials.
- [FastAPI](https://fastapi.tiangolo.com/): REST endpoints (/city, /weather) with OpenAPI docs and validated access to 3D, geospatial, and time-series data.
- [Jupyter](https://jupyter.org/): Notebook environment (dependencies and env vars preloaded) for exploratory queries, ETL prototypes, reproducible analysis.
- [QWC2](https://github.com/qwc-services/qwc2): Web mapping client for 2D/3D visualization, layer styling, spatial inspection, quick dataset validation.
- [PostgREST](https://postgrest.org/): Auto-generated REST API over PostgreSQL schemas (tables, views, RPC) using DB roles for auth; rapid, lightweight data access without extra backend code.
- [pygeoapi](https://pygeoapi.io/): OGC API (Features/Coverages/Processes) server exposing PostGIS data via standards-based JSON & HTML endpoints for interoperable geospatial discovery and querying.

## infdb-db :material-database:
The core service **infdb-db** hosts the PostgreSQL database with extensions for geospatial, time series and graph data, serving as the central database within the platform. It handles data storage, retrieval, and management, ensuring integrity and high availability for connected services and tools.

![alt text](infdb-db.png)

**Technologies:**

    ### PostgreSQL
    The world's most advanced open source relational database. It handles the relational data modeling, concurrency, and reliability.

    ### PostGIS
    Adds support for geographic objects to the PostgreSQL database.
    -   **Features**: Spatial indexing, geometry types (Points, Lines, Polygons), and spatial query functions.
    -   **Use Case**: Storing building footprints, energy networks, and administrative boundaries.

    ### TimescaleDB
    An open-source time-series database optimized for fast ingest and complex queries.
    -   **Features**: Automatic partitioning (hypertables), continuous aggregates, and data retention policies.
    -   **Use Case**: Storing high-frequency sensor data, energy consumption profiles, and weather data.

    ### pgRouting
    Extends the PostGIS/PostgreSQL geospatial database to provide geospatial routing functionality.
    -   **Use Case**: Network analysis, shortest path calculations for district heating pipes or power lines.

    ### 3D City Database (3DCityDB)
    A free open source package for the PostgreSQL/PostGIS database system to store, represent, and manage virtual 3D city models on top of the CityGML standard.


### infdb-importer

## Management Interfaces

### pgAdmin
A web-based administration platform for PostgreSQL.
-   **Purpose**: GUI for database management, query execution, and server monitoring.
-   **Access**: Typically exposed on port `8080`.

## API Services

### FastAPI
Custom Python-based API server.
-   **Purpose**: Specialized endpoints for complex logic that cannot be handled by simple SQL views.
-   **Docs**: OpenAPI (Swagger) docs available at `/docs` endpoint.

### PostgREST
Serves a fully RESTful API from any existing PostgreSQL database.
-   **Purpose**: Instant CRUD APIs for tables and views in the `api` schema.

### pygeoapi
A Python implementation of the OGC API suite of standards.
-   **Purpose**: Standardized geospatial data access (OGC API Features).

## Visualization and Analysis

### QGIS Web Client (QWC2)
A responsive web application for QGIS Server.
-   **Purpose**: 2D/3D map visualization of stored geospatial data.

### Jupyter Lab
Interactive development environment.
-   **Purpose**: Python-based data analysis, prototyping, and notebook sharing.

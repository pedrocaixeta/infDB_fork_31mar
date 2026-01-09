---
icon: material/cogs
---

# Services

the infDB comes with a suite of pre-configured services to facilitate data management and access.


## Data

### infdb-db

The **infdb-db** is the central data storage engine, built upon **PostgreSQL**, relying on its robustness and extensibility.

![alt text](image.png)

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

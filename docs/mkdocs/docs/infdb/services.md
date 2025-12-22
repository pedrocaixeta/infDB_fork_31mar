---
icon: material/cogs
---

# Integrated Services

infDB comes with a suite of pre-configured services to facilitate data management and access.

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

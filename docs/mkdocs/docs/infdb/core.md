# infDB Core Architecture

The **infDB Core** is the central data storage engine, built upon **PostgreSQL**, relying on its robustness and extensibility.

## Key Technologies

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

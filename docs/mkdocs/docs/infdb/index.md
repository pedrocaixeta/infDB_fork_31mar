# infDB - Infrastructure and Energy Database

<p align="center">
  <img src="../assets/img/logo_infdb_text.png" alt="infDB logo" width="200"/>
</p>

The infDB is a modular and flexible data platform built on dockerized services that can be easily activated and configured for specific use cases. This architecture ensures portability across all platforms. By providing standardized interfaces and APIs, infDB fosters an extensible ecosystem that empowers users to integrate custom tools and workflows seamlessly.

Its architecture is composed of two main components:

<!-- : :material-database: **[infdb-db](infdb/core.md)** – PostgreSQL database for geospatial and time series data. -->
: :fontawesome-solid-gears: **[Services](infdb/services.md)** – Preconfigured dockerized open-source tools providing base functionality.
: :material-tools: **[Tools](infdb/tools.md)** – Software interacting with the infDB.

![infDB Overview](../assets/img/infdb-overview.png)

<!-- ## Core
The foundation is a PostgreSQL database enhanced with TimescaleDB, PostGIS, PGRouting, and the 3D City Database:

- [TimescaleDB](https://www.timescale.com/): Scalable time-series storage (weather, load, generation) with hypertables, compression, optional continuous aggregates.
- [PostGIS](https://postgis.net/): Spatial/geographic objects (buildings, parcels, networks) with geometry queries, projections, and spatial indexing.
- [PGRouting](https://pgrouting.org/): Network routing algorithms (shortest path, reachability) on road and infrastructure graphs for mobility and grid analysis.
- [3D City Database](http://www.3dcitydb.org/): Virtual 3D city model storage (buildings, terrain, infrastructure) with CityGML support, spatial indexing, and semantic queries for detailed urban analysis. -->

## Services
Services follow microservice architecture principles, enabling independent development and deployment while improving modularity, scalability, and adaptability.

**Available services:**

- [infdb-db](#infdb-db-material-database): Core PostgreSQL database with PostGIS, timescaledb, and pgrouting extensions; handles all central storage and queries.
- [infdb-importer](#infdb-importer-material-cloud-download): Automates the ingestion, structuring, and integration of external open data formats into the platform.
- [pgAdmin](https://www.pgadmin.org/): Web UI for inspecting schemas, running SQL, managing roles; auto-configured credentials.
- [FastAPI](https://fastapi.tiangolo.com/): REST endpoints (/city, /weather) with OpenAPI docs and validated access to 3D, geospatial, and time-series data.
- [Jupyter](https://jupyter.org/): Notebook environment (dependencies and env vars preloaded) for exploratory queries, ETL prototypes, reproducible analysis.
- [QWC2](https://github.com/qwc-services/qwc2): Web mapping client for 2D/3D visualization, layer styling, spatial inspection, quick dataset validation.
- [PostgREST](https://postgrest.org/): Auto-generated REST API over PostgreSQL schemas (tables, views, RPC) using DB roles for auth; rapid, lightweight data access without extra backend code.
- [pygeoapi](https://pygeoapi.io/): OGC API (Features/Coverages/Processes) server exposing PostGIS data via standards-based JSON & HTML endpoints for interoperable geospatial discovery and querying.

More details of available services can be found at **[Services](services.md)**.

## Tools
The infDB ecosystem includes a variety of tools designed to handle different aspects of data workflows. These so called tools are software that interact with infDB and process data through standardized, open interfaces. This modular approach allows you to tackle problems of any complexity by combining different tools into custom toolchains.

For a comprehensive list of integrated tools and additional information, see **[Tools](../tools/index.md)**.
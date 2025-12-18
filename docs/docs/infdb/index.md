# infDB - Infrastructure and Energy Database


## Core
The foundation is a PostgreSQL database enhanced with TimescaleDB, PostGIS, PGRouting, and the 3D City Database:

- [TimescaleDB](https://www.timescale.com/): Scalable time-series storage (weather, load, generation) with hypertables, compression, optional continuous aggregates.
- [PostGIS](https://postgis.net/): Spatial/geographic objects (buildings, parcels, networks) with geometry queries, projections, and spatial indexing.
- [PGRouting](https://pgrouting.org/): Network routing algorithms (shortest path, reachability) on road and infrastructure graphs for mobility and grid analysis.
- [3D City Database](http://www.3dcitydb.org/): Virtual 3D city model storage (buildings, terrain, infrastructure) with CityGML support, spatial indexing, and semantic queries for detailed urban analysis.

## Services
Integrated, preconfigured services extending the infDB:

- [pgAdmin](https://www.pgadmin.org/): Web UI for inspecting schemas, running SQL, managing roles; auto-configured credentials.
- [FastAPI](https://fastapi.tiangolo.com/): REST endpoints (/city, /weather) with OpenAPI docs and validated access to 3D, geospatial, and time-series data.
- [Jupyter](https://jupyter.org/): Notebook environment (dependencies and env vars preloaded) for exploratory queries, ETL prototypes, reproducible analysis.
- [QWC2](https://github.com/qwc-services/qwc2): Web mapping client for 2D/3D visualization, layer styling, spatial inspection, quick dataset validation.
- [PostgREST](https://postgrest.org/): Auto-generated REST API over PostgreSQL schemas (tables, views, RPC) using DB roles for auth; rapid, lightweight data access without extra backend code.
- [pygeoapi](https://pygeoapi.io/): OGC API (Features/Coverages/Processes) server exposing PostGIS data via standards-based JSON & HTML endpoints for interoperable geospatial discovery and querying.

These services provide core functionalities and support a seamless path from ingestion to analysis and visualization.

## Tools
Tools are external software, scripts, or workflows that interact with infDB through its standardized APIs and database schemas, enabling specialized analysis and processing capabilities.

#### Currently Integrated Tools
The following tools are currently integrated with infDB:

- **infDB-loader**: Containerized solution for automated ingestion of public open data for Germany
- **infDB-basedata**: Containerized pipeline for data transformation, validation, and enrichment
- **[pylovo](https://github.com/tum-ens/pylovo)**: Python tool for generating synthetic low-voltage distribution grids
- **[EnTiSe](https://github.com/tum-ens/EnTiSe)**: Python tool for energy time series generation and management

Additional community-developed or domain-specific tools can be easily integrated through infDB's standardized APIs and database schemas.
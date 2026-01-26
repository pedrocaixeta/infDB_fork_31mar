# infDB - Infrastructure and Energy Database

<p align="center">
  <img src="../assets/img/logo_infdb_text.png" alt="infDB logo" width="00"/>
</p>

The infDB is a modular and flexible data platform built on dockerized services that can be easily activated and configured for specific use cases. This architecture ensures portability across all platforms. By providing standardized interfaces and APIs, infDB fosters an extensible ecosystem that empowers users to integrate custom tools and workflows seamlessly.

![infDB Overview](../assets/img/infdb-overview.png)

###Review: Auch hier fände ich einen Bezug auf die Graphik gut###

Its architecture is composed of two main components:

: :fontawesome-solid-gears: **[Services](services.md)** – Dockerized open-source software providing base functionality.
: :material-tools: **[Tools](../tools/index.md)** – Software interacting with the infDB.

## Services
infDB services follow microservice architecture principles, enabling independent development and deployment while improving modularity, scalability, and adaptability.

![alt text](../usage/services.png)

- [infdb-db](services/infdb-db.md): Core PostgreSQL database with PostGIS, timescaledb, and pgrouting extensions; handles all central storage and queries.
- [infdb-importer](services/infdb-importer.md): Automates the ingestion, structuring, and integration of external open data formats into the platform.
- [pgAdmin](services/pgadmin.md): Web UI for inspecting schemas, running SQL, managing roles; auto-configured credentials.
- [FastAPI](services/fastapi.md): REST endpoints with OpenAPI docs and validated access to 3D, geospatial, and time-series data.
- [Jupyter](services/jupyter.md): Notebook environment for exploratory queries, ETL prototypes, reproducible analysis.
- [QWC2](services/qwc2.md): Web mapping client for 2D/3D visualization, layer styling, spatial inspection, quick dataset validation.
- [PostgREST](services/postgrest.md): Auto-generated REST API over PostgreSQL schemas using DB roles for auth; rapid, lightweight data access without extra backend code.
- [pygeoapi](services/pygeoapi.md): OGC API (Features/Coverages/Processes) server exposing PostGIS data via standards-based JSON & HTML endpoints for interoperable geospatial discovery and querying.
- [Opencloud](services/opencloud.md): Cloud infrastructure and deployment management for scalable service orchestration and resource provisioning.

## Tools
The infDB ecosystem includes a variety of tools designed to handle different aspects of data workflows. These so called tools are software that interact with infDB and process data through standardized, open interfaces. This modular approach allows you to tackle problems of any complexity by combining different tools into custom toolchains.

For a comprehensive list of integrated tools and additional information, see **[Tools](../tools/index.md)**.

## Python Package
Moreover, there is a python package `infdb` that can be used to interact with the infDB database and services. It provides functionalities for database connections, logging, configuration management, and utility functions. You can find more information about the package in the **[API -> pypackage](../api/pypackage.md)**.
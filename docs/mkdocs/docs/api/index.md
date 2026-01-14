# API Reference

The infDB platform offers several APIs to interact with the database and its services. Below is an overview of the main APIs available:

## [Python Package](pypackage.md)
The python package `infdb` provides a convenient way to interact with the infDB database and services programmatically. It includes functionalities for database connections, configuration management, logging, and utility functions.
The internal `infdb_package` for direct Python integration.

See [API -> pypackage](pypackage.md) for detailed usage instructions.

## FastAPI
The FastAPI is currently only forwarding to the PostgREST service.

- **Default endpoint:** `http:///host-address:8000`
- **Default docs endpoint:** `http://host-address:8000/docs`

See the official [FastAPI documentation](https://fastapi.tiangolo.com/) for more details.

## pygeoapi
The pygeoAPI provides OGC compliant API for geospatial data sharing and discovery. It supports standards-based access to geospatial data, making it compatible with GIS clients and web mapping applications. It also supports multiple data formats and includes interactive API documentation for testing endpoints.

- **Default endpoint:** `http:///host-address:8001`
- **Default docs endpoint:** `http://host-address:8001/openapi`

See the official [pygeoapi documentation](https://pygeoapi.io/) for more details.

## PostgREST
The PostgREST API provides an automatic REST API for PostgreSQL databases. It uses this for standard CRUD operations on tables and views. PostgREST automatically generates interactive Swagger documentation at `/docs` endpoint, allowing you to explore and test all available endpoints directly from your browser.

- **Default endpoint:** `http:///host-address:8002`
- **Default docs endpoint:** `http:///host-address:8002/docs`

See the official [PostgREST documentation](https://postgrest.org/en/stable/) for more details.
---
icon: material/database-edit
---

# Database Management

This section covers how the infDB manages its data schemas and extensions.

## Schema Organization

The infDB uses multiple schemas to organize data logically:

-   `public`: Default schema (try to avoid populating this with business logic).
-   `citydb`: Dedicated schema for 3DCityDB data.
-   `api`: Exposed views and functions for the PostgREST API.
-   `timeseries`: Optimized tables for time-series data.

## Extensions

The database is initialized with the following extensions enabled:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgrouting;
-- and others managed by initialization scripts
```

## Connection Details

By default, the database is accessible at:
-   **Host**: `localhost` (or `db` within Docker network)
-   **Port**: `5432`
-   **User**: `postgres` (or as configured in `.env`)

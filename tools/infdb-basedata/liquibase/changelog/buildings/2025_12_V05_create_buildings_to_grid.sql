--liquibase formatted sql
--changeset marvin.huang:1.0.5.0 labels:infdb-basedata,infdb-basedata-buildings
-- Create building to grid cell mapping
CREATE TABLE IF NOT EXISTS ${output_schema}.bld2grid (
	objectid text NOT NULL,
	id text NOT NULL,
	resolution_meters int4 NULL,
    CONSTRAINT bld2grid_pkey PRIMARY KEY (objectid, id)
);


--changeset marvin.huang:1.0.5.1 labels:infdb-basedata,infdb-basedata-buildings
-- Find nearest time series for each building
CREATE TABLE IF NOT EXISTS ${output_schema}.bld2ts
(
    id serial PRIMARY KEY,
	bld_objectid text NOT NULL,
	ts_metadata_id int4 NULL,
	ts_metadata_name text NULL,
	dist float8 NULL,
	geom public.geometry NULL
);

-- Add geometry column for visualization
ALTER TABLE ${output_schema}.bld2ts
    ADD COLUMN IF NOT EXISTS geom geometry;
CREATE INDEX IF NOT EXISTS idx_bld2ts_objectid ON ${output_schema}.bld2ts (bld_objectid);
ALTER TABLE ${output_schema}.bld2ts ADD CONSTRAINT UNIQUE_bld_ts UNIQUE (bld_objectid, ts_metadata_name);
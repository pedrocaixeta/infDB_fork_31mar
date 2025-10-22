-- Create building to grid cell mapping
CREATE TABLE {output_schema}.bld2grid AS
    SELECT
        b.objectid,
        g.id,
        g.resolution_meters
    FROM {input_schema}.buildings_lod2 b
    JOIN {input_schema}.grid_cells g
        ON ST_Intersects(ST_transform(g.geom, {EPSG}), (b.centroid));

-- Find nearest time series for each building
CREATE TABLE {output_schema}.bld2ts AS
SELECT bld.objectid AS bld_objectid,
       ts_metadata.id AS ts_metadata_id,
       ts_metadata.name AS ts_metadata_name,
       ts_metadata.dist
FROM {input_schema}.buildings_lod2 bld
         CROSS JOIN LATERAL (
    SELECT bld.objectid, ts_metadata.id, ts_metadata.name, ST_transform(ts_metadata.geom, {EPSG}) <-> bld.geom AS dist
    FROM {input_schema}.openmeteo_ts_metadata AS ts_metadata
    ORDER BY dist
    LIMIT 1
    ) ts_metadata;

-- Add geometry column for visualization
ALTER TABLE {output_schema}.bld2ts
    ADD COLUMN IF NOT EXISTS geom   geometry;
UPDATE {output_schema}.bld2ts
SET geom = ST_ShortestLine(bld.centroid, ST_transform(ts.geom, {EPSG}))
FROM {input_schema}.buildings_lod2 bld, {input_schema}.openmeteo_ts_metadata ts
WHERE bld2ts.ts_metadata_id = ts.id
  AND bld2ts.bld_objectid = bld.objectid
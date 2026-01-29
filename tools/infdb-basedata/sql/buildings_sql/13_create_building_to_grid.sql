-- Summary: Establishes relationships between buildings and other spatial
-- datasets. It creates mapping tables linking buildings to grid cells (bld2grid)
-- to find the nearest weather time series metadata (bld2ts) for each
-- building.

-- Create building to grid cell mapping
DELETE FROM {output_schema}.bld2grid target
WHERE NOT EXISTS (
    SELECT 1
    FROM {input_schema}.buildings_lod2 src
    WHERE src.objectid = target.objectid
--      AND src.gemeindeschluessel IN {list_gemeindeschluessel}
);

INSERT INTO {output_schema}.bld2grid (objectid, id, resolution_meters)
SELECT b.objectid,
       g.id,
       g.resolution_meters
FROM {input_schema}.buildings_lod2 b
JOIN {input_schema}.grid_cells g
    ON ST_Intersects(ST_transform(g.geom, {EPSG}), b.centroid)
WHERE b.gemeindeschluessel = '{ags}'
ON CONFLICT (objectid,id) DO UPDATE
SET resolution_meters = EXCLUDED.resolution_meters;


DELETE FROM {output_schema}.bld2ts target
WHERE NOT EXISTS (
    SELECT 1
    FROM {input_schema}.buildings_lod2 src
    WHERE src.objectid = target.bld_objectid
--      AND src.gemeindeschluessel IS IN {list_gemeindeschluessel}
);

-- Find nearest time series for each building
INSERT INTO {output_schema}.bld2ts (
    bld_objectid,
    ts_metadata_id,
    ts_metadata_name,
    dist
)
SELECT
    bld.objectid AS bld_objectid,
    ts.id        AS ts_metadata_id,
    ts.name      AS ts_metadata_name,
    ts.dist
FROM {input_schema}.buildings_lod2 bld
CROSS JOIN LATERAL (
    SELECT
        m.id,
        m.name,
        ST_Transform(m.geom, {EPSG}) <-> bld.geom AS dist
    FROM {input_schema}.openmeteo_ts_metadata m
    ORDER BY dist
    LIMIT 1
) ts
WHERE bld.gemeindeschluessel = '{ags}'
ON CONFLICT (bld_objectid,ts_metadata_name)
DO UPDATE
SET
    ts_metadata_id   = EXCLUDED.ts_metadata_id,
    dist             = EXCLUDED.dist
;


UPDATE {output_schema}.bld2ts
SET geom = ST_ShortestLine(bld.centroid, ST_transform(ts.geom, {EPSG}))
FROM {input_schema}.buildings_lod2 bld, {input_schema}.openmeteo_ts_metadata ts
WHERE bld.gemeindeschluessel = '{ags}'
  AND bld2ts.ts_metadata_id = ts.id
  AND bld2ts.bld_objectid = bld.objectid
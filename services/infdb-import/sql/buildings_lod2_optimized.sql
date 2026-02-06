-- SQL script to create and populate a buildings table from 3D City DB.
-- OPTIMIZED VERSION: Uses temp tables and single INSERT instead of multiple UPDATEs
-- This avoids scanning the entire table repeatedly, making it much faster for incremental loads

--------------------------------------------------------------
-- Create schema and table structure
--------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS {output_schema};

--------------------------------------------------------------
-- 02_create_buildings_table.sql
-- Create buildings table
--------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {output_schema}.building_lod2
(
    id                       SERIAL PRIMARY KEY,
    feature_id               integer UNIQUE,
    objectid                 text,
    gemeindeschluessel       text,
    objectclass_id           int,
    height                   double precision,
    groundsurface_flaeche    double precision,
    storeysaboveground       integer,
    building_function_code   text NOT NULL,
    zip_codes                text[],
    streets                  text[],
    house_numbers            text[],
    cities                   text[],
    countries                text[],
    states                   text[],
    geom                     geometry,
    centroid                 geometry
);

CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.building_lod2 USING GIST (geom);
CREATE INDEX IF NOT EXISTS building_centroid_idx ON {output_schema}.building_lod2 USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_building_type_check ON {output_schema}.building_lod2 (id, objectid, building_function_code);
CREATE INDEX IF NOT EXISTS building_lod2_feature_id_idx ON {output_schema}.building_lod2 (feature_id);
CREATE INDEX IF NOT EXISTS building_lod2_objectid_idx ON {output_schema}.building_lod2 (objectid);
CREATE INDEX IF NOT EXISTS building_lod2_gks_idx ON {output_schema}.building_lod2 (gemeindeschluessel);

--------------------------------------------------------------
-- Step 1: Create temp table with base building data
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_buildings_base;
CREATE TEMP TABLE tmp_buildings_base AS
WITH gemeindeschluessel_data AS (
    SELECT feature_id, val_string
    FROM property
    WHERE name = 'Gemeindeschluessel'
      AND val_string IN ({gemeindeschluessel})
)
SELECT 
    f.id AS feature_id,
    f.objectclass_id,
    f.objectid,
    gsd.val_string as gemeindeschluessel,
    p.val_string as building_function_code
FROM feature f
JOIN property p ON f.id = p.feature_id
JOIN gemeindeschluessel_data gsd ON f.id = gsd.feature_id
WHERE f.objectclass_id = 901
  AND p.name = 'function'
  AND p.val_string LIKE '31001_%';

CREATE INDEX ON tmp_buildings_base(feature_id);
CREATE INDEX ON tmp_buildings_base(objectid);
ANALYZE tmp_buildings_base;

--------------------------------------------------------------
-- Step 2: Create temp table with height data (deduplicated)
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_height_data;
CREATE TEMP TABLE tmp_height_data AS
SELECT DISTINCT ON (feature_id) 
    feature_id, 
    val_double AS height
FROM property p
WHERE p.name = 'value'
  AND p.parent_id IN (SELECT id FROM property WHERE name = 'height')
  AND p.feature_id IN (SELECT feature_id FROM tmp_buildings_base)
ORDER BY feature_id, val_double DESC NULLS LAST;

CREATE INDEX ON tmp_height_data(feature_id);
ANALYZE tmp_height_data;

--------------------------------------------------------------
-- Step 3: Create temp table with floor number data (deduplicated)
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_floor_data;
CREATE TEMP TABLE tmp_floor_data AS
SELECT DISTINCT ON (feature_id)
    feature_id, 
    val_int AS storeysaboveground
FROM property
WHERE name = 'storeysAboveGround'
  AND feature_id IN (SELECT feature_id FROM tmp_buildings_base)
ORDER BY feature_id, val_int DESC NULLS LAST;

CREATE INDEX ON tmp_floor_data(feature_id);
ANALYZE tmp_floor_data;

--------------------------------------------------------------
-- Step 4: Create temp table with ground surface geometry (deduplicated)
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_ground_data;
CREATE TEMP TABLE tmp_ground_data AS
WITH group_901 AS (
    SELECT
        b.feature_id,
        b.objectid,
        gd.geometry_properties ->> 'objectId' AS root_object_id,
        child ->> 'objectId' AS child_object_id
    FROM tmp_buildings_base b
    JOIN feature f ON f.id = b.feature_id
    JOIN geometry_data gd ON f.id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
    WHERE f.objectclass_id = 901
),
group_710 AS (
    SELECT
        feature.objectid AS objectid,
        gd.geometry_properties ->> 'objectId' AS root_object_id,
        child ->> 'objectId' AS child_object_id,
        gd.geometry
    FROM feature
    JOIN geometry_data gd ON feature.id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
    WHERE objectclass_id = 710
),
all_surfaces AS (
    SELECT
        group_901.feature_id,
        group_710.geometry AS geom,
        ST_Area(group_710.geometry) AS area,
        ROW_NUMBER() OVER (PARTITION BY group_901.feature_id ORDER BY ST_Area(group_710.geometry) DESC NULLS LAST) AS rn
    FROM group_901
    JOIN group_710 ON group_901.child_object_id = group_710.child_object_id
)
SELECT 
    feature_id,
    geom,
    area
FROM all_surfaces
WHERE rn = 1;

CREATE INDEX ON tmp_ground_data(feature_id);
ANALYZE tmp_ground_data;

--------------------------------------------------------------
-- Step 5: Create temp table with address data (all addresses aggregated)
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_address_data;
CREATE TEMP TABLE tmp_address_data AS
SELECT
    b.feature_id,
    ARRAY_AGG(DISTINCT a.city ORDER BY a.city) FILTER (WHERE a.city IS NOT NULL) AS cities,
    ARRAY_AGG(DISTINCT a.country ORDER BY a.country) FILTER (WHERE a.country IS NOT NULL) AS countries,
    ARRAY_AGG(DISTINCT a.zip_code ORDER BY a.zip_code) FILTER (WHERE a.zip_code IS NOT NULL) AS zip_codes,
    ARRAY_AGG(DISTINCT a.state ORDER BY a.state) FILTER (WHERE a.state IS NOT NULL) AS states,
    ARRAY_AGG(DISTINCT regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '') ORDER BY regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '')) FILTER (WHERE a.street IS NOT NULL) AS streets,
    ARRAY_AGG(DISTINCT (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] ORDER BY (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1]) FILTER (WHERE (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] IS NOT NULL) AS house_numbers
FROM tmp_buildings_base b
JOIN property p ON b.feature_id = p.feature_id
JOIN address a ON p.val_address_id = a.id
GROUP BY b.feature_id;

CREATE INDEX ON tmp_address_data(feature_id);
ANALYZE tmp_address_data;

--------------------------------------------------------------
-- Step 6: Single INSERT with all data and ON CONFLICT for updates
--------------------------------------------------------------
INSERT INTO {output_schema}.building_lod2 (
    feature_id,
    objectclass_id,
    objectid,
    gemeindeschluessel,
    building_function_code,
    height,
    groundsurface_flaeche,
    geom,
    centroid,
    storeysaboveground,
    streets,
    house_numbers,
    cities,
    countries,
    zip_codes,
    states
)
SELECT
    b.feature_id,
    b.objectclass_id,
    b.objectid,
    b.gemeindeschluessel,
    b.building_function_code,
    h.height,
    g.area,
    g.geom,
    ST_Centroid(g.geom),
    GREATEST(COALESCE(f.storeysaboveground, 1), 1),
    a.streets,
    a.house_numbers,
    a.cities,
    a.countries,
    a.zip_codes,
    a.states
FROM tmp_buildings_base b
LEFT JOIN tmp_height_data h ON b.feature_id = h.feature_id
LEFT JOIN tmp_ground_data g ON b.feature_id = g.feature_id
LEFT JOIN tmp_floor_data f ON b.feature_id = f.feature_id
LEFT JOIN tmp_address_data a ON b.feature_id = a.feature_id
ON CONFLICT (feature_id) DO UPDATE SET
    objectclass_id = EXCLUDED.objectclass_id,
    objectid = EXCLUDED.objectid,
    gemeindeschluessel = EXCLUDED.gemeindeschluessel,
    building_function_code = EXCLUDED.building_function_code,
    height = EXCLUDED.height,
    groundsurface_flaeche = EXCLUDED.groundsurface_flaeche,
    geom = EXCLUDED.geom,
    centroid = EXCLUDED.centroid,
    storeysaboveground = EXCLUDED.storeysaboveground,
    streets = EXCLUDED.streets,
    house_numbers = EXCLUDED.house_numbers,
    cities = EXCLUDED.cities,
    countries = EXCLUDED.countries,
    zip_codes = EXCLUDED.zip_codes,
    states = EXCLUDED.states;

--------------------------------------------------------------
-- Step 7: Cleanup temp tables
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_buildings_base;
DROP TABLE IF EXISTS tmp_height_data;
DROP TABLE IF EXISTS tmp_floor_data;
DROP TABLE IF EXISTS tmp_ground_data;
DROP TABLE IF EXISTS tmp_address_data;

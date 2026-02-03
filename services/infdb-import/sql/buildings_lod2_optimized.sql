-- SQL script to create and populate a buildings table from 3D City DB.
-- OPTIMIZED VERSION: Uses temporary tables with indices for better performance.
-- Author: Optimization based on original by Patrick Buchenberg
-- Date: 2026-02-03

--------------------------------------------------------------
-- 00_create_schema.sql
-- Create output schema if not exists
--------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS {output_schema};

--------------------------------------------------------------
-- 01_create_temp_indices.sql
-- Create indices on source tables to speed up queries
-- (These are created once and reused across multiple runs)
--------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_property_name ON property(name);
CREATE INDEX IF NOT EXISTS idx_property_feature_id ON property(feature_id);
CREATE INDEX IF NOT EXISTS idx_property_parent_id ON property(parent_id);
CREATE INDEX IF NOT EXISTS idx_property_val_string ON property(val_string);
CREATE INDEX IF NOT EXISTS idx_feature_objectclass_id ON feature(objectclass_id);
CREATE INDEX IF NOT EXISTS idx_geometry_data_feature_id ON geometry_data(feature_id);

--------------------------------------------------------------
-- 02_create_temp_tables.sql
-- Pre-filter data into temporary tables with indices
-- This avoids repeated full table scans
--------------------------------------------------------------

-- Temp table: Buildings for current gemeindeschluessel scope
DROP TABLE IF EXISTS tmp_buildings_scope;
CREATE TEMP TABLE tmp_buildings_scope AS
SELECT f.id AS feature_id,
       f.objectclass_id,
       f.objectid,
       gsd.val_string AS gemeindeschluessel,
       p.val_string AS building_function_code
FROM feature f
JOIN property p ON f.id = p.feature_id
JOIN property gsd ON f.id = gsd.feature_id
WHERE f.objectclass_id = 901  -- building
  AND p.name = 'function'
  AND p.val_string LIKE '31001_%'
  AND gsd.name = 'Gemeindeschluessel'
  AND gsd.val_string IN ({gemeindeschluessel});

CREATE INDEX ON tmp_buildings_scope(feature_id);
CREATE INDEX ON tmp_buildings_scope(objectid);
CREATE INDEX ON tmp_buildings_scope(gemeindeschluessel);

-- Temp table: Height data
DROP TABLE IF EXISTS tmp_height_data;
CREATE TEMP TABLE tmp_height_data AS
SELECT p.feature_id, p.val_double AS height
FROM property p
WHERE p.name = 'value'
  AND p.parent_id IN (SELECT id FROM property WHERE name = 'height')
  AND p.feature_id IN (SELECT feature_id FROM tmp_buildings_scope);

CREATE INDEX ON tmp_height_data(feature_id);

-- Temp table: Floor number data
DROP TABLE IF EXISTS tmp_floor_number_data;
CREATE TEMP TABLE tmp_floor_number_data AS
SELECT feature_id, val_int AS storeysAboveGround
FROM property
WHERE name = 'storeysAboveGround'
  AND feature_id IN (SELECT feature_id FROM tmp_buildings_scope);

CREATE INDEX ON tmp_floor_number_data(feature_id);

-- Temp table: Address data
DROP TABLE IF EXISTS tmp_address_data;
CREATE TEMP TABLE tmp_address_data AS
SELECT b.feature_id,
       a.city,
       a.country,
       a.zip_code,
       a.state,
       regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '') AS street,
       (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] AS house_number
FROM tmp_buildings_scope b
JOIN property p ON b.feature_id = p.feature_id
JOIN address a ON p.val_address_id = a.id;

CREATE INDEX ON tmp_address_data(feature_id);

-- Temp table: Ground surface geometry data (the most expensive query)
DROP TABLE IF EXISTS tmp_ground_data;
CREATE TEMP TABLE tmp_ground_data AS
WITH group_901 AS (
    SELECT
        b.objectid AS objectid,
        b.feature_id AS feature_id,
        gd.geometry_properties ->> 'objectId' AS root_object_id,
        child ->> 'objectId' AS child_object_id
    FROM tmp_buildings_scope b
    JOIN geometry_data gd ON b.feature_id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
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
)
SELECT
    group_901.objectid AS objectid,
    group_901.feature_id AS feature_id,
    group_710.objectid AS ground_surface_objectid,
    group_710.geometry AS geom,
    ST_Area(group_710.geometry) AS area
FROM group_901
JOIN group_710 ON group_901.child_object_id = group_710.child_object_id;

CREATE INDEX ON tmp_ground_data(objectid);
CREATE INDEX ON tmp_ground_data(feature_id);

--------------------------------------------------------------
-- 03_create_buildings_table.sql
-- Create buildings table (if not exists)
--------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {output_schema}.buildings_lod2
(
    id                       SERIAL PRIMARY KEY,
    feature_id               integer,
    objectid                 text,
    gemeindeschluessel       text,
    objectclass_id           int,
    height                   double precision,
    groundsurface_flaeche    double precision,
    storeysaboveground       integer,
    building_function_code   text NOT NULL,
    zip_code                 text,
    street                   text,
    house_number             text,
    city                     text,
    country                  text,
    state                    text,
    geom                     geometry,
    centroid                 geometry
);

CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.buildings_lod2 USING GIST (geom);
CREATE INDEX IF NOT EXISTS building_centroid_idx ON {output_schema}.buildings_lod2 USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_building_type_check ON {output_schema}.buildings_lod2 (id, objectid, building_function_code);
CREATE INDEX IF NOT EXISTS buildings_lod2_feature_id_idx ON {output_schema}.buildings_lod2 (feature_id);
CREATE INDEX IF NOT EXISTS buildings_lod2_objectid_idx ON {output_schema}.buildings_lod2 (objectid);
CREATE INDEX IF NOT EXISTS buildings_lod2_gks_idx ON {output_schema}.buildings_lod2 (gemeindeschluessel);

--------------------------------------------------------------
-- 04_insert_buildings.sql
-- Insert buildings with all data in a single pass
-- Using LEFT JOINs to temp tables for efficiency
--------------------------------------------------------------
INSERT INTO {output_schema}.buildings_lod2 (
    feature_id,
    objectclass_id,
    objectid,
    gemeindeschluessel,
    building_function_code,
    height,
    groundsurface_flaeche,
    geom,
    centroid,
    storeysAboveGround,
    street,
    house_number,
    city,
    country,
    zip_code,
    state
)
SELECT
    b.feature_id,
    b.objectclass_id,
    b.objectid,
    b.gemeindeschluessel,
    b.building_function_code,
    h.height,
    g.area AS groundsurface_flaeche,
    g.geom,
    ST_Centroid(g.geom) AS centroid,
    GREATEST(COALESCE(f.storeysAboveGround, 1), 1) AS storeysAboveGround,
    a.street,
    a.house_number,
    a.city,
    a.country,
    a.zip_code,
    a.state
FROM tmp_buildings_scope b
LEFT JOIN tmp_height_data h ON b.feature_id = h.feature_id
LEFT JOIN tmp_ground_data g ON b.objectid = g.objectid
LEFT JOIN tmp_floor_number_data f ON b.feature_id = f.feature_id
LEFT JOIN tmp_address_data a ON b.feature_id = a.feature_id;

--------------------------------------------------------------
-- 05_cleanup.sql
-- Drop temporary tables to free memory
--------------------------------------------------------------
DROP TABLE IF EXISTS tmp_buildings_scope;
DROP TABLE IF EXISTS tmp_height_data;
DROP TABLE IF EXISTS tmp_ground_data;
DROP TABLE IF EXISTS tmp_floor_number_data;
DROP TABLE IF EXISTS tmp_address_data;

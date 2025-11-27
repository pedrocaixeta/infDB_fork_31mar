-- SQL script to create and populate a buildings table from 3D City DB.

--------------------------------------------------------------
-- 00_cleanup.sql
-- Cleanup existing buildings table if it exists
--------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS {output_schema};
DROP TABLE IF EXISTS {output_schema}.buildings_lod2;

--------------------------------------------------------------
-- 02_create_buildings_table.sql
-- Create buildings table
--------------------------------------------------------------
CREATE TABLE {output_schema}.buildings_lod2
(
    id                SERIAL PRIMARY KEY,
    feature_id        integer,
    objectid          text,
    gemeindeschluessel text,
    objectclass_id    int,
    height            double precision,
    groundsurface_flaeche        double precision,
    storeysaboveground      integer,
    -- building_use      text NOT NULL,
    building_function_code   text NOT NULL,
    -- building_type     text,
    -- occupants         int,
    -- households        int,
    -- construction_year text,
    zip_code          text,
    street            text,
    house_number     text,
    city              text,
    country          text,
    state            text,
    geom              geometry,
    centroid          geometry
);
CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.buildings_lod2 USING GIST (geom);
CREATE INDEX IF NOT EXISTS building_centroid_idx ON {output_schema}.buildings_lod2 USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_building_type_check ON {output_schema}.buildings_lod2 (id, objectid, building_function_code);

--------------------------------------------------------------
-- 03_fill_id_object_id_building.sql
-- Fill buildings with corresponding identifier (id, feature_id, objectid and building_function_code columns)
--------------------------------------------------------------
INSERT INTO {output_schema}.buildings_lod2 (feature_id, objectclass_id, objectid, building_function_code)
SELECT f.id AS 
       feature_id,
       f.objectclass_id,
       f.objectid,
    --    {output_schema}.classify_building_use(p.val_string) as building_use,
       p.val_string                                     as building_function_code
FROM feature f
         JOIN property p ON f.id = p.feature_id
WHERE f.objectclass_id = 901 -- =building
  AND p.name = 'function'
  AND p.val_string LIKE '31001_%'  -- only allow buildings
  -- AND p.val_string <> '31001_2463' -- exclude garages
  -- AND p.val_string <> '31001_2513' -- exclude water containers
-- ORDER BY f.id
;

-----------------------------------------------------------------
-- 0X_fill_gemeindeschluessel.sql
-- fill gemeindeschluessel column
-----------------------------------------------------------------
WITH gemeindeschluessel_data AS (SELECT feature_id, val_string
                           FROM property
                           WHERE name = 'Gemeindeschluessel')
UPDATE {output_schema}.buildings_lod2 b
SET gemeindeschluessel = fnd.val_string
FROM gemeindeschluessel_data fnd
WHERE b.feature_id = fnd.feature_id;

--------------------------------------------------------------
-- 04_fill_height.sql
-- fill height column
--------------------------------------------------------------
WITH height_data AS (SELECT p.feature_id, p.val_double
                     FROM property p
                     WHERE p.name = 'value'
                       AND p.parent_id IN (SELECT id FROM property WHERE name = 'height'))
UPDATE {output_schema}.buildings_lod2 b
SET height = hd.val_double
FROM height_data hd
WHERE b.feature_id = hd.feature_id;

-- -- delete buildings below a height threshold
-- DELETE
-- FROM {output_schema}.buildings_lod2
-- WHERE height < 3.5;


--------------------------------------------------------------
-- 05_fill_floor_area_geom.sql
-- fill geom and floor_area columns
--------------------------------------------------------------
WITH ground_data AS (
    -- SELECT 
    --     regexp_replace(f.objectid, '_[^_]*-.*$', '')    as building_objectid,
    --     cast(p.val_string as double precision)          as area,
    --     ST_Force2D(gd.geometry)                         as geom,
    --     f.id as feature_id
    -- FROM feature f
    --       JOIN geometry_data gd ON f.id = gd.feature_id
    --       JOIN property p ON f.id = p.feature_id
    -- WHERE f.objectclass_id = 710 -- GroundSurface
    -- AND p.name = 'Flaeche'

    WITH group_901 AS (
    SELECT
        feature.objectid AS objectid,
        feature.id as feature_id,
        gd.geometry_properties ->> 'objectId' as root_object_id,
        child ->> 'objectId' as child_object_id
    FROM feature
             JOIN geometry_data gd ON feature.id = gd.feature_id
             CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
    WHERE objectclass_id = 901
    ),
    group_710 AS (
         SELECT
             feature.objectid AS objectid,
             gd.geometry_properties ->> 'objectId' as root_object_id,
             child ->> 'objectId' as child_object_id,
            gd.geometry
         FROM feature
                  JOIN geometry_data gd ON feature.id = gd.feature_id
                  CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
         WHERE objectclass_id = 710
    )
    SELECT
        group_901.objectid as objectid,
        group_901.feature_id as feature_id,
        group_710.objectid as ground_surface_objectid,
        group_710.geometry as geom,  -- ST_MakeValid(st_force2d( group_710.geometry))
        st_area(group_710.geometry) as area
    FROM group_901
            JOIN group_710
                  ON group_901.child_object_id = group_710.child_object_id
)
UPDATE {output_schema}.buildings_lod2 b
SET groundsurface_flaeche = ground_data.area,
    geom       = ground_data.geom,
    centroid   = ST_Centroid(ground_data.geom)
FROM ground_data
WHERE b.objectid = ground_data.objectid;
--WHERE b.feature_id = ground_data.feature_id;

-- -- delete buildings below an area threshold
-- DELETE
-- FROM {output_schema}.buildings_lod2
-- WHERE buildings.floor_area < 12;

-----------------------------------------------------------------
-- 07_fill_floor_number.sql
-- fill floor_number column
-----------------------------------------------------------------
WITH floor_number_data AS (SELECT feature_id, val_int
                           FROM property
                           WHERE name = 'storeysAboveGround')
UPDATE {output_schema}.buildings_lod2 b
SET storeysAboveGround = GREATEST(fnd.val_int, 1)
FROM floor_number_data fnd
WHERE b.feature_id = fnd.feature_id;

-- -- fill in missing floor_number values
-- WITH average_floor_height AS (SELECT building_use_id,
--                                      PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY (height / floor_number) ) as height_per_floor
--                               FROM {output_schema}.buildings_lod2
--                               GROUP BY building_use_id)
-- UPDATE {output_schema}.buildings_lod2 b
-- SET storeysAboveGround = GREATEST(ROUND(height / COALESCE(afh.height_per_floor, height)), 1)
-- FROM average_floor_height afh
-- WHERE b.storeysAboveGround IS NULL
--   AND b.building_use_id = afh.building_use_id;

---------------------------------------------------------------
-- 99_fill_address_id.sql
-- fill address related columns
----------------------------------------------------------------
WITH split_addresses AS (
  SELECT b.feature_id,
         a.city,
         a.country,
         a.zip_code,
         a.state,
         regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '') AS street,
         (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] AS house_number,
         a.street AS original_street,
         unnest(string_to_array(a.street, ';')) AS individual_street
  FROM {output_schema}.buildings_lod2 b
  JOIN property p ON b.feature_id = p.feature_id
  JOIN address  a ON p.val_address_id = a.id
)
UPDATE {output_schema}.buildings_lod2 b
SET street = sad.street,
    house_number = sad.house_number,
    city = sad.city,
    country = sad.country,
    zip_code = sad.zip_code,
    state = sad.state
    -- original_street = sad.original_street
FROM split_addresses sad
WHERE b.feature_id = sad.feature_id;

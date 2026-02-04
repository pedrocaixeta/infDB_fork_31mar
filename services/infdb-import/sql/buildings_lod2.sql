-- SQL script to create and populate a buildings table from 3D City DB.

--------------------------------------------------------------
-- 00_cleanup.sql
-- Cleanup existing buildings table if it exists
--------------------------------------------------------------
DROP SCHEMA IF EXISTS {output_schema} CASCADE;
CREATE SCHEMA IF NOT EXISTS {output_schema};

--------------------------------------------------------------
-- 02_create_buildings_table.sql
-- Create buildings table partition
--------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} PARTITION OF opendata.building_lod2 FOR VALUES IN ('{ags_id}');
-- done in parent table
-- CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.{table_name} USING GIST (geom);
-- CREATE INDEX IF NOT EXISTS building_centroid_idx ON {output_schema}.{table_name} USING GIST (centroid);
-- CREATE INDEX IF NOT EXISTS idx_building_type_check ON {output_schema}.{table_name} (id, objectid, building_function_code);
-- CREATE INDEX IF NOT EXISTS {table_name}_feature_id_idx ON {output_schema}.{table_name} (feature_id);
-- CREATE INDEX IF NOT EXISTS {table_name}_gks_objectid_idx ON {output_schema}.{table_name} (gemeindeschluessel, objectid);

--------------------------------------------------------------
-- 03_fill_id_object_id_building.sql
-- Fill buildings with corresponding identifier (id, feature_id, objectid and building_function_code columns)
--------------------------------------------------------------
WITH gemeindeschluessel_data AS (SELECT feature_id, val_string
                           FROM property
                           WHERE name = 'Gemeindeschluessel')

INSERT INTO {output_schema}.{table_name} (feature_id, objectclass_id, objectid, gemeindeschluessel, ags_id, building_function_code)
SELECT f.id AS 
       feature_id,
       f.objectclass_id,
       f.objectid,
       gsd.val_string as gemeindeschluessel,
       substring(gsd.val_string, 1, 2) as ags_id,
    --    {output_schema}.classify_building_use(p.val_string) as building_use,
       p.val_string                                     as building_function_code
FROM feature f
         JOIN property p ON f.id = p.feature_id
JOIN gemeindeschluessel_data gsd ON f.id = gsd.feature_id
WHERE f.objectclass_id = 901 -- =building
  AND p.name = 'function'
  AND p.val_string LIKE '31001_%'  -- only allow buildings
  -- AND p.val_string <> '31001_2463' -- exclude garages
  -- AND p.val_string <> '31001_2513' -- exclude water containers
-- ORDER BY f.id
AND gsd.val_string IN ({gemeindeschluessel});  -- filter by gemeindeschluessel

-- -----------------------------------------------------------------
-- -- 0X_fill_gemeindeschluessel.sql
-- -- fill gemeindeschluessel column
-- -----------------------------------------------------------------
-- -- depricated, now done in 03_fill_id_object_id_building.sql
-- -- just kept for reference
-- -- Patrick Buchenberg 2024-12-19
-- WITH gemeindeschluessel_data AS (SELECT feature_id, val_string
--                            FROM property
--                            WHERE name = 'Gemeindeschluessel')
-- UPDATE {output_schema}.{table_name} b
-- SET gemeindeschluessel = fnd.val_string
-- FROM gemeindeschluessel_data fnd
-- WHERE b.feature_id = fnd.feature_id;

--------------------------------------------------------------
-- 04_fill_height.sql
-- fill height column
--------------------------------------------------------------
WITH height_data AS (SELECT p.feature_id, p.val_double
                     FROM property p
                     WHERE p.name = 'value'
                       AND p.parent_id IN (SELECT id FROM property WHERE name = 'height'))
UPDATE {output_schema}.{table_name} b
SET height = hd.val_double
FROM height_data hd
WHERE b.feature_id = hd.feature_id
  AND b.gemeindeschluessel IN ({gemeindeschluessel});

-- -- delete buildings below a height threshold
-- DELETE
-- FROM {output_schema}.{table_name}
-- WHERE height < 3.5;


--------------------------------------------------------------
-- 05_fill_floor_area_geom.sql
-- fill geom and floor_area columns
--------------------------------------------------------------
--------------------------------------------------------------
-- This query extracts building ground surfaces from the 3D City DB
-- geometry hierarchy by:
-- 1. Finding buildings (objectclass_id 901) and their objectids of their child surface geometries
-- 2. Matching those references to ground surfaces (objectclass_id 710)
-- 3. Calculating the area of each ground surface geometry
-- 4. Updating the buildings table with:
--    - groundsurface_flaeche: ground surface area in square meters
--    - geom: the ground surface geometry (footprint)
--    - centroid: calculated center point of the geometry
-- Author: Patrick Buchenberg - Hackthon@Darmstadt
--------------------------------------------------------------
WITH ground_data AS (
    WITH group_901 AS (
        SELECT
            f.objectid AS objectid,
            f.id AS feature_id,
            gd.geometry_properties ->> 'objectId' AS root_object_id,
            child ->> 'objectId' AS child_object_id
        FROM {output_schema}.{table_name} b
        JOIN feature f ON f.id = b.feature_id
        JOIN geometry_data gd ON f.id = gd.feature_id
        CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
        WHERE f.objectclass_id = 901
          AND b.gemeindeschluessel IN ({gemeindeschluessel})
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
        group_710.geometry as geom,
        st_area(group_710.geometry) as area
    FROM group_901
            JOIN group_710
                  ON group_901.child_object_id = group_710.child_object_id
)
UPDATE {output_schema}.{table_name} b
SET groundsurface_flaeche = ground_data.area,
    geom       = ground_data.geom,
    centroid   = ST_Centroid(ground_data.geom)
FROM ground_data
WHERE b.objectid = ground_data.objectid
  AND b.gemeindeschluessel IN ({gemeindeschluessel});

-- -- delete buildings below an area threshold
-- DELETE
-- FROM {output_schema}.{table_name}
-- WHERE buildings.floor_area < 12;

-----------------------------------------------------------------
-- 07_fill_floor_number.sql
-- fill floor_number column
-----------------------------------------------------------------
WITH floor_number_data AS (SELECT feature_id, val_int
                           FROM property
                           WHERE name = 'storeysAboveGround')
UPDATE {output_schema}.{table_name} b
SET storeysAboveGround = GREATEST(fnd.val_int, 1)
FROM floor_number_data fnd
WHERE b.feature_id = fnd.feature_id
  AND b.gemeindeschluessel IN ({gemeindeschluessel});

-- -- fill in missing floor_number values
-- WITH average_floor_height AS (SELECT building_use_id,
--                                      PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY (height / floor_number) ) as height_per_floor
--                               FROM {output_schema}.{table_name}
--                               GROUP BY building_use_id)
-- UPDATE {output_schema}.{table_name} b
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
  FROM {output_schema}.{table_name} b
  JOIN property p ON b.feature_id = p.feature_id
  JOIN address  a ON p.val_address_id = a.id
)
UPDATE {output_schema}.{table_name} b
SET street = sad.street,
    house_number = sad.house_number,
    city = sad.city,
    country = sad.country,
    zip_code = sad.zip_code,
    state = sad.state
    -- original_street = sad.original_street
FROM split_addresses sad
WHERE b.feature_id = sad.feature_id
  AND b.gemeindeschluessel IN ({gemeindeschluessel});

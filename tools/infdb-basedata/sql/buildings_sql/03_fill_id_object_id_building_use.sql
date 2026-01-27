-- Summary: Syncs the buildings table with buildings_lod2 source data.
-- It inserts new buildings, updates existing ones, and removes obsolete entries.
-- Key attributes like objectid, building_use, and address information are
-- populated while resetting derived columns.

-- Delete objectid which do not exist anymore
DELETE FROM {output_schema}.buildings target
WHERE NOT EXISTS (
    SELECT 1
    FROM {input_schema}.buildings_lod2 src
    WHERE src.objectid = target.objectid
--      AND src.gemeindeschluessel IS IN {list_gemeindeschluessel}
);

-- Fill id, objectid and building use columns
INSERT INTO {output_schema}.buildings (feature_id, objectid, building_use, building_use_id, street, house_number, gemeindeschluessel)
SELECT
       b.feature_id,
       b.objectid,
       {output_schema}.classify_building_use(b.building_function_code) as building_use,
       b.building_function_code                                     as building_use_id,
       b.street,
       b.house_number,
       b.gemeindeschluessel
FROM {input_schema}.buildings_lod2 b
-- WHERE b.gemeindeschluessel IS IN {list_gemeindeschluessel}
--    WHERE src.gemeindeschluessel IS IN {list_gemeindeschluessel}
  WHERE building_function_code LIKE '31001_%'  -- only allow buildings
  AND building_function_code <> '31001_2463' -- exclude garages
  AND building_function_code <> '31001_2513' -- exclude water containers
  AND b.geom IS NOT NULL
ON CONFLICT (objectid) DO UPDATE
SET
    feature_id       = EXCLUDED.feature_id,
    building_use     = EXCLUDED.building_use,
    building_use_id  = EXCLUDED.building_use_id,
    street           = EXCLUDED.street,
    house_number     = EXCLUDED.house_number,
    gemeindeschluessel = EXCLUDED.gemeindeschluessel,
    -- set remaining columns to NULL
    height           = NULL,
    floor_area       = NULL,
    floor_number     = NULL,
    building_type    = NULL,
    occupants        = NULL,
    households       = NULL,
    construction_year= NULL,
    postcode         = NULL,
    address_street_id= NULL,
    geom             = NULL,
    centroid         = NULL
;

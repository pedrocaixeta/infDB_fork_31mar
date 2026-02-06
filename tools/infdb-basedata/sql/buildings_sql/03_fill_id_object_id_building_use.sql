-- Summary: Syncs the buildings table with building_lod2 source data.
-- It inserts new buildings, updates existing ones, and removes obsolete entries.
-- Key attributes like objectid, building_use, and address information are
-- populated while resetting derived columns.


-- Pre-calculate the source dataset in a temp table with proper indexing
DROP TABLE IF EXISTS temp_lod2_data;
CREATE TEMP TABLE temp_lod2_data AS
SELECT
       b.feature_id,
       b.objectid,
       {output_schema}.classify_building_use(b.building_function_code) as building_use,
       b.building_function_code                                     as building_use_id,
       b.street,
       b.house_number,
       b.gemeindeschluessel
FROM {input_schema}.building_lod2 b
WHERE b.gemeindeschluessel = '{ags}'
  AND building_function_code LIKE '31001_%'  -- only allow buildings
  AND building_function_code <> '31001_2463' -- exclude garages
  AND building_function_code <> '31001_2513' -- exclude water containers
  AND b.geom IS NOT NULL;

CREATE UNIQUE INDEX idx_temp_lod2_data_oid ON temp_lod2_data (objectid);
ANALYZE temp_lod2_data;

-- Delete objectid which do not exist anymore (only for current AGS region)
DELETE FROM {output_schema}.buildings target
WHERE target.gemeindeschluessel = '{ags}'
  AND NOT EXISTS (
    SELECT 1
    FROM temp_lod2_data src
    WHERE src.objectid = target.objectid
);

-- Update existing records (Bulk Update)
UPDATE {output_schema}.buildings target
SET
    feature_id       = src.feature_id,
    building_use     = src.building_use,
    building_use_id  = src.building_use_id,
    street           = src.street,
    house_number     = src.house_number,
    gemeindeschluessel = src.gemeindeschluessel,
    -- Reset derived columns to NULL
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
FROM temp_lod2_data src
WHERE target.objectid = src.objectid;

-- Insert new records
INSERT INTO {output_schema}.buildings (feature_id, objectid, building_use, building_use_id, street, house_number, gemeindeschluessel)
SELECT
    src.feature_id,
    src.objectid,
    src.building_use,
    src.building_use_id,
    src.street,
    src.house_number,
    src.gemeindeschluessel
FROM temp_lod2_data src
WHERE NOT EXISTS (
    SELECT 1
    FROM {output_schema}.buildings target
    WHERE target.objectid = src.objectid
);

DROP TABLE temp_lod2_data;

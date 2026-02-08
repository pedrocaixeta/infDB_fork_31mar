ANALYZE feature;
ANALYZE property;
ANALYZE geometry_data;

CREATE INDEX IF NOT EXISTS geometry_data_geometry_properties_index
    ON citydb.geometry_data USING gin (geometry_properties);
CREATE INDEX IF NOT EXISTS idx_property_name ON property(name);
CREATE INDEX IF NOT EXISTS idx_property_val_string ON property(val_string);
CREATE INDEX IF NOT EXISTS idx_feature_objectclass ON feature(objectclass_id);

CREATE SCHEMA IF NOT EXISTS {output_schema};

-- A. Base Buildings (Filtered by Municipality)
DROP TABLE IF EXISTS building_base;
CREATE TEMP TABLE building_base AS
SELECT
    f.id AS feature_id,
    f.objectclass_id,
    f.objectid,
    MAX(CASE WHEN p.name = 'function' THEN p.val_string END) AS building_function_code,
    p_gs.val_string AS gemeindeschluessel,
    LEFT(p_gs.val_string, 2) AS ags_id
FROM feature f
    INNER JOIN property p ON f.id = p.feature_id
    INNER JOIN property p_gs ON f.id = p_gs.feature_id
WHERE f.objectclass_id = 901
    AND p.name = 'function'
    AND p.val_string >= '31001_' AND p.val_string < '31002'
    AND p_gs.name = 'Gemeindeschluessel'
    AND p_gs.val_string IN ({ags})  -- FILTER FIRST
GROUP BY f.id, f.objectclass_id, f.objectid, p_gs.val_string;


-- Drop existing partition to avoid conflict with wrong ags_id value
DROP TABLE IF EXISTS {output_schema}.{table_name}_lod2 CASCADE;
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name}_lod2
    PARTITION OF opendata.building_lod2 
    FOR VALUES IN ('{ags_id}');

-- Then just INSERT without re-joining property
INSERT INTO {output_schema}.{table_name}_lod2 (
    ags_id, feature_id, objectid, gemeindeschluessel, objectclass_id,
    height, storeysaboveground, building_function_code, zip_code, street, 
    house_number, city, country, state
)
SELECT
    bb.ags_id,
    bb.feature_id,
    bb.objectid,
    bb.gemeindeschluessel,
    bb.objectclass_id,
    MAX(CASE WHEN p.name = 'value' AND p.parent_id IN (SELECT id FROM property WHERE name = 'height') THEN p.val_double END) AS height,
    MAX(CASE WHEN p.name = 'storeysAboveGround' THEN p.val_int END) AS storeysaboveground,
    bb.building_function_code,
    a.zip_code,
    regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '') AS street,
    (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] AS house_number,
    a.city, a.country, a.state
FROM building_base bb
    LEFT JOIN property p ON bb.feature_id = p.feature_id AND p.name IN ('value', 'storeysAboveGround')
    LEFT JOIN address a ON p.val_address_id = a.id
GROUP BY bb.feature_id, bb.objectclass_id, bb.objectid, bb.building_function_code, 
         bb.gemeindeschluessel, bb.ags_id, a.zip_code, a.street, a.city, a.country, a.state;


CREATE INDEX IF NOT EXISTS idx_building_lod2_objectid ON {output_schema}.{table_name}_lod2 (objectid);
CREATE INDEX IF NOT EXISTS idx_building_lod2_gemeindeschluessel ON {output_schema}.{table_name}_lod2 (gemeindeschluessel);
CREATE INDEX IF NOT EXISTS idx_building_lod2_ags_id ON {output_schema}.{table_name}_lod2 (ags_id);
CREATE INDEX IF NOT EXISTS idx_building_lod2_height ON {output_schema}.{table_name}_lod2 (height);
CREATE INDEX IF NOT EXISTS idx_building_lod2_storeys ON {output_schema}.{table_name}_lod2 (storeysaboveground);
CREATE INDEX IF NOT EXISTS idx_building_lod2_street ON {output_schema}.{table_name}_lod2 (street);
CREATE INDEX IF NOT EXISTS idx_building_lod2_house_number ON {output_schema}.{table_name}_lod2 (house_number);
CREATE INDEX IF NOT EXISTS idx_building_lod2_city ON {output_schema}.{table_name}_lod2 (city);
CREATE INDEX IF NOT EXISTS idx_building_lod2_country ON {output_schema}.{table_name}_lod2 (country);
CREATE INDEX IF NOT EXISTS idx_building_lod2_zip_code ON {output_schema}.{table_name}_lod2 (zip_code);
CREATE INDEX IF NOT EXISTS idx_building_lod2_state ON {output_schema}.{table_name}_lod2 (state);
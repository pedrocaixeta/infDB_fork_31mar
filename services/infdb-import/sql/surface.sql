ANALYZE feature;
ANALYZE property;
ANALYZE geometry_data;

CREATE INDEX IF NOT EXISTS geometry_data_geometry_properties_index
    ON citydb.geometry_data USING gin (geometry_properties);
CREATE INDEX IF NOT EXISTS idx_property_name ON property(name);
CREATE INDEX IF NOT EXISTS idx_property_val_string ON property(val_string);
CREATE INDEX IF NOT EXISTS idx_feature_objectclass ON feature(objectclass_id);

CREATE SCHEMA IF NOT EXISTS {output_schema};


-- A. Building_lod2 Table
DROP TABLE IF EXISTS {output_schema}.{table_name}_lod2;
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name}_lod2
    PARTITION OF opendata.building_lod2 
    FOR VALUES IN ('{ags_id}');

INSERT INTO {output_schema}.{table_name}_lod2 (
    ags_id,
    feature_id,
    objectid,
    gemeindeschluessel,
    objectclass_id,
    height,
    storeysaboveground,
    building_function_code,
    zip_code,
    street,
    house_number,
    city,
    country,
    state
)
    SELECT
        LEFT(MAX(CASE WHEN p.name = 'Gemeindeschluessel' THEN p.val_string END), 2) AS ags_id,
        f.id AS feature_id,
        f.objectid,
        MAX(CASE WHEN p.name = 'Gemeindeschluessel' THEN p.val_string END) AS gemeindeschluessel,
        f.objectclass_id,
        MAX(CASE WHEN p.name = 'value' AND p.parent_id IN (SELECT id FROM property WHERE name = 'height') THEN p.val_double END) AS height,
        MAX(CASE WHEN p.name = 'storeysAboveGround' THEN p.val_int END) AS storeysaboveground,
        MAX(CASE WHEN p.name = 'function' THEN p.val_string END) AS building_function_code,
        -- table address
        a.zip_code,
        regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '') AS street,
        (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] AS house_number,
        a.city,
        a.country,
        a.state
    FROM feature f
            JOIN property p ON f.id = p.feature_id
    JOIN address a ON p.val_address_id = a.id
    WHERE f.objectclass_id = 901
    AND p.name IN ('function', 'Gemeindeschluessel', 'height', 'storeysAboveGround')
    AND (p.name = 'function' AND p.val_string >= '31001_' AND p.val_string < '31002')
    AND (p.name = 'Gemeindeschluessel' AND p.val_string IN ({ags}))
    GROUP BY f.id, f.objectclass_id, f.objectid, a.street, a.city, a.country, a.zip_code, a.state;

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


-- Create indexes on the temporary table for faster joins
DROP TABLE IF EXISTS {output_schema}.{table_name}_ids;
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name}_ids AS (
    SELECT
        f.objectid,
        child ->> 'objectId' AS child_object_id,
        f.objectclass_id,
        gd.feature_id,
        gd.id as geometry_data_id
    FROM feature f
    JOIN geometry_data gd ON f.id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
    WHERE f.objectclass_id IN (709, 710, 712, 901)
      AND (child ->> 'objectId') IS NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_surface_ids_objectid ON {output_schema}.{table_name}_ids (objectid);
CREATE INDEX IF NOT EXISTS idx_surface_ids_child_object_id ON {output_schema}.{table_name}_ids (child_object_id);
CREATE INDEX IF NOT EXISTS idx_surface_ids_objectclass_id ON {output_schema}.{table_name}_ids (objectclass_id);


-- Create building surfaces table
DROP TABLE IF EXISTS {output_schema}.{table_name}_surface;
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name}_surface AS
SELECT
    sid2.objectid,
    sid.objectclass_id,
    oc.classname,
    gd.geometry
--     debugging columns:
--     sid.child_object_id,
--     sid.feature_id,
--     sid.geometry_data_id,
FROM {output_schema}.{table_name}_ids sid
JOIN {output_schema}.{table_name}_ids sid2 ON sid.child_object_id = sid2.child_object_id AND sid2.objectclass_id = 901
JOIN geometry_data gd ON gd.id = sid.geometry_data_id
JOIN objectclass oc ON oc.id = sid.objectclass_id
WHERE sid.objectclass_id IN (709, 710, 712);    -- 709: RoofSurface, 710: WallSurface, 712: GroundSurface

CREATE INDEX IF NOT EXISTS idx_surface_objectid ON {output_schema}.{table_name}_surface (objectid);
CREATE INDEX IF NOT EXISTS idx_surface_objectclass_id ON {output_schema}.{table_name}_surface (objectclass_id);
CREATE INDEX IF NOT EXISTS idx_surface_classname ON {output_schema}.{table_name}_surface (classname);
CREATE INDEX IF NOT EXISTS idx_surface_geometry ON {output_schema}.{table_name}_surface USING GIST(geometry);
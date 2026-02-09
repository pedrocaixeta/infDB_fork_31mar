CREATE INDEX IF NOT EXISTS geometry_data_geometry_properties_index
    ON citydb.geometry_data USING gin (geometry_properties);
CREATE INDEX IF NOT EXISTS idx_property_name ON property(name);
CREATE INDEX IF NOT EXISTS idx_property_val_string ON property(val_string);
CREATE INDEX IF NOT EXISTS idx_feature_objectclass ON feature(objectclass_id);

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
    gd.geometry as geom
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
CREATE INDEX IF NOT EXISTS idx_surface_geometry ON {output_schema}.{table_name}_surface USING GIST(geom);

-- Create a view for easier access to building geometries (e.g., for export)
CREATE OR REPLACE VIEW {output_schema}.{table_name}_geometry AS
SELECT
    s.objectid,
    s.geom
FROM {output_schema}.{table_name}_surface s
WHERE s.objectclass_id = 712
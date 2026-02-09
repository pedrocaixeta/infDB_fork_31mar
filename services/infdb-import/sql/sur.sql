ANALYZE feature;
ANALYZE geometry_data;

DROP TABLE IF EXISTS {output_schema}.{table_name}_ids;
CREATE TABLE {output_schema}.{table_name}_ids AS
SELECT
    f.objectid,
    child ->> 'objectId' AS child_object_id,
    gd.id AS geometry_data_id,
    gd.objectclass_id,
    gd.geometry
FROM feature f
    JOIN geometry_data gd ON f.id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
WHERE f.objectclass_id IN (709, 710, 712, 901)
    AND (child ->> 'objectId') IS NOT NULL;

-- Only 2 critical indexes
CREATE INDEX idx_surface_ids_child_object_id ON {output_schema}.{table_name}_ids (child_object_id);
CREATE INDEX idx_surface_ids_objectid ON {output_schema}.{table_name}_ids (objectid);

CREATE TABLE {output_schema}.{table_name}_surface AS
SELECT
    sid2.objectid,
    sid.objectclass_id,
    oc.classname,
    sid.geometry AS geom
FROM {output_schema}.{table_name}_ids sid
    JOIN {output_schema}.{table_name}_ids sid2 
        ON sid.child_object_id = sid2.child_object_id 
        AND sid2.objectclass_id = 901
    JOIN objectclass oc ON oc.id = sid.objectclass_id
WHERE sid.objectclass_id IN (709, 710, 712);
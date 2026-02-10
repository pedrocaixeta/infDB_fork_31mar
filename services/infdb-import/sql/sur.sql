-- ANALYZE feature;
-- ANALYZE geometry_data;

CREATE SCHEMA IF NOT EXISTS tmp_bld;
DROP TABLE IF EXISTS tmp_bld.{table_name}_ids;
CREATE TABLE tmp_bld.{table_name}_ids AS
SELECT
    f.objectid,
    child ->> 'objectId' AS child_object_id,
    gd.id AS geometry_data_id,
    f.objectclass_id
    -- gd.geometry
FROM feature f
    JOIN geometry_data gd ON f.id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
WHERE f.objectclass_id IN (709, 710, 712, 901)
    -- AND f.objectid LIKE '{object_id_prefix}%'
    AND (child ->> 'objectId') IS NOT NULL;

-- Only 2 critical indexes
CREATE INDEX IF NOT EXISTS idx_surface_ids_objectid ON tmp_bld.{table_name}_ids (objectid);
CREATE INDEX IF NOT EXISTS idx_surface_ids_child_object_id ON tmp_bld.{table_name}_ids (child_object_id);
CREATE INDEX IF NOT EXISTS idx_surface_ids_geometry_data_id ON tmp_bld.{table_name}_ids (geometry_data_id);
CREATE INDEX IF NOT EXISTS idx_surface_ids_objectclass_id ON tmp_bld.{table_name}_ids (objectclass_id);


DROP TABLE IF EXISTS {output_schema}.{table_name} CASCADE;
CREATE TABLE {output_schema}.{table_name} AS
SELECT
    sid2.objectid,
    sid.objectclass_id,
    oc.classname,
    gd.geometry AS geom
FROM tmp_bld.{table_name}_ids sid
    JOIN tmp_bld.{table_name}_ids sid2 
        ON sid.child_object_id = sid2.child_object_id 
        AND sid2.objectclass_id = 901
    JOIN objectclass oc ON oc.id = sid.objectclass_id
    JOIN geometry_data gd ON gd.id = sid.geometry_data_id
WHERE sid.objectclass_id IN (709, 710, 712);
CREATE INDEX IF NOT EXISTS {table_name}_objectid_idx ON {output_schema}.{table_name} (objectid);
CREATE INDEX IF NOT EXISTS {table_name}_objectclass_id_idx ON {output_schema}.{table_name} (objectclass_id);
CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {output_schema}.{table_name} USING GIST (geom);



DROP TABLE IF EXISTS {output_schema}.{bld_table_name}_view;
CREATE MATERIALIZED VIEW {output_schema}.{bld_table_name}_view AS
SELECT 
    bld.*,
    sur.geom,
    ST_Centroid(sur.geom) AS centroid
FROM {output_schema}.building_lod2 bld
JOIN {output_schema}.{table_name} sur ON bld.objectid = sur.objectid
WHERE sur.objectclass_id = 710; -- 710 = ground surface
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_objectid_idx ON {output_schema}.{bld_table_name}_view (objectid);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_geom_idx ON {output_schema}.{bld_table_name}_view USING GIST (geom);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_centroid_idx ON {output_schema}.{bld_table_name}_view USING GIST (centroid);
-- ANALYZE feature;
-- ANALYZE geometry_data;

CREATE SCHEMA IF NOT EXISTS tmp_bld;
DROP TABLE IF EXISTS tmp_bld.{table_name}_ids;
CREATE TABLE tmp_bld.{table_name}_ids AS
SELECT
    f.objectid as building_objectid,
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
CREATE INDEX IF NOT EXISTS idx_surface_ids_building_objectid ON tmp_bld.{table_name}_ids (building_objectid);
CREATE INDEX IF NOT EXISTS idx_surface_ids_child_object_id ON tmp_bld.{table_name}_ids (child_object_id);
CREATE INDEX IF NOT EXISTS idx_surface_ids_geometry_data_id ON tmp_bld.{table_name}_ids (geometry_data_id);
CREATE INDEX IF NOT EXISTS idx_surface_ids_objectclass_id ON tmp_bld.{table_name}_ids (objectclass_id);


DROP TABLE IF EXISTS {output_schema}.{table_name} CASCADE;
CREATE TABLE {output_schema}.{table_name} AS
SELECT
    sid2.building_objectid,
    sid.objectclass_id,
    oc.classname,
    ST_Area(gd.geometry) AS area,
    gd.geometry AS geom
FROM tmp_bld.{table_name}_ids sid
    JOIN tmp_bld.{table_name}_ids sid2 
        ON sid.child_object_id = sid2.child_object_id 
        AND sid2.objectclass_id = 901
    JOIN objectclass oc ON oc.id = sid.objectclass_id
    JOIN geometry_data gd ON gd.id = sid.geometry_data_id
WHERE sid.objectclass_id IN (709, 710, 712);
CREATE INDEX IF NOT EXISTS {table_name}_building_objectid_idx ON {output_schema}.{table_name} (building_objectid);
CREATE INDEX IF NOT EXISTS {table_name}_objectclass_id_idx ON {output_schema}.{table_name} (objectclass_id);
CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {output_schema}.{table_name} USING GIST (geom);



DROP TABLE IF EXISTS {output_schema}.{bld_table_name}_view;
CREATE MATERIALIZED VIEW {output_schema}.{bld_table_name}_view AS
SELECT 
    bld.*,
    ST_area(sur.geom) AS groundsurface_flaeche,
    ST_Multi(sur.geom) AS geom,
    ST_Centroid(sur.geom) AS centroid
FROM {output_schema}.building_lod2 bld
JOIN {output_schema}.{table_name} sur ON bld.objectid = sur.building_objectid
WHERE sur.objectclass_id = 710; -- 710 = ground surface

CREATE INDEX IF NOT EXISTS {bld_table_name}_view_objectid_idx ON {output_schema}.{bld_table_name}_view (objectid);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_id_idx ON {output_schema}.{bld_table_name}_view (id);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_ags_id_idx ON {output_schema}.{bld_table_name}_view (ags_id);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_feature_id_idx ON {output_schema}.{bld_table_name}_view (feature_id);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_gemeindeschluessel_idx ON {output_schema}.{bld_table_name}_view (gemeindeschluessel);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_objectclass_id_idx ON {output_schema}.{bld_table_name}_view (objectclass_id);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_height_idx ON {output_schema}.{bld_table_name}_view (height);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_storeysaboveground_idx ON {output_schema}.{bld_table_name}_view (storeysaboveground);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_building_function_code_idx ON {output_schema}.{bld_table_name}_view (building_function_code);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_zip_code_idx ON {output_schema}.{bld_table_name}_view (zip_code);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_street_idx ON {output_schema}.{bld_table_name}_view (street);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_house_number_idx ON {output_schema}.{bld_table_name}_view (house_number);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_city_idx ON {output_schema}.{bld_table_name}_view (city);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_country_idx ON {output_schema}.{bld_table_name}_view (country);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_state_idx ON {output_schema}.{bld_table_name}_view (state);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_groundsurface_flaeche_idx ON {output_schema}.{bld_table_name}_view (groundsurface_flaeche);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_geom_idx ON {output_schema}.{bld_table_name}_view USING GIST (geom);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_centroid_idx ON {output_schema}.{bld_table_name}_view USING GIST (centroid);

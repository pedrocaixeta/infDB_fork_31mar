-- OPTIMIZED SINGLE-PASS BUILDINGS LOADER
-- Combines all data extraction in CTEs and executes a single INSERT
--------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {output_schema};

DROP TABLE IF EXISTS {output_schema}.{table_name};
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} 
    PARTITION OF opendata.building_lod2 
    FOR VALUES IN ('{ags_id}');

-- Single-pass insert with all data joined via CTEs
WITH 
-- A. Base Buildings (Filtered by Municipality)
base_buildings AS (
    SELECT 
        f.id AS feature_id,
        f.objectclass_id,
        f.objectid,
        p_func.val_string AS building_function_code,
        p_gs.val_string AS gemeindeschluessel,
        substring(p_gs.val_string, 1, 2) AS ags_id
    FROM feature f
    JOIN property p_func ON f.id = p_func.feature_id
    JOIN property p_gs ON f.id = p_gs.feature_id
    WHERE f.objectclass_id = 901
      AND p_func.name = 'function'
      AND p_func.val_string LIKE '31001_%'
      AND p_gs.name = 'Gemeindeschluessel'
      AND p_gs.val_string IN ({gemeindeschluessel})
),

-- B. Height Data (Deduped)
height_data AS (
    SELECT DISTINCT ON (p.feature_id) 
        p.feature_id, 
        p.val_double
    FROM property p
    WHERE p.name = 'value'
      AND p.parent_id IN (SELECT id FROM property WHERE name = 'height')
    ORDER BY p.feature_id, p.id DESC
),

-- C. Floor Data (Deduped)
floor_data AS (
    SELECT DISTINCT ON (feature_id)
        feature_id, 
        val_int
    FROM property
    WHERE name = 'storeysAboveGround'
    ORDER BY feature_id, id DESC
),

-- D. Address Data (Deduped & Parsed)
address_data AS (
    SELECT DISTINCT ON (p.feature_id)
        p.feature_id,
        regexp_replace(trim(a.street), '\s*\d+[\w,]*$', '') AS street,
        (regexp_match(trim(a.street), '\s*(\d+[\w,]*)$'))[1] AS house_number,
        a.city,
        a.country,
        a.zip_code,
        a.state
    FROM property p
    JOIN address a ON p.val_address_id = a.id
    WHERE p.feature_id IN (SELECT feature_id FROM base_buildings)
    ORDER BY p.feature_id, a.id DESC
),

-- E. Geometry Data (Extract ground surfaces via JSONB navigation - optimized)
geometry_data AS (
    WITH building_child_ids AS (
        -- First collect all child_object_ids from our buildings
        SELECT
            bb.feature_id,
            child ->> 'objectId' AS child_object_id
        FROM base_buildings bb
        JOIN geometry_data gd ON bb.feature_id = gd.feature_id
        CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
    ),
    ground_surface_geoms AS (
        -- Then only look up geometries for those specific child_object_ids
        SELECT
            child ->> 'objectId' AS child_object_id,
            gd.geometry
        FROM feature f
        JOIN geometry_data gd ON f.id = gd.feature_id
        CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
        WHERE f.objectclass_id = 710
          AND (child ->> 'objectId') IN (SELECT child_object_id FROM building_child_ids)
    )
    SELECT DISTINCT ON (bc.feature_id)
        bc.feature_id,
        gs.geometry AS geom,
        st_area(gs.geometry) AS area,
        ST_Centroid(gs.geometry) AS centroid
    FROM building_child_ids bc
    JOIN ground_surface_geoms gs ON bc.child_object_id = gs.child_object_id
    ORDER BY bc.feature_id
)

-- Execute single INSERT with all data
INSERT INTO {output_schema}.{table_name} (
    feature_id, 
    objectclass_id, 
    objectid, 
    gemeindeschluessel, 
    ags_id, 
    building_function_code, 
    height, 
    storeysAboveGround,
    groundsurface_flaeche, 
    geom, 
    centroid,
    street, 
    house_number, 
    city, 
    country, 
    zip_code, 
    state
)
SELECT 
    bb.feature_id,
    bb.objectclass_id,
    bb.objectid,
    bb.gemeindeschluessel,
    bb.ags_id,
    bb.building_function_code,
    hd.val_double,
    GREATEST(fd.val_int, 1),
    gd.area,
    gd.geom,
    gd.centroid,
    ad.street,
    ad.house_number,
    ad.city,
    ad.country,
    ad.zip_code,
    ad.state
FROM base_buildings bb
LEFT JOIN height_data hd ON bb.feature_id = hd.feature_id
LEFT JOIN floor_data fd ON bb.feature_id = fd.feature_id
LEFT JOIN geometry_data gd ON bb.feature_id = gd.feature_id
LEFT JOIN address_data ad ON bb.feature_id = ad.feature_id;
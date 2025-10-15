-- fill geom and floor_area columns
WITH ground_data AS (
    SELECT objectid as building_objectid,
        feature_id,
        groundsurface_flaeche          as area,
        ST_Transform(ST_Force2D(b.geom), 3035)     as geom
    FROM {input_schema}.buildings_lod2 b
    --       JOIN geometry_data gd ON f.id = gd.feature_id
    --       JOIN property p ON gd.feature_id = p.feature_id
    -- WHERE f.objectclass_id = 710 -- GroundSurface
    -- AND p.name = 'Flaeche'
)
UPDATE {output_schema}.buildings_pylovo b
SET floor_area = gd.area,
    geom       = gd.geom,
    centroid   = ST_Centroid(gd.geom)
FROM ground_data gd
WHERE b.feature_id = gd.feature_id;

-- delete buildings below an area threshold
DELETE
FROM {output_schema}.buildings_pylovo
WHERE buildings_pylovo.floor_area < 12;
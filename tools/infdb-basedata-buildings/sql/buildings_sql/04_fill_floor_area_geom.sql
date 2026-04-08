-- Summary: Updates the geom, centroid, and floor_area columns and
-- removes small buildings with a floor area of less than 12 square meters.
-- If groundsurface_flaeche is NULL, the floor area is calculated from
-- the building_surface table (objectclass_id = 710) using safe_area_fallback.

-- fill geom and floor_area columns
WITH ground_data AS (
    SELECT
        objectid AS building_objectid,
        feature_id,
        groundsurface_flaeche AS area,
        b.geom
    FROM {input_schema}.building_view b
    WHERE b.gemeindeschluessel = '{ags}'
),
ground_data_filled AS (
    SELECT
        gd.building_objectid,
        gd.feature_id,
        COALESCE(gd.area, {output_schema}.safe_area_fallback(bs.geom)) as area,
        gd.geom
    FROM ground_data gd
    LEFT JOIN {input_schema}.building_surface bs
        ON  bs.building_objectid = gd.building_objectid
        AND bs.objectclass_id    = 710
        AND gd.area IS NULL
)
UPDATE temp_buildings b
SET floor_area = gd.area,
    geom       = ST_Transform(ST_Force2D(gd.geom), {EPSG}),
    centroid   = ST_Centroid(ST_Transform(ST_Force2D(gd.geom), {EPSG}))
FROM ground_data_filled gd
WHERE b.feature_id = gd.feature_id;

-- delete buildings below an area threshold
DELETE
FROM temp_buildings b
WHERE b.floor_area < 12;

-- Summary: Updates the geom, centroid, and floor_area columns and
-- removes small buildings with a floor area of less than 12 square meters.

-- fill geom and floor_area columns
WITH ground_data AS (
    SELECT objectid as building_objectid,
        feature_id,
        groundsurface_flaeche          as area,
        ST_Transform(ST_Force2D(b.geom), {EPSG})     as geom
    FROM {input_schema}.buildings_lod2 b
    WHERE b.gemeindeschluessel = '{ags}'

)
UPDATE {output_schema}.buildings b
SET floor_area = gd.area,
    geom       = gd.geom,
    centroid   = ST_Centroid(gd.geom)
FROM ground_data gd
WHERE b.gemeindeschluessel = '{ags}'
  AND b.feature_id = gd.feature_id;

-- delete buildings below an area threshold
DELETE
FROM {output_schema}.buildings b
WHERE b.gemeindeschluessel = '{ags}'
  AND b.floor_area < 12;

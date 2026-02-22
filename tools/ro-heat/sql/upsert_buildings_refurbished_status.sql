CREATE TABLE IF NOT EXISTS {output_schema}.buildings_refurbished_status
(
    building_objectid TEXT PRIMARY KEY,
    floor_area DOUBLE PRECISION,
    floor_number BIGINT,
    building_type TEXT,
    construction_year BIGINT,
    wall_area DOUBLE PRECISION,
    roof_area DOUBLE PRECISION,
    window_area DOUBLE PRECISION,
    outer_wall BIGINT,
    rooftop BIGINT,
    "window" BIGINT
);

INSERT INTO {output_schema}.buildings_refurbished_status (
    building_objectid,
    floor_area,
    floor_number,
    building_type,
    construction_year,
    wall_area,
    roof_area,
    window_area,
    outer_wall,
    rooftop,
    "window"
)
SELECT building_objectid,
    floor_area,
    floor_number,
    building_type,
    construction_year,
    wall_area,
    roof_area,
    window_area,
    outer_wall,
    rooftop,
    "window"
FROM {output_schema}.temp_buildings_refurbished_status_{ags}
ON CONFLICT (building_objectid)
DO UPDATE SET floor_area = EXCLUDED.floor_area,
floor_number = EXCLUDED.floor_number,
building_type = EXCLUDED.building_type,
construction_year = EXCLUDED.construction_year,
wall_area = EXCLUDED.wall_area,
roof_area = EXCLUDED.roof_area,
window_area = EXCLUDED.window_area,
outer_wall = EXCLUDED.outer_wall,
rooftop = EXCLUDED.rooftop,
"window" = EXCLUDED."window";

DROP table {output_schema}.temp_buildings_refurbished_status_{ags};
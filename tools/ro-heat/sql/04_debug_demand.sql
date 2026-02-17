-- CREATE TABLE IF NOT EXISTS {output_schema}.tmp_debug_demand (
--     -- refurbished status
--     building_objectid text PRIMARY KEY,
--     -- annual heating demand
--     "heating:demand[Wh]" double precision,
--     "heating:demand_per_area[kWh/m²]" double precision,
--     -- rc values
--     resistance double precision,
--     capacitance double precision,
--     floor_area double precision,
--     floor_number integer,
--     building_type text,
--     construction_year integer,
--     wall_area double precision,
--     roof_area double precision,
--     window_area double precision,
--     outer_wall text,
--     rooftop text,
--     "window" text,
--     -- basedata
--     objectclass_id text,
--     height double precision,
--     storesaboveground integer,
--     building_function_code text,
--     gemeindeschluessel text,
--     geom geometry
--     -- opendata
-- );

-- INSERT INTO {output_schema}.tmp_debug_demand (
--     building_objectid,
--     "heating:demand[Wh]",
--     "heating:demand_per_area[kWh/m²]",
--     resistance,
--     capacitance,
--     floor_area,
--     floor_number,
--     building_type,
--     construction_year,
--     wall_area,
--     roof_area,
--     window_area,
--     outer_wall,
--     rooftop,
--     "window",
--     objectclass_id,
--     height,
--     storesaboveground,
--     building_function_code,
--     gemeindeschluessel,
--     geom
-- )
CREATE OR REPLACE VIEW {output_schema}.debug_demand AS
SELECT
    ahd."heating:demand[Wh]",
    ((ahd."heating:demand[Wh]") / (brs.floor_area * brs.floor_number))/1000 AS "heating:demand_per_area[kWh/m²]",
    brc.resistance,
    brc.capacitance,
    brs.*,
    bbl.id,
    bbl.feature_id,
    bbl.height,
    -- bbl.floor_area,
    -- bbl.floor_number,
    bbl.building_use,
    bbl.building_use_id,
    -- bbl.building_type,
    bbl.occupants,
    bbl.households,
    -- bbl.construction_year,
    bbl.postcode,
    bbl.address_street_id,
    bbl.street,
    bbl.house_number,
    bbl.gemeindeschluessel,
    bbl.centroid,
    bbl.geom
FROM {output_schema}.buildings_refurbished_status brs
JOIN {output_schema}.annual_heating_demand ahd ON brs.building_objectid = ahd.building_objectid
JOIN {output_schema}.buildings_rc brc ON brs.building_objectid = brc.building_objectid
JOIN basedata.buildings bbl ON brs.building_objectid = bbl.objectid;
-- WHERE bbl.gemeindeschluessel LIKE '{ags}'
-- ON CONFLICT (building_objectid) DO UPDATE
-- SET
--     "heating:demand[Wh]" = EXCLUDED."heating:demand[Wh]",
--     "heating:demand_per_area[kWh/m²]" = EXCLUDED."heating:demand_per_area[kWh/m²]",
--     resistance = EXCLUDED.resistance,
--     capacitance = EXCLUDED.capacitance,
--     building_objectid = EXCLUDED.building_objectid,
--     floor_area = EXCLUDED.floor_area,
--     floor_number = EXCLUDED.floor_number,
--     building_type = EXCLUDED.building_type,
--     construction_year = EXCLUDED.construction_year,
--     wall_area = EXCLUDED.wall_area,
--     roof_area = EXCLUDED.roof_area,
--     window_area = EXCLUDED.window_area,
--     outer_wall = EXCLUDED.outer_wall,
--     rooftop = EXCLUDED.rooftop,
--     "window" = EXCLUDED."window",
--     objectclass_id = EXCLUDED.objectclass_id,
--     height = EXCLUDED.height,
--     storesaboveground = EXCLUDED.storesaboveground,
--     building_function_code = EXCLUDED.building_function_code,
--     gemeindeschluessel = EXCLUDED.gemeindeschluessel,
--     geom = EXCLUDED.geom;
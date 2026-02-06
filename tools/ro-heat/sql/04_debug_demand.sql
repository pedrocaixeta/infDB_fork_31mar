CREATE TABLE IF NOT EXISTS {output_schema}.tmp_debug_demand (
    objectid text PRIMARY KEY,
    gemeindeschluessel text,
    "heating:demand[Wh]" double precision,
    resistance double precision,
    capacitance double precision,
    -- columns from brs.*
    -- add explicit columns here to match brs.*
    "heating:demand_per_area[kWh/m²]" double precision,
    geom geometry
);

INSERT INTO {output_schema}.tmp_debug_demand (
    objectid,
    gemeindeschluessel,
    "heating:demand[Wh]",
    resistance,
    capacitance,
    -- brs.* columns
    "heating:demand_per_area[kWh/m²]",
    geom
)
SELECT
    bl2.objectid,
    bl2.gemeindeschluessel,
    ahd."heating:demand[Wh]",
    brc.resistance,
    brc.capacitance,
    -- brs.* columns
    ((ahd."heating:demand[Wh]") / (bl2.floor_area * bl2.floor_number))/1000 AS "heating:demand_per_area[kWh/m²]",
    bl2.geom
FROM basedata.buildings bl2
JOIN {output_schema}.annual_heating_demand ahd
    ON bl2.objectid = ahd.building_objectid
JOIN {output_schema}.buildings_rc brc
    ON ahd.building_objectid = brc.building_objectid
JOIN {output_schema}.buildings_refurbished_status brs
    ON ahd.building_objectid = brs.building_objectid
WHERE bl2.gemeindeschluessel LIKE '{ags}'
ON CONFLICT (objectid) DO UPDATE
SET
    gemeindeschluessel = EXCLUDED.gemeindeschluessel,
    "heating:demand[Wh]" = EXCLUDED."heating:demand[Wh]",
    resistance = EXCLUDED.resistance,
    capacitance = EXCLUDED.capacitance,
    -- brs.* columns
    "heating:demand_per_area[kWh/m²]" = EXCLUDED."heating:demand_per_area[kWh/m²]",
    geom = EXCLUDED.geom;
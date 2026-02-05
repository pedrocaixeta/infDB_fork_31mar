CREATE TABLE IF NOT EXISTS ro_heat.tmp_debug_demand (
    objectid bigint PRIMARY KEY,
    gemeindeschluessel text,
    "heating:demand[Wh]" numeric,
    resistance numeric,
    capacitance numeric,
    -- columns from brs.*
    -- add explicit columns here to match brs.*
    "heating:demand_per_area[kWh/m²]" numeric,
    geom geometry
);

INSERT INTO ro_heat.tmp_debug_demand (
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
JOIN ro_heat.annual_heating_demand ahd
    ON bl2.objectid = ahd.building_objectid
JOIN ro_heat.buildings_rc brc
    ON ahd.building_objectid = brc.building_objectid
JOIN ro_heat.buildings_refurbished_status brs
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
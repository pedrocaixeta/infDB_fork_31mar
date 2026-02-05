DROP TABLE IF EXISTS ro_heat.tmp_debug_demand;
CREATE TABLE ro_heat.tmp_debug_demand AS
    SELECT objectid,
            bl2.gemeindeschluessel,
            ahd."heating:demand[Wh]",
            brc.resistance,
            brc.capacitance,
            brs.*,
            -- bl2.*,
            (( ahd."heating:demand[Wh]" ) / (bl2.floor_area * bl2.floor_number))/1000 AS "heating:demand_per_area[kWh/m²]",
            bl2.geom
    FROM basedata.buildings bl2
    JOIN ro_heat.annual_heating_demand ahd
        ON bl2.objectid = ahd.building_objectid
    JOIN ro_heat.buildings_rc brc
        ON ahd.building_objectid = brc.building_objectid
    JOIN ro_heat.buildings_refurbished_status brs
        ON ahd.building_objectid = brs.building_objectid
    WHERE bl2.gemeindeschluessel LIKE '{ags}';
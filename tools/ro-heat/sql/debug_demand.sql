DROP TABLE IF EXISTS ro_heat.tmp_debug_demand;
CREATE TABLE ro_heat.tmp_debug_demand AS
    SELECT objectid,
            gemeindeschluessel,
            "heating:demand[Wh]",
            brc.resistance,
            floor_area,
            floor_number,
            (( "heating:demand[Wh]" ) / (floor_area * floor_number)) AS "heating:demand_per_area[Wh/m²]",
            geom
    FROM basedata.buildings bl2
    JOIN ro_heat.annual_heating_demand
        ON bl2.objectid = annual_heating_demand.building_objectid
    JOIN ro_heat.buildings_rc brc
        ON annual_heating_demand.building_objectid = brc.building_objectid
    WHERE gemeindeschluessel LIKE '{ags}';
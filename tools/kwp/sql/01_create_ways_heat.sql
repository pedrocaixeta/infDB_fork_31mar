-- DROP TABLE IF EXISTS {output_schema}.ways_heat_demand;
CREATE TABLE {output_schema}.ways_heat_demand AS
    SELECT ways.way_id, ways.verkehrslinie_id_basemap as objectid, ways.name, ways.name_kurz, ways.clazz, ways.postcode, ways.geom, sum(entise_summary."heating:demand[Wh]") as total_heating_demand, st_length(ways.geom) as length, sum(entise_summary."heating:demand[Wh]") / st_length(ways.geom) as demand_per_length
    FROM {input_schema_basedata}.buildings_to_ways
    JOIN {input_schema_basedata}.buildings ON buildings_to_ways.building_id = buildings.id
    JOIN {input_schema_ro-heat}.entise_summary ON buildings.objectid = entise_summary.building_objectid
    JOIN {input_schema_basedata}.ways ON way_way_id = ways.way_id
    GROUP BY ways.way_id, ways.verkehrslinie_id_basemap, ways.name, ways.name_kurz, ways.clazz, ways.postcode, ways.geom;
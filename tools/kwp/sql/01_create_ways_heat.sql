-- DROP TABLE IF EXISTS {output_schema}.ways_heat_demand;
CREATE TABLE {output_schema}.ways_heat_demand AS
    SELECT ways.way_id, ways.verkehrslinie_id_basemap as objectid, ways.name, ways.name_kurz, ways.clazz, ways.postcode, ways.geom, sum(entise_output.demand_heating) as total_heating_demand, st_length(ways.geom) as length, sum(entise_output.demand_heating) / st_length(ways.geom) as demand_per_length
    FROM {input_schema_basedata}.connections_buildings_to_ways
    JOIN {input_schema_basedata}.buildings_pylovo ON connections_buildings_to_ways.building_id = buildings_pylovo.id
    JOIN {input_schema_ro-heat}.entise_output ON buildings_pylovo.objectid = entise_output.building_objectid
    JOIN {input_schema_basedata}.ways ON way_way_id = ways.way_id
    GROUP BY ways.way_id, ways.verkehrslinie_id_basemap, ways.name, ways.name_kurz, ways.clazz, ways.postcode, ways.geom;
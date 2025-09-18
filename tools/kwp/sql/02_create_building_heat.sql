-- DROP TABLE IF EXISTS {output_schema}.buildings_heat_demand;
CREATE TABLE {output_schema}.buildings_heat_demand AS
    SELECT buildings_pylovo.*, entise_output.demand_heating, connections_buildings_to_ways.connection_geom
    FROM {input_schema_basedata}.buildings_pylovo
    JOIN {input_schema_ro-heat}.entise_output ON entise_output.building_objectid = buildings_pylovo.objectid
    JOIN {input_schema_ro-heat}.buildings_rc ON buildings_rc.building_objectid = buildings_pylovo.objectid
    JOIN {input_schema_basedata}.connections_buildings_to_ways ON buildings_pylovo.id = connections_buildings_to_ways.building_id

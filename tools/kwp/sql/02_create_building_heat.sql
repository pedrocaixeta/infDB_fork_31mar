-- DROP TABLE IF EXISTS {output_schema}.buildings_heat_demand;
CREATE TABLE {output_schema}.buildings_heat_demand AS
    SELECT buildings.*, entise_output.demand_heating, buildings_to_ways.connection_geom, buildings_to_ways.endpoint_geom,
    buildings_rc.construction_year as rc_construction_year, buildings_rc.wall_area as rc_wall_area, 
    buildings_rc.roof_area as rc_roof_area, buildings_rc.window_area as rc_window_area,
    buildings_rc.outer_wall as rc_outer_wall, buildings_rc.rooftop as rc_rooftop,
    buildings_rc.window as rc_window 
    FROM {input_schema_basedata}.buildings
    JOIN {input_schema_ro-heat}.entise_output ON entise_output.building_objectid = buildings.objectid
    JOIN {input_schema_ro-heat}.buildings_rc ON buildings_rc.building_objectid = buildings.objectid
    JOIN {input_schema_basedata}.buildings_to_ways ON buildings.id = buildings_to_ways.building_id

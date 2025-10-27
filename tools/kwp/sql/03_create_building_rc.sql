-- DROP TABLE IF EXISTS {output_schema}.buildings_rc;
CREATE TABLE {output_schema}.buildings_rc AS
SELECT buildings_rc.*, buildings.geom
FROM {input_schema_basedata}.buildings
    JOIN {input_schema_ro-heat}.buildings_rc ON buildings_rc.building_objectid = buildings.objectid;

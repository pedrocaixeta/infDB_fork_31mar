-- DROP TABLE IF EXISTS {output_schema}.buildings_rc;
CREATE TABLE {output_schema}.buildings_rc AS
SELECT buildings_rc.*, buildings_pylovo.geom
FROM {input_schema_basedata}.buildings_pylovo
    JOIN {input_schema_ro-heat}.buildings_rc ON buildings_rc.building_objectid = buildings_pylovo.objectid;

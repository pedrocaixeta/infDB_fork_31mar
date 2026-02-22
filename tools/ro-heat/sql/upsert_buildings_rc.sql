CREATE TABLE IF NOT EXISTS {output_schema}.buildings_rc
(
    building_objectid TEXT PRIMARY KEY,
    resistance DOUBLE PRECISION,
    capacitance DOUBLE PRECISION
);

INSERT INTO {output_schema}.buildings_rc (building_objectid,
                                          resistance,
                                          capacitance)
SELECT building_objectid,
       resistance,
       capacitance
FROM {output_schema}.temp_buildings_rc_{ags}
ON CONFLICT (building_objectid)
DO UPDATE SET resistance = EXCLUDED.resistance,
capacitance = EXCLUDED.capacitance;

DROP table {output_schema}.temp_buildings_rc_{ags};
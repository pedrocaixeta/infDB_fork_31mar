-- Assign buildings to the closest street
-- Parameters:
-- {streets_schema}, {streets_table}, {streets_id_expr}, {streets_geom}
-- {buildings_schema}, {buildings_table}, {buildings_id_expr}, {buildings_geom}
-- {output_schema}, {output_table}

DROP TABLE IF EXISTS {output_schema}.{output_table};

CREATE TABLE {output_schema}.{output_table} AS
SELECT
    {buildings_id_expr} AS building_id,
    {streets_id_expr} AS street_id,
    ST_Distance(b.{buildings_geom}, s.{streets_geom}) AS nearest_distance,
    ST_ShortestLine(b.{buildings_geom}, s.{streets_geom}) AS geom
FROM
    {buildings_schema}.{buildings_table} b
CROSS JOIN LATERAL (
    SELECT
        *
    FROM
        {streets_schema}.{streets_table} s
    ORDER BY
        b.{buildings_geom} <-> s.{streets_geom}
    LIMIT 1
) s;

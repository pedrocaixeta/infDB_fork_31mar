-- Calculate linear heat density for street segments

DROP TABLE IF EXISTS {output_schema}.{output_table};

CREATE TABLE {output_schema}.{output_table} AS
WITH street_heat_demand AS (
    SELECT
        bts.street_id,
        SUM(hd.{heat_demand_column}) AS total_heat_demand
    FROM
        {buildings_to_streets_schema}.{buildings_to_streets_table} AS bts
    JOIN
        {heat_demand_schema}.{heat_demand_table} AS hd
    ON
        bts.building_id::text = hd.{heat_demand_id}::text
    GROUP BY
        bts.street_id
)
SELECT
    s.{streets_id} AS street_id,
    s.{streets_geom} AS geometry,
    COALESCE(shd.total_heat_demand, 0) AS total_heat_demand,
    ST_Length(s.{streets_geom}) AS street_length,
    CASE
        WHEN ST_Length(s.{streets_geom}) > 0 THEN COALESCE(shd.total_heat_demand, 0) / ST_Length(s.{streets_geom})
        ELSE 0
    END AS linear_heat_density
FROM
    {streets_schema}.{streets_table} AS s
LEFT JOIN
    street_heat_demand AS shd
ON
    s.{streets_id} = shd.street_id;
-- Create spatial index
CREATE INDEX idx_{output_table}_geom ON {output_schema}.{output_table} USING GIST (geometry);

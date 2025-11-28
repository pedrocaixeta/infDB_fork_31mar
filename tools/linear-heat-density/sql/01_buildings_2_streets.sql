-- Assign buildings with heat demand to their nearest streets

DROP TABLE IF EXISTS {output_schema}.{output_table};

CREATE TABLE {output_schema}.{output_table} AS
SELECT DISTINCT ON (b.{input_buildings_id})
    b.{input_buildings_id} AS building_id,
    s.{input_streets_id} AS street_id,
    ST_Distance(b.{input_buildings_geom}, s.{input_streets_geom}) AS distance
FROM 
    {input_buildings_schema}.{input_buildings_table} AS b
CROSS JOIN LATERAL (
    SELECT 
        {input_streets_id},
        {input_streets_geom}
    FROM 
        {input_streets_schema}.{input_streets_table} AS s
    ORDER BY 
        b.{input_buildings_geom} <-> s.{input_streets_geom}
    LIMIT 1
) AS s
ORDER BY 
    b.{input_buildings_id};
-- Create index on building_id for faster lookups
CREATE INDEX idx_{output_table}_building_id ON {output_schema}.{output_table}(building_id);

-- Create index on street_id for faster lookups
CREATE INDEX idx_{output_table}_street_id ON {output_schema}.{output_table}(street_id);

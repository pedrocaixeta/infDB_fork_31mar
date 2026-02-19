-- Calculate linear heat density for street segments
-- This table stores the heat demand per unit length for each street
CREATE SCHEMA IF NOT EXISTS {output_schema};
CREATE TABLE IF NOT EXISTS {output_schema}.{output_table} (
    street_id TEXT PRIMARY KEY,
    geom GEOMETRY,
    total_heat_demand NUMERIC,
    street_length NUMERIC,
    linear_heat_density NUMERIC,
    gemeindeschluessel TEXT
);

-- Pre-calculate street lengths to avoid repeated ST_Length calls
WITH street_lengths AS (
    -- Get all streets that intersect with the specified municipality (AGS)
    SELECT
        {streets_id_expr} AS street_id,
        s.{streets_geom} AS geom,
        ST_Length(s.{streets_geom}) AS street_length
    FROM
        {streets_schema}.{streets_table} AS s
    JOIN
        opendata.bkg_vg5000_gem AS gem
    ON
        ST_Intersects(s.{streets_geom}, gem.geom)
    WHERE
        gem.ags = '{ags}'
),
-- Aggregate heat demand for each street from connected buildings
street_heat_demand AS (
    SELECT
        bts.{buildings_to_streets_ways_id_column},
        SUM(h.{heat_demand_column}) AS total_heat_demand
    FROM
        {buildings_to_streets_schema}.{buildings_to_streets_table} AS bts
    JOIN
        {heat_demand_schema}.{heat_demand_table} AS h
    ON
        bts.{buildings_to_streets_building_id_column}::text = {heat_demand_id_expr}
    WHERE bts.gemeindeschluessel = '{ags}'
    GROUP BY
        bts.{buildings_to_streets_ways_id_column}
)
-- Insert or update records with calculated linear heat density
INSERT INTO {output_schema}.{output_table}
SELECT
    sl.street_id,
    sl.geom,
    COALESCE(shd.total_heat_demand, 0),
    sl.street_length,
    -- Calculate linear heat density (heat demand per meter of street)
    CASE WHEN sl.street_length > 0 
        THEN COALESCE(shd.total_heat_demand, 0) / sl.street_length 
        ELSE 0 END,
    '{ags}' 

FROM
    street_lengths AS sl
LEFT JOIN
    street_heat_demand AS shd ON sl.street_id = shd.street_id
ON CONFLICT (street_id) DO UPDATE SET
    geom = EXCLUDED.geom,
    total_heat_demand = EXCLUDED.total_heat_demand,
    street_length = EXCLUDED.street_length,
    linear_heat_density = EXCLUDED.linear_heat_density,
    gemeindeschluessel = EXCLUDED.gemeindeschluessel;

-- Create spatial index for efficient geometric queries
CREATE INDEX IF NOT EXISTS idx_{output_table}_geom ON {output_schema}.{output_table} USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_{output_table}_gemeindeschluessel ON {output_schema}.{output_table} (gemeindeschluessel);
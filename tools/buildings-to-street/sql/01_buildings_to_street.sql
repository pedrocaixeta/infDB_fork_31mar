-- =====================================================================
-- Buildings to Street Assignment Script
-- =====================================================================
-- Purpose: Assigns each building to its nearest street based on geometric distance
-- Performance: Uses spatial indexes and LATERAL joins for optimal performance
-- =====================================================================

-- Drop the output table if it exists
DROP TABLE IF EXISTS {output_schema}.{output_table};

-- Create temporary tables with unique IDs if needed
-- This ensures we have stable IDs even if source tables don't have them

DROP TABLE IF EXISTS temp_buildings_with_id;
CREATE TEMP TABLE temp_buildings_with_id AS
SELECT 
    COALESCE({buildings_id}::TEXT, 'building_' || ROW_NUMBER() OVER ()) AS building_id,
    {buildings_geom} AS building_geom
FROM 
    {buildings_schema}.{buildings_table};

-- Create spatial index on temporary buildings table
CREATE INDEX idx_temp_buildings_geom ON temp_buildings_with_id USING GIST(building_geom);

DROP TABLE IF EXISTS temp_streets_with_id;
CREATE TEMP TABLE temp_streets_with_id AS
SELECT 
    COALESCE({streets_id}::TEXT, 'street_' || ROW_NUMBER() OVER ()) AS street_id,
    {streets_geom} AS street_geom
FROM 
    {streets_schema}.{streets_table};

-- Create spatial index on temporary streets table
CREATE INDEX idx_temp_streets_geom ON temp_streets_with_id USING GIST(street_geom);

-- Analyze temp tables for better query planning
ANALYZE temp_buildings_with_id;
ANALYZE temp_streets_with_id;

-- Create the output table with building-to-street assignments
CREATE TABLE {output_schema}.{output_table} AS
WITH building_street_distances AS (
    -- For each building, find the nearest street using LATERAL join
    SELECT 
        b.building_id,
        b.building_geom,
        nearest.street_id,
        nearest.street_geom,
        nearest.distance
    FROM 
        temp_buildings_with_id b
    CROSS JOIN LATERAL (
        -- Find the single nearest street for this building
        SELECT 
            s.street_id,
            s.street_geom,
            ST_Distance(b.building_geom, s.street_geom) AS distance
        FROM 
            temp_streets_with_id s
        WHERE 
            -- Use bounding box filter first (uses spatial index)
            s.street_geom && ST_Expand(b.building_geom, 100)
        ORDER BY 
            -- Order by actual distance to get the nearest
            b.building_geom <-> s.street_geom
        LIMIT 1
    ) nearest
)
SELECT 
    building_id,
    street_id,
    distance,
    -- Add geometry columns for potential visualization
    building_geom,
    street_geom,
    -- Create a line connecting building centroid to nearest point on street
    ST_MakeLine(
        ST_Centroid(building_geom),
        ST_ClosestPoint(street_geom, building_geom)
    ) AS connection_line
FROM 
    building_street_distances
ORDER BY 
    building_id;

-- =====================================================================
-- Create indexes for performance
-- =====================================================================

-- Primary key
ALTER TABLE {output_schema}.{output_table} 
ADD PRIMARY KEY (building_id);

-- Index on street_id for joining
CREATE INDEX idx_{output_table}_street_id 
ON {output_schema}.{output_table}(street_id);

-- Index on distance for filtering
CREATE INDEX idx_{output_table}_distance 
ON {output_schema}.{output_table}(distance);

-- Spatial index on building geometry
CREATE INDEX idx_{output_table}_building_geom 
ON {output_schema}.{output_table} USING GIST(building_geom);

-- Spatial index on street geometry
CREATE INDEX idx_{output_table}_street_geom 
ON {output_schema}.{output_table} USING GIST(street_geom);

-- Spatial index on connection line
CREATE INDEX idx_{output_table}_connection_line 
ON {output_schema}.{output_table} USING GIST(connection_line);

-- =====================================================================
-- Summary statistics
-- =====================================================================

-- Display summary of assignments
DO $$
DECLARE
    total_buildings INTEGER;
    avg_distance NUMERIC;
    max_distance NUMERIC;
    min_distance NUMERIC;
BEGIN
    SELECT 
        COUNT(*),
        ROUND(AVG(distance)::NUMERIC, 2),
        ROUND(MAX(distance)::NUMERIC, 2),
        ROUND(MIN(distance)::NUMERIC, 2)
    INTO 
        total_buildings,
        avg_distance,
        max_distance,
        min_distance
    FROM 
        {output_schema}.{output_table};
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Buildings to Street Assignment Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total buildings assigned: %', total_buildings;
    RAISE NOTICE 'Average distance: % meters', avg_distance;
    RAISE NOTICE 'Minimum distance: % meters', min_distance;
    RAISE NOTICE 'Maximum distance: % meters', max_distance;
    RAISE NOTICE '========================================';
END $$;

    

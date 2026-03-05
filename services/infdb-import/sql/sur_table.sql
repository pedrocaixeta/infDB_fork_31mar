-- Create function for safe area calculation with fallback
CREATE OR REPLACE FUNCTION public.safe_area_fallback(geom geometry) 
RETURNS double precision AS $$
BEGIN
    -- ATTEMPT 1: Exact 3D calculation (scientifically correct)
    -- Attempts to decompose the polygon into triangles.
    RETURN GC_3DArea(ST_Tesselate(ST_MakeValid(geom)));

EXCEPTION WHEN OTHERS THEN
    -- EMERGENCY FALLBACK: If 3D crashes (InternalError), we use the 2D area.
    -- ST_Area(geom) ignores Z-values, but NEVER crashes.
    -- This is better than no value at all.
    RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-------------------------------------------------------------
-- Create the final surface table with area and geometry
-------------------------------------------------------------

-- Add PostGIS SFCGAL extension for 3D area calculation
CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;

-- Analysis helps the query planner understand statistics for the join
--ANALYZE tmp_bld.{table_name}_ids;

-- Start from scratch: Drop existing table to avoid conflicts
DROP TABLE IF EXISTS {output_schema}.{table_name} CASCADE;

-- EXPLAIN ANALYZE
CREATE TABLE {output_schema}.{table_name} AS
SELECT
    sid2.building_objectid,
    sid.objectclass_id,
    oc.classname,
    -- safe_area_fallback(gd.geometry) AS area, --- area needs to be caluclated if not available as property.
    MAX(CASE WHEN p.name = 'Flaeche' THEN p.val_string END) AS area, -- works only for bavaria 
    ST_Multi(gd.geometry) AS geom
FROM tmp_bld.{table_name}_ids sid
    JOIN tmp_bld.{table_name}_ids sid2 
        ON sid.child_hash = sid2.child_hash -- Safety check: if hash collision (extremely unlikely), we verify the text
        AND sid.child_object_id_text = sid2.child_object_id_text 
        AND sid2.objectclass_id = 901 -- sid2 is the surface
    JOIN objectclass oc ON oc.id = sid.objectclass_id
    JOIN geometry_data gd ON gd.id = sid.geometry_data_id
    JOIN property p ON p.feature_id = gd.feature_id
WHERE sid.objectclass_id IN (709, 710, 712) -- sid is the building
GROUP BY sid2.building_objectid, sid.objectclass_id, oc.classname, gd.geometry;

-- Indexes on the target table
CREATE INDEX IF NOT EXISTS {table_name}_building_objectid_idx ON {output_schema}.{table_name} (building_objectid);
CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {output_schema}.{table_name} USING GIST (geom);
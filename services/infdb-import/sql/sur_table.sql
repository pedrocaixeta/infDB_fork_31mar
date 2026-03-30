-- ============================================================
-- Function: public.safe_area_fallback(geometry)
-- Purpose:
--   - Provides a resilient, multi-stage calculation for 3D surface area.
--   - Specifically designed to handle "dirty" architectural data (walls, roofs).
--   - Bypasses strict SFCGAL planarity requirements while maintaining accuracy.
-- Logic Flow:
--   1. PRIMARY ATTEMPT: CG_3DArea (SFCGAL)
--      - Uses the high-precision SFCGAL kernel for mathematically exact results.
--      - Fails Case: If a 3D polygon is "non-planar" (warped). Even a microscopic
--        floating-point deviation prevents SFCGAL from identifying a single flat plane.
--   2. SECONDARY ATTEMPT: Newell's Method (Vector Cross Product)
--      - Fallback for non-planar geometries. It calculates the "Vector Area" by
--        summing the cross products of all edges in 3D space.
--      - Fails Case: Perfectly vertical walls that are "zero-thickness" in the XY
--        plane (collinear). In this orientation, the 3D vector components can
--        mathematically collapse to zero, even if the wall has significant height.
--   3. FINAL FALLBACK: 2D Plane Projection
--      - Used when 3D calculations return zero for shapes that clearly have area.
--      - Logic: "Tips" the geometry onto its side (XZ or YZ planes) using rotation.
--      - This forces the database to see the "face" of a vertical wall as a 2D
--        surface, allowing a standard ST_Area calculation to capture the area.
-- Safety:
--   - Uses EXCEPTION blocks to prevent "Invalid Geometry" errors from crashing queries.
--   - Handles MultiPolygons by decomposing them and summing individual part areas.
--   - Returns 0.0 only if the geometry is truly a point or a line.
-- ============================================================

CREATE OR REPLACE FUNCTION public.safe_area_fallback(geom geometry) 
RETURNS double precision AS $$
DECLARE
    total_area double precision := 0;
    poly_part geometry;
    cp_x double precision; cp_y double precision; cp_z double precision;
    pt record;
BEGIN
    -- 1. Try SFCGAL (Best for valid planar shapes)
    BEGIN
        total_area := CG_3DArea(geom);
        IF total_area > 0 THEN RETURN ROUND(total_area::numeric, 3)::double precision; END IF;
    EXCEPTION WHEN OTHERS THEN 
        -- Continue to manual
    END;

    -- 2. Loop through parts for MultiPolygons
    FOR poly_part IN SELECT (ST_Dump(geom)).geom LOOP
        cp_x := 0; cp_y := 0; cp_z := 0;

        FOR pt IN (
            SELECT 
                ST_X(p) as x, ST_Y(p) as y, ST_Z(p) as z,
                lead(ST_X(p)) OVER () as next_x,
                lead(ST_Y(p)) OVER () as next_y,
                lead(ST_Z(p)) OVER () as next_z
            FROM (SELECT (ST_DumpPoints(poly_part)).geom as p) AS d
        ) LOOP
            IF pt.next_x IS NOT NULL THEN
                cp_x := cp_x + (pt.y * pt.next_z - pt.z * pt.next_y);
                cp_y := cp_y + (pt.z * pt.next_x - pt.x * pt.next_z);
                cp_z := cp_z + (pt.x * pt.next_y - pt.y * pt.next_x);
            END IF;
        END LOOP;
        
        total_area := total_area + (0.5 * SQRT(POW(cp_x, 2) + POW(cp_y, 2) + POW(cp_z, 2)));
    END LOOP;

    -- 3. FINAL FALLBACK: If still 0, it's a perfectly vertical wall on a line.
    -- We project to the XZ or YZ plane to catch the "missing" area.
    IF total_area = 0 THEN
        RETURN ROUND(
            GREATEST(
                ST_Area(ST_Transform(ST_SnapToGrid(geom, 0.0001), 0)),
                ST_Area(ST_Force2D(ST_RotateX(geom, pi()/2))),
                ST_Area(ST_Force2D(ST_RotateY(geom, pi()/2)))
            )::numeric,
            3
        )::double precision;
    END IF;

    RETURN ROUND(total_area::numeric, 3)::double precision;
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
    COALESCE(
        NULLIF(MAX(CASE WHEN p.name = 'Flaeche' THEN p.val_string END)::double precision, 0),
        safe_area_fallback(gd.geometry)
    ) AS area,
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
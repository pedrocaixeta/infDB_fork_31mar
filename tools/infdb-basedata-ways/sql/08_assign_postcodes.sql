-- ============================================================
-- Populate postcode for ways_tem and connection_lines_tem using postcode polygons
--
-- Notes:
-- - Detects a target SRID once (from postcodes_germany.geom; falls back to {epsg} if SRID is 0/NULL)
-- - For each table, computes a representative point per geometry (ST_PointOnSurface)
-- - Transforms that point into the target SRID for consistent spatial predicates
-- - Joins against {input_schema}.postcodes_germany using bbox prefilter (&&) plus ST_Intersects
-- - Updates only rows with postcode IS NULL and valid, non-empty geometries
-- ============================================================

-- Precompute the target SRID once
DO $$
DECLARE
    v_srid integer; -- target SRID used for point transformation and intersection checks
BEGIN
    SELECT COALESCE(NULLIF(ST_SRID(geom), 0), {epsg}) -- use table SRID if set, else fallback to {epsg}
    INTO v_srid
    FROM {input_schema}.postcodes_germany
    WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) -- require a valid polygon geometry
    LIMIT 1;

    -- Update ways_tem
    UPDATE ways_tem w
    SET postcode = pc.plz::int -- assign postcode as integer
    FROM (
        SELECT
            ctid AS rid, -- row identifier used for stable join back to ways_tem
            ST_Transform(ST_PointOnSurface(geom), v_srid) AS pt -- representative point in target SRID
        FROM ways_tem
        WHERE postcode IS NULL
          AND geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
    ) pts
    JOIN {input_schema}.postcodes_germany pc
        ON pc.geom && pts.pt -- bbox prefilter for index usage
       AND ST_Intersects(pc.geom, pts.pt) -- point-in-polygon check
    WHERE w.ctid = pts.rid; -- update only the intended rows

    -- Update connection_lines_tem
    UPDATE connection_lines_tem w
    SET postcode = pc.plz::int -- assign postcode as integer
    FROM (
        SELECT
            ctid AS rid, -- row identifier used for stable join back to connection_lines_tem
            ST_Transform(ST_PointOnSurface(geom), v_srid) AS pt -- representative point in target SRID
        FROM connection_lines_tem
        WHERE postcode IS NULL
          AND geom IS NOT NULL
          AND NOT ST_IsEmpty(geom)
    ) pts
    JOIN {input_schema}.postcodes_germany pc
        ON pc.geom && pts.pt -- bbox prefilter for index usage
       AND ST_Intersects(pc.geom, pts.pt) -- point-in-polygon check
    WHERE w.ctid = pts.rid; -- update only the intended rows

END;
$$;
-- ============================================================
-- 04_d_filter_short_and_isolated_ways.sql
-- Delete ways that are:
-- 1) Loops (start point = end point)
-- 2) Short dead-end/junction ways (< {min_length_deadend_junction} meters)
-- 3) Isolated ways (not connected to any other way)
-- Then reassign affected buildings to appropriate alternatives
-- Note: Designed for parallel processing - each AGS in separate container
-- ============================================================


-- Table to store deleted way IDs and their replacement ways
DROP TABLE IF EXISTS filtered_ways;
CREATE TEMP TABLE filtered_ways (
    old_way_id text NOT NULL,
    new_way_id text  -- can be NULL for isolated ways (will use nearest)
);
CREATE INDEX ON filtered_ways (old_way_id);

DO $$
DECLARE
    v_min_length double precision := {min_length_deadend_junction}::double precision;
    v_snap_tol double precision := 0.25; -- same as merge tolerance
    r RECORD;
    v_connected_way text;
BEGIN
    -- Process each way to determine if it should be filtered
    FOR r IN
        SELECT
            w.id::text AS way_id,
            w.geom,
            ST_Length(w.geom) AS way_length,
            ST_StartPoint(w.geom) AS start_pt,
            ST_EndPoint(w.geom) AS end_pt,
            -- Check if it's a loop
            ST_Equals(ST_StartPoint(w.geom), ST_EndPoint(w.geom)) AS is_loop,
            -- Count connections at start
            (SELECT COUNT(*)
             FROM ways_tem w2
             WHERE w2.id::text <> w.id::text
               AND w2.geom IS NOT NULL
               AND (ST_DWithin(w2.geom, ST_StartPoint(w.geom), v_snap_tol)
                    OR ST_DWithin(w2.geom, ST_EndPoint(w.geom), v_snap_tol))
            ) AS connection_count
        FROM ways_tem w
        WHERE w.geom IS NOT NULL
          AND GeometryType(w.geom) = 'LINESTRING'
          AND NOT ST_IsEmpty(w.geom)
    LOOP
        -- Determine if this way should be deleted
        IF (
            -- Case 1: Loop
            r.is_loop
            --OR
            -- Case 2: Short way
            --r.way_length < v_min_length
            OR
            -- Case 3: Isolated way (no connections)
            r.connection_count = 0
        ) THEN
            -- Find replacement way for buildings
            v_connected_way := NULL;

            -- For loops and short ways: try to find a connected way
            IF NOT (r.connection_count = 0) THEN
                -- Find the first connected way (prefer connections at endpoints)
                SELECT w2.id::text INTO v_connected_way
                FROM ways_tem w2
                WHERE w2.id::text <> r.way_id
                  AND w2.geom IS NOT NULL
                  AND (ST_DWithin(w2.geom, r.start_pt, v_snap_tol)
                       OR ST_DWithin(w2.geom, r.end_pt, v_snap_tol))
                ORDER BY LEAST(
                    ST_Distance(w2.geom, r.start_pt),
                    ST_Distance(w2.geom, r.end_pt)
                ) ASC
                LIMIT 1;
            END IF;

            -- Record the deletion and replacement
            INSERT INTO filtered_ways (old_way_id, new_way_id)
            VALUES (r.way_id, v_connected_way);

            -- Log what we're doing
            IF r.is_loop THEN
                RAISE NOTICE 'Deleting loop way: % (length: %m, connected to: %)',
                    r.way_id, ROUND(r.way_length::numeric, 2), COALESCE(v_connected_way, 'NONE');
            ELSIF r.way_length < v_min_length THEN
                RAISE NOTICE 'Deleting short way: % (length: %m, min: %m, connected to: %)',
                    r.way_id, ROUND(r.way_length::numeric, 2), v_min_length, COALESCE(v_connected_way, 'NONE');
            ELSIF r.connection_count = 0 THEN
                RAISE NOTICE 'Deleting isolated way: % (length: %m, no connections)',
                    r.way_id, ROUND(r.way_length::numeric, 2);
            END IF;
        END IF;
    END LOOP;

    -- Delete the filtered ways from ways_tem
    DELETE FROM ways_tem
    WHERE id::text IN (SELECT old_way_id FROM filtered_ways);

    RAISE NOTICE 'Deleted % ways total', (SELECT COUNT(*) FROM filtered_ways);
END $$;

-- Reassign buildings affected by deleted ways
-- This handles both cases:
-- 1) Ways with new_way_id: direct reassignment to connected way
-- 2) Ways with NULL new_way_id (isolated): will fall back to nearest way logic
SELECT {output_schema}.update_assigned_way_id(
    '{ags}',
    'filtered_ways'::regclass,
    'old_way_id',
    'new_way_id'
) AS buildings_updated;
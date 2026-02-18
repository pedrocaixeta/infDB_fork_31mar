/* =====================================================================================
   SCRIPT 3: Orchestrator (create connection lines + split ways)
   ------------------------------------------------------------
   End-to-end pipeline:

   Phase 1) Build temp candidates
     - {output_schema}.generate_building_way_connection_candidates()

   Phase 2) Insert connection lines into ways_tem
     - If connection point is near start/end of the way (< 0.1), connect to that endpoint
       and remove the candidate (no splitting needed).
     - Else insert the precomputed shortest line (splitting required later).

   Phase 3) Group remaining connection points by affected way
     - Builds TEMP TABLE grouped_splits with ordered connection point arrays.
     - Only executed if {connection_line_segmentation} is true

   Phase 4) Split ways at those points and reinsert the resulting segments
     - Deletes the original way row from ways_tem (by id)
     - Calls split_way_at_connection_points(old_geom, connection_points)
     - Inserts each segment back via {output_schema}.insert_way_segment()
     - Only executed if {connection_line_segmentation} is true

   Requires:
     - split_way_at_connection_points(geometry, geometry[]) exists and returns SETOF geometry
   ===================================================================================== */

DO $$
DECLARE
    s RECORD;
    part geometry;
    connection_line_segmentation boolean := {connection_line_segmentation};
BEGIN
    -- Phase 1: analysis (unchanged)
    PERFORM {output_schema}.generate_building_way_connection_candidates();


    -- Phase 2: insert connection lines — set-based, no row loop
    -- Near start point: connect to start, no split needed
    PERFORM {output_schema}.insert_way_segment(
        c.old_way_ags,
        'connection_line',
        ST_MakeLine(c.center, ST_StartPoint(c.old_geom))
    )
    FROM temp_building_connection_candidates c
    WHERE ST_Distance(ST_StartPoint(c.old_geom), c.connection_point) < 0.1;

    DELETE FROM temp_building_connection_candidates
    WHERE ST_Distance(ST_StartPoint(old_geom), connection_point) < 0.1;

    -- Near end point: connect to end, no split needed
    PERFORM {output_schema}.insert_way_segment(
        c.old_way_ags,
        'connection_line',
        ST_MakeLine(c.center, ST_EndPoint(c.old_geom))
    )
    FROM temp_building_connection_candidates c
    WHERE ST_Distance(ST_EndPoint(c.old_geom), c.connection_point) < 0.1;

    DELETE FROM temp_building_connection_candidates
    WHERE ST_Distance(ST_EndPoint(old_geom), connection_point) < 0.1;

    -- Middle points: insert shortest line as-is (splitting later)
    PERFORM {output_schema}.insert_way_segment(
        c.old_way_ags,
        'connection_line',
        c.new_geom
    )
    FROM temp_building_connection_candidates c;


    -- Phase 3 & 4: only if segmentation enabled
    IF connection_line_segmentation THEN

        -- Phase 3: group split points per way (unchanged, already set-based)
        DROP TABLE IF EXISTS grouped_splits;
        CREATE TEMP TABLE grouped_splits AS
        SELECT
            old_way_id,
            old_geom,
            old_way_ags,
            ARRAY_AGG(connection_point ORDER BY ST_LineLocatePoint(old_geom, connection_point)) AS connection_points
        FROM temp_building_connection_candidates
        GROUP BY old_way_id, old_geom, old_way_ags;

        -- Phase 4: batch delete originals first, then loop only for splitting
        -- (split function returns SETOF so we still need a loop, but DELETE is now bulk)
        DELETE FROM ways_tem
        WHERE id IN (SELECT old_way_id FROM grouped_splits);

        FOR s IN SELECT * FROM grouped_splits LOOP
            FOR part IN
                SELECT * FROM {output_schema}.split_way_at_connection_points(s.old_geom, s.connection_points)
            LOOP
                PERFORM {output_schema}.insert_way_segment(
                    s.old_way_ags,
                    'segmented_way',
                    part
                );
            END LOOP;
        END LOOP;

    END IF;
END;
$$;
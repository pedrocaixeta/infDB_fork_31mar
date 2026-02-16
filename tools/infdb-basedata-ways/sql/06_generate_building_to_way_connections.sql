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
    r RECORD;
    s RECORD;
    final_geom geometry;
    part geometry;
    connection_line_segmentation boolean := {connection_line_segmentation};
BEGIN
    -- Phase 1: analysis
    PERFORM {output_schema}.generate_building_way_connection_candidates();


    -- Phase 2: insert connection lines
    FOR r IN SELECT * FROM temp_building_connection_candidates
    LOOP
        -- Near start point: connect to start, no split needed
        IF ST_Distance(ST_StartPoint(r.old_geom), r.connection_point) < 0.1 THEN
            final_geom := ST_MakeLine(r.center, ST_StartPoint(r.old_geom));

            PERFORM {output_schema}.insert_way_segment(
                r.old_way_ags,
                'connection_line',
                final_geom
            );

            DELETE FROM temp_building_connection_candidates
            WHERE building_id = r.building_id;

        -- Near end point: connect to end, no split needed
        ELSIF ST_Distance(ST_EndPoint(r.old_geom), r.connection_point) < 0.1 THEN
            final_geom := ST_MakeLine(r.center, ST_EndPoint(r.old_geom));

            PERFORM {output_schema}.insert_way_segment(
                r.old_way_ags,
                'connection_line',
                final_geom
            );

            DELETE FROM temp_building_connection_candidates
            WHERE building_id = r.building_id;

        -- Middle: insert shortest line, splitting will happen later
        ELSE
            final_geom := r.new_geom;

            PERFORM {output_schema}.insert_way_segment(
                r.old_way_ags,
                'connection_line',
                final_geom
            );
        END IF;
    END LOOP;

    -- Phase 3 & 4: Only execute if connection_line_segmentation is true
    IF connection_line_segmentation THEN
        -- Phase 3: group split points per way
        DROP TABLE IF EXISTS grouped_splits;
        CREATE TEMP TABLE grouped_splits AS
        SELECT
            old_way_id,
            old_geom,
            old_way_ags,
            ARRAY_AGG(connection_point ORDER BY ST_LineLocatePoint(old_geom, connection_point)) AS connection_points
        FROM temp_building_connection_candidates
        GROUP BY old_way_id, old_geom, old_way_ags;

        -- Phase 4: split and reinsert segments
        FOR s IN SELECT * FROM grouped_splits
        LOOP
            -- Remove original way
            DELETE FROM ways_tem
            WHERE id = s.old_way_id;

            -- Insert split segments
            FOR part IN SELECT * FROM {output_schema}.split_way_at_connection_points(s.old_geom, s.connection_points)
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
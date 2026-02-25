-- ============================================================
-- End-to-end pipeline: generate and insert building connection lines, optionally split affected ways
--
-- Notes:
-- - Phase 1: Generates building-to-way connection candidates in a temp table
-- - Phase 2: Inserts connection lines into the connection_lines temp table via insert_way_segment()
--     - If connection point is near the start/end of the way (< 0.1), connect directly to that endpoint
--       and remove the candidate (no splitting required)
--     - Otherwise, insert the precomputed shortest line (splitting may be applied later)
-- - Phase 2b: Aggregates connection line lengths per affected way and updates ways_tem.length_connection_line
-- - Phase 3: Groups remaining connection points per affected way into grouped_splits (ordered along the line)
-- - Phase 4: Deletes original ways and reinserts split segments using split_way_at_connection_points()
-- - Phase 3/4 execute only when {connection_line_segmentation} is true
-- ============================================================

DO $$
DECLARE
    s RECORD; -- current grouped_splits row (old_way_id, old_geom, old_way_ags, connection_points)
    part geometry; -- one returned segment geometry from split_way_at_connection_points
    connection_line_segmentation boolean := {connection_line_segmentation}; -- feature flag for segmentation
BEGIN
    -- Phase 1: generate building-to-way connection candidates
    PERFORM {output_schema}.generate_building_way_connection_candidates();


    -- Phase 2: insert connection lines (near-start cases)
    PERFORM {output_schema}.insert_way_segment(
        c.old_way_ags,                                    -- AGS tag for inserted segment
        'connection_line',                                -- class routes insert into connection_lines_tem
        ST_MakeLine(c.center, ST_StartPoint(c.old_geom))   -- connect building center to way start endpoint
    )
    FROM temp_building_connection_candidates c
    WHERE ST_Distance(ST_StartPoint(c.old_geom), c.connection_point) < 0.1; -- near-start threshold

    DELETE FROM temp_building_connection_candidates
    WHERE ST_Distance(ST_StartPoint(old_geom), connection_point) < 0.1; -- remove handled candidates


    -- Phase 2: insert connection lines (near-end cases)
    PERFORM {output_schema}.insert_way_segment(
        c.old_way_ags,                                  -- AGS tag for inserted segment
        'connection_line',                              -- class routes insert into connection_lines_tem
        ST_MakeLine(c.center, ST_EndPoint(c.old_geom))   -- connect building center to way end endpoint
    )
    FROM temp_building_connection_candidates c
    WHERE ST_Distance(ST_EndPoint(c.old_geom), c.connection_point) < 0.1; -- near-end threshold

    DELETE FROM temp_building_connection_candidates
    WHERE ST_Distance(ST_EndPoint(old_geom), connection_point) < 0.1; -- remove handled candidates


    -- Phase 2: insert remaining connection lines (mid-point cases)
    PERFORM {output_schema}.insert_way_segment(
        c.old_way_ags,          -- AGS tag for inserted segment
        'connection_line',      -- class routes insert into connection_lines_tem
        c.new_geom              -- precomputed shortest connection line
    )
    FROM temp_building_connection_candidates c;


    -- Phase 2b: accumulate total connection line length per old_way_id into ways_tem.length_connection_line
    UPDATE ways_tem w
    SET length_connection_line = COALESCE(length_connection_line, 0) + agg.total_length -- accumulate length
    FROM (
        SELECT
            old_way_id,                      -- affected way id
            SUM(ST_Length(new_geom)) AS total_length -- summed connection line length for that way
        FROM temp_building_connection_candidates
        GROUP BY old_way_id
    ) agg
    WHERE w.id = agg.old_way_id; -- match affected way row


    -- Phase 3 & 4: only if segmentation is enabled
    IF connection_line_segmentation THEN

        -- Phase 3: group ordered split points per affected way
        DROP TABLE IF EXISTS grouped_splits;
        CREATE TEMP TABLE grouped_splits AS
        SELECT
            old_way_id, -- way id to be split
            old_geom,   -- original way geometry to split
            old_way_ags, -- AGS tag of the way
            ARRAY_AGG(connection_point ORDER BY ST_LineLocatePoint(old_geom, connection_point)) AS connection_points -- ordered points
        FROM temp_building_connection_candidates
        GROUP BY old_way_id, old_geom, old_way_ags;

        -- Phase 4: delete original ways prior to reinserting split segments
        DELETE FROM ways_tem
        WHERE id IN (SELECT old_way_id FROM grouped_splits); -- bulk delete originals

        -- Reinsert split segments for each affected way
        FOR s IN SELECT * FROM grouped_splits LOOP
            FOR part IN
                SELECT * FROM {output_schema}.split_way_at_connection_points(s.old_geom, s.connection_points) -- split into segments
            LOOP
                PERFORM {output_schema}.insert_way_segment(
                    s.old_way_ags,     -- AGS tag for inserted segment
                    'segmented_way',   -- class label for inserted segment
                    part               -- segment geometry
                );
            END LOOP;
        END LOOP;

    END IF;
END;
$$;
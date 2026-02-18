-- ============================================================
-- 00_b_initalization.sql
-- Creates shared functions and tables for parallel AGS processing
-- 
-- Purpose:
--   - Creates shared functions used across all workers
--   - Creates the global ways_segmented output table
--   - Uses advisory lock to prevent race conditions
--
-- Safety:
--   - Uses pg_try_advisory_lock() for atomic coordination
--   - First worker to acquire lock creates resources
--   - Other workers wait 3 seconds then proceed
--   - All workers verify resources exist before continuing
--
-- Expected Duration: ~2-3 seconds for resource creation
-- ============================================================

DO $$
DECLARE
    -- Unique lock key for initialization coordination
    -- All workers compete for this same lock
    lock_key bigint := 999999999;
    
    -- Flag indicating if this worker acquired the lock
    got_lock boolean;
BEGIN
    -- ================================================================
    -- STEP 1: Try to acquire initialization lock (non-blocking)
    -- ================================================================
    -- pg_try_advisory_lock returns immediately:
    --   TRUE  = lock acquired, this worker will create resources
    --   FALSE = lock held by another worker, wait for them to finish
    got_lock := pg_try_advisory_lock(lock_key);
    
    IF got_lock THEN
        -- ============================================================
        -- PATH A: This worker won the lock race
        -- ============================================================
        RAISE NOTICE '[Init] Lock acquired - checking if resources need creation...';
        
        -- Check if resources already exist (idempotency check)
        -- This handles cases where:
        --   1) Script is run multiple times
        --   2) Previous run was interrupted
        --   3) Resources were created manually
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = '{output_schema}' 
              AND tablename = 'ways_segmented'
        ) THEN
            -- ========================================================
            -- Resources don't exist - create them now
            -- ========================================================
            RAISE NOTICE '[Init] Creating shared resources...';
            
            -- --------------------------------------------------------
            -- Function 1: update_assigned_way_id
            -- --------------------------------------------------------
            -- Updates buildings.assigned_way_id based on filtered ways
            -- with fallback to nearest suitable way
            CREATE OR REPLACE FUNCTION {output_schema}.update_assigned_way_id(
                p_ags text,
                p_mapping_table regclass,
                p_old_way_col text,
                p_new_way_col text DEFAULT 'new_way_id'
            )
            RETURNS bigint
            LANGUAGE plpgsql
            AS $func$
            DECLARE
                v_updated bigint := 0;
                v_sql text;
            BEGIN
                v_sql := format(
                    'WITH buildings_to_reassign AS (
                        SELECT
                            b.id,
                            b.centroid,
                            b.assigned_way_id,
                            m.%I AS old_way_id,
                            m.%I AS new_way_id
                        FROM {output_schema}.buildings AS b
                        JOIN %s AS m ON b.assigned_way_id = m.%I
                        WHERE b.centroid IS NOT NULL
                          AND b.gemeindeschluessel = %L
                    ),
                    best_replacement AS (
                        SELECT
                            btr.id,
                            COALESCE(
                                btr.new_way_id,
                                (SELECT w.id::text
                                 FROM ways_tem w
                                 WHERE w.klasse <> ''110''
                                   AND ST_DWithin(btr.centroid, w.geom, 2000)
                                   AND ST_Distance(btr.centroid, w.geom) > 0.1
                                   AND w.id::text <> btr.old_way_id
                                 ORDER BY btr.centroid <-> w.geom
                                 LIMIT 1
                                )
                            ) AS assigned_way_id
                        FROM buildings_to_reassign btr
                    )
                    UPDATE {output_schema}.buildings AS b
                    SET assigned_way_id = br.assigned_way_id
                    FROM best_replacement br
                    WHERE b.id = br.id
                      AND br.assigned_way_id IS NOT NULL',
                    p_old_way_col,
                    p_new_way_col,
                    p_mapping_table,
                    p_old_way_col,
                    p_ags
                );

                EXECUTE v_sql;
                GET DIAGNOSTICS v_updated = ROW_COUNT;
                RAISE NOTICE 'Updated % buildings for AGS %', v_updated, p_ags;
                
                RETURN v_updated;
            END;
            $func$;
            
            RAISE NOTICE '[Init] ✓ Created update_assigned_way_id function';
            
            -- --------------------------------------------------------
            -- Function 2: insert_way_segment
            -- --------------------------------------------------------
            -- Inserts a new way segment into ways_tem
            CREATE OR REPLACE FUNCTION {output_schema}.insert_way_segment(
                p_ags text,
                p_klasse text,
                p_geom geometry
            ) RETURNS void
            LANGUAGE plpgsql
            AS $func$
            DECLARE
                v_new text;
            BEGIN
                IF p_geom IS NULL THEN
                    RETURN;
                END IF;

                v_new := md5(random()::text || clock_timestamp()::text);

                INSERT INTO ways_tem (ags, id, klasse, objektart, geom, postcode)
                VALUES (p_ags, v_new, p_klasse, NULL, p_geom, NULL);
            END;
            $func$;
            
            RAISE NOTICE '[Init] ✓ Created insert_way_segment function';
            
            -- --------------------------------------------------------
            -- Function 3: split_way_at_connection_points
            -- --------------------------------------------------------
            -- Splits a line geometry into multiple segments at specified points
            CREATE OR REPLACE FUNCTION {output_schema}.split_way_at_connection_points(
                line geometry, 
                points geometry[]
            )
            RETURNS TABLE(part geometry) 
            LANGUAGE plpgsql
            AS $func$
            DECLARE
                i INTEGER;
                start_fraction FLOAT := 0;
                end_fraction FLOAT;
            BEGIN
                -- Iterate through all split points
                FOR i IN 1 .. array_length(points, 1)
                LOOP
                    -- Calculate fractional position of current point along line
                    end_fraction := ST_LineLocatePoint(line, points[i]);
                    
                    -- Extract segment if valid (end > start)
                    IF end_fraction > start_fraction THEN
                        RETURN QUERY
                        SELECT ST_LineSubstring(line, start_fraction, end_fraction);
                    END IF;
                    
                    -- Update start position for next iteration
                    start_fraction := end_fraction;
                END LOOP;

                -- Handle final segment (last point to line end)
                IF start_fraction < 1 THEN
                    RETURN QUERY
                    SELECT ST_LineSubstring(line, start_fraction, 1);
                END IF;
            END;
            $func$;
            
            RAISE NOTICE '[Init] ✓ Created split_way_at_connection_points function';
            
            -- --------------------------------------------------------
            -- Function 4: generate_building_way_connection_candidates
            -- --------------------------------------------------------
            -- Creates temp table with building-to-way connection analysis
            CREATE OR REPLACE FUNCTION {output_schema}.generate_building_way_connection_candidates()
            RETURNS void
            LANGUAGE plpgsql
            AS $func$
            BEGIN
                DROP TABLE IF EXISTS temp_building_connection_candidates;

                CREATE TEMP TABLE temp_building_connection_candidates AS
                WITH b AS (
                    SELECT
                        b.id               AS building_id,
                        b.centroid         AS center,
                        b.assigned_way_id
                    FROM {output_schema}.buildings b
                    WHERE b.centroid IS NOT NULL
                      AND b.assigned_way_id IS NOT NULL
                ),
                matched AS (
                    SELECT
                        b.building_id,
                        b.center,
                        w.id               AS old_way_id,
                        w.geom             AS old_geom,
                        w.ags              AS old_way_ags
                    FROM b
                    JOIN ways_tem w
                      ON w.id = b.assigned_way_id::text
                    WHERE w.geom IS NOT NULL
                      AND w.klasse <> 'connection_line'
                ),
                computed AS (
                    SELECT
                        m.building_id,
                        m.center,
                        m.old_way_id,
                        m.old_geom,
                        m.old_way_ags,
                        ST_ShortestLine(m.center, m.old_geom) AS new_geom,
                        ST_ClosestPoint(m.old_geom, m.center) AS connection_point
                    FROM matched m
                )
                SELECT DISTINCT ON (building_id)
                    building_id,
                    center,
                    new_geom,
                    old_way_id,
                    old_geom,
                    old_way_ags,
                    connection_point
                FROM computed;

                CREATE INDEX temp_candidates_old_way_idx
                    ON temp_building_connection_candidates (old_way_id);

                CREATE INDEX temp_candidates_connection_gix
                    ON temp_building_connection_candidates
                    USING GIST (connection_point);
            END;
            $func$;
            
            RAISE NOTICE '[Init] ✓ Created generate_building_way_connection_candidates function';
            
            -- --------------------------------------------------------
            -- Table: ways_segmented (global output table)
            -- --------------------------------------------------------
            -- Create empty table with same structure as ways_tem
            CREATE TABLE {output_schema}.ways_segmented AS
            SELECT
                ags,
                id,
                klasse,
                objektart,
                geom,
                postcode
            FROM ways_tem
            WHERE false;  -- Creates structure only, no data

            -- Set NOT NULL constraints
            ALTER TABLE {output_schema}.ways_segmented
                ALTER COLUMN ags SET NOT NULL,
                ALTER COLUMN id  SET NOT NULL;

            -- Create indexes for performance
            CREATE INDEX ways_segmented_ags_idx
                ON {output_schema}.ways_segmented (ags);

            CREATE INDEX ways_segmented_geom_gix
                ON {output_schema}.ways_segmented USING GIST (geom);

            -- Prevent duplicates per AGS+id
            CREATE UNIQUE INDEX ways_segmented_ags_id_ux
                ON {output_schema}.ways_segmented (ags, id);
            
            RAISE NOTICE '[Init] ✓ Created ways_segmented table with indexes';


            -- --------------------------------------------------------
            -- Table: nodes (global output table)
            -- Columns:
            --   ags      text      NOT NULL
            --   id       text      NOT NULL   -- unique node id (or use uuid)
            --   way_ids  text[]    NOT NULL   -- list of way ids belonging to the node
            -- --------------------------------------------------------

            DROP TABLE IF EXISTS {output_schema}.nodes;

            -- Create empty table structure (no rows)
            CREATE TABLE {output_schema}.nodes AS
            SELECT
                CAST(NULL AS text)   AS ags,
                CAST(NULL AS text)   AS id,
                CAST(NULL AS text[]) AS way_ids
            WHERE false;

            -- Constraints
            ALTER TABLE {output_schema}.nodes
                ALTER COLUMN ags     SET NOT NULL,
                ALTER COLUMN id      SET NOT NULL,
                ALTER COLUMN way_ids SET NOT NULL;

            -- Optional defaults
            ALTER TABLE {output_schema}.nodes
                ALTER COLUMN way_ids SET DEFAULT ARRAY[]::text[];

            -- Indexes
            CREATE INDEX nodes_ags_idx
                ON {output_schema}.nodes (ags);

            CREATE INDEX nodes_way_ids_gin
                ON {output_schema}.nodes USING GIN (way_ids);

            -- Prevent duplicates per AGS+id
            CREATE UNIQUE INDEX nodes_ags_id_ux
                ON {output_schema}.nodes (ags, id);

            RAISE NOTICE '[Init] ✓ Created nodes table with indexes';


            
            -- --------------------------------------------------------
            -- Column: buildings.assigned_way_id
            -- --------------------------------------------------------
            -- Ensure the column exists (idempotent)
            ALTER TABLE {output_schema}.buildings
                ADD COLUMN IF NOT EXISTS assigned_way_id text;
            
            RAISE NOTICE '[Init] ✓ Ensured buildings.assigned_way_id column exists';
            
            RAISE NOTICE '[Init] ✓✓✓ All resources created successfully ✓✓✓';
            
        ELSE
            -- ========================================================
            -- Resources already exist - skip creation
            -- ========================================================
            RAISE NOTICE '[Init] Resources already exist, skipping creation';
        END IF;
        
        -- Release the lock so other workers can proceed
        PERFORM pg_advisory_unlock(lock_key);
        RAISE NOTICE '[Init] Lock released';
        
    ELSE
        -- ============================================================
        -- PATH B: Another worker is creating resources
        -- ============================================================
        RAISE NOTICE '[Init] Lock not acquired - another worker is initializing';
        RAISE NOTICE '[Init] Waiting 3 seconds for initialization to complete...';
        
        -- Wait for the other worker to finish
        -- 3 seconds is sufficient as initialization takes ~2-3 seconds
        PERFORM pg_sleep(3);
        
        RAISE NOTICE '[Init] Wait complete - proceeding';
    END IF;
    
    -- ================================================================
    -- STEP 2: Verify resources exist (safety check for all workers)
    -- ================================================================
    -- This ensures initialization was successful before any worker proceeds
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables 
        WHERE schemaname = '{output_schema}' 
          AND tablename = 'ways_segmented'
    ) THEN
        RAISE EXCEPTION '[Init] FATAL ERROR: ways_segmented table does not exist after initialization. Check logs for errors.';
    END IF;
    
    -- Verify critical functions exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc 
        WHERE proname = 'split_way_at_connection_points' 
          AND pronamespace = '{output_schema}'::regnamespace
    ) THEN
        RAISE EXCEPTION '[Init] FATAL ERROR: split_way_at_connection_points function does not exist after initialization. Check logs for errors.';
    END IF;
    
    -- ================================================================
    -- STEP 3: All clear - ready to proceed
    -- ================================================================
    RAISE NOTICE '[Init] ✓✓✓ Verification complete - ready for AGS processing ✓✓✓';
    
END $$;
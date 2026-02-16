-- ============================================================
-- 04_c_merge_ways_from_chains.sql
-- Merge ways_tem by chain_candidates
-- - For each chain (array of way_ids), merge all geometries
-- - Insert 1 new way row, delete old ones
-- - Record mapping in merged_ways (old_way_id_1, old_way_id_2, new_way_id)
-- - Update buildings.assigned_way_id via update_assigned_way_id
--
-- Assumptions:
-- - ways_tem columns: (id text, klasse, objektart, geom, ags, postcode)
-- - ways_tem.geom is LINESTRING
-- - chain_candidates exists and has: (chain_id, way_ids text[], way_count)
-- Note: Designed for parallel processing - each AGS in separate container
-- ============================================================


-- Mapping table for building reassignment (TEMP is fine)
DROP TABLE IF EXISTS merged_ways;
CREATE TEMP TABLE merged_ways (
    old_way_id_1 text NOT NULL,
    old_way_id_2 text NOT NULL,
    new_way_id   text NOT NULL
);
CREATE INDEX ON merged_ways (old_way_id_1);
CREATE INDEX ON merged_ways (old_way_id_2);
CREATE INDEX ON merged_ways (new_way_id);

DO $$
DECLARE
    r RECORD;
    v_new text;
    v_rows int;

    -- attributes chosen from one representative row (first id in chain)
    v_klasse text;
    v_objektart text;
    v_ags text;
    v_postcode integer;

    v_geom geometry;
    v_distinct_ways text[];
    v_snap_tol double precision := 0.25; -- slightly larger than detection tolerance (0.20)
BEGIN
    -- iterate chains
    FOR r IN
        SELECT chain_id, way_ids, way_count
        FROM chain_candidates
        ORDER BY way_count DESC, chain_id
    LOOP
        -- skip empty / trivial
        IF r.way_ids IS NULL OR array_length(r.way_ids, 1) IS NULL OR array_length(r.way_ids, 1) < 2 THEN
            CONTINUE;
        END IF;

        -- Get distinct way_ids to avoid processing duplicates
        SELECT array_agg(DISTINCT x) INTO v_distinct_ways
        FROM unnest(r.way_ids) AS x;

        -- Skip if only 1 distinct way (duplicates don't create a real chain)
        IF v_distinct_ways IS NULL OR array_length(v_distinct_ways, 1) < 2 THEN
            CONTINUE;
        END IF;

        -- Generate a new id for merged way
        v_new := md5(random()::text || clock_timestamp()::text);

        -- Pick attributes from the first way in the chain
        SELECT w.klasse, w.objektart, w.ags, w.postcode
        INTO v_klasse, v_objektart, v_ags, v_postcode
        FROM ways_tem w
        WHERE w.id::text = v_distinct_ways[1]
        LIMIT 1;

        -- Merge geometry for whole chain with snapping tolerance
        WITH geom_collection AS (
            SELECT w.geom
            FROM ways_tem w
            WHERE w.id::text = ANY(v_distinct_ways)
        ),
        all_geoms AS (
            SELECT ST_Collect(geom) AS ref_geom
            FROM geom_collection
        ),
        snapped_geoms AS (
            SELECT ST_Snap(g.geom, ag.ref_geom, v_snap_tol) AS geom
            FROM geom_collection g
            CROSS JOIN all_geoms ag
        )
        SELECT ST_LineMerge(ST_Union(geom))
        INTO v_geom
        FROM snapped_geoms;

        -- Validate merged geometry
        IF v_geom IS NULL OR ST_IsEmpty(v_geom) THEN
            RAISE NOTICE 'Merge produced null/empty geometry for chain_id=% (count=%). Skipping.', r.chain_id, r.way_count;
            CONTINUE;
        END IF;

        -- Insert merged way
        INSERT INTO ways_tem (id, klasse, objektart, geom, ags, postcode)
        VALUES (v_new, v_klasse, v_objektart, v_geom, v_ags, v_postcode);

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE NOTICE 'Merge insert failed for chain_id=% (count=%). Skipping.', r.chain_id, r.way_count;
            CONTINUE;
        END IF;

        -- Record mapping old -> new for ALL distinct ways in chain
        INSERT INTO merged_ways (old_way_id_1, old_way_id_2, new_way_id)
        SELECT
            x.old_id AS old_way_id_1,
            x.old_id AS old_way_id_2,  -- same value, so update_assigned_way_id can match either column
            v_new    AS new_way_id
        FROM unnest(v_distinct_ways) AS x(old_id);

        -- Delete old ways (using distinct ways)
        DELETE FROM ways_tem
        WHERE id::text = ANY(v_distinct_ways);

        -- NOTE: we intentionally inserted the new way first, then deleted old ones
        -- so that if something fails mid-loop, you still keep geometry in table.
    END LOOP;
END $$;
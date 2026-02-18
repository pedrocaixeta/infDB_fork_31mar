-- ============================================================
-- 04_a_build_merge_candidates.sql  (optimized)
-- Build temporary merge-candidates table for debugging
-- Assumes: ways_tem.geom is LINESTRING (not MULTILINESTRING)
-- Note: Designed for parallel processing - each AGS in separate container
--
-- Output: merge_candidates (same schema as before, just faster)
-- ============================================================
DROP TABLE IF EXISTS {output_schema}.merge_candidates;
DROP TABLE IF EXISTS {output_schema}.merge_candidates_debug;

-- ── 0) Tune this if needed (meters; EPSG:25832) ─────────────
DO $$ BEGIN
    PERFORM set_config('app.tol_m', '0.20', false);
END $$;


-- ── 1) Precompute endpoints once ─────────────────────────────
--    Indexing points instead of linestrings makes DWithin much cheaper.

DROP TABLE IF EXISTS way_endpoints;
CREATE TEMP TABLE way_endpoints AS
SELECT
    w.id::text        AS way_id,
    ST_StartPoint(w.geom) AS start_pt,
    ST_EndPoint(w.geom)   AS end_pt
FROM ways_tem w
WHERE w.geom IS NOT NULL
  AND GeometryType(w.geom) = 'LINESTRING'
  AND NOT ST_IsEmpty(w.geom);

CREATE INDEX way_endpoints_start_gix ON way_endpoints USING gist (start_pt);
CREATE INDEX way_endpoints_end_gix   ON way_endpoints USING gist (end_pt);
CREATE INDEX way_endpoints_way_id_ix ON way_endpoints (way_id);


-- ── 2) Create merge_candidates (same schema as before) ───────

DROP TABLE IF EXISTS merge_candidates;
CREATE TEMP TABLE merge_candidates (
    way_id text PRIMARY KEY,

    start_pt              geometry(Point),
    start_cnt             integer          NOT NULL,
    start_neighbor_ids    text[]           NOT NULL,
    start_nearest_id      text,
    start_nearest_dist_m  double precision,

    end_pt                geometry(Point),
    end_cnt               integer          NOT NULL,
    end_neighbor_ids      text[]           NOT NULL,
    end_nearest_id        text,
    end_nearest_dist_m    double precision,

    tol_m                 double precision NOT NULL,
    created_at            timestamptz DEFAULT now()
);

CREATE INDEX merge_candidates_start_pt_gix ON merge_candidates USING gist (start_pt);
CREATE INDEX merge_candidates_end_pt_gix   ON merge_candidates USING gist (end_pt);


-- ── 3) Fill merge_candidates ──────────────────────────────────
--
-- Strategy:
--   For each endpoint (start / end), do ONE set-based spatial join
--   against way_endpoints. Aggregate neighbours and nearest in a single pass.
--   LEFT JOIN so isolated ways (cnt=0) still get a row.

WITH
tol AS (
    SELECT current_setting('app.tol_m')::double precision AS m
),

-- ── 3a) Start-endpoint neighbours ────────────────────────────
start_agg AS (
    SELECT
        b.way_id,

        -- number of ways whose geometry comes within tol of this start point
        COUNT(n.way_id)                                                         AS start_cnt,

        -- all neighbour ids (same ordering as original: alphabetical)
        COALESCE(
            array_agg(n.way_id ORDER BY n.way_id),
            ARRAY[]::text[]
        )                                                                        AS start_neighbor_ids,

        -- nearest neighbour: first element when sorted by distance
        (array_agg(n.way_id
                   ORDER BY ST_Distance(n.start_pt, b.start_pt)   -- point-vs-point, fast
                            + ST_Distance(n.end_pt,   b.start_pt)  -- take whichever end is closer
        ))[1]                                                                    AS start_nearest_id,

        -- distance to nearest neighbour
        MIN(LEAST(
            ST_Distance(n.start_pt, b.start_pt),
            ST_Distance(n.end_pt,   b.start_pt)
        ))                                                                       AS start_nearest_dist_m

    FROM way_endpoints b
    -- spatial join: neighbours whose ANY endpoint is within tol
    LEFT JOIN way_endpoints n
           ON n.way_id <> b.way_id
          AND (   ST_DWithin(n.start_pt, b.start_pt, (SELECT m FROM tol))
               OR ST_DWithin(n.end_pt,   b.start_pt, (SELECT m FROM tol)))
    GROUP BY b.way_id
),

-- ── 3b) End-endpoint neighbours ──────────────────────────────
end_agg AS (
    SELECT
        b.way_id,

        COUNT(n.way_id)                                                         AS end_cnt,

        COALESCE(
            array_agg(n.way_id ORDER BY n.way_id),
            ARRAY[]::text[]
        )                                                                        AS end_neighbor_ids,

        (array_agg(n.way_id
                   ORDER BY ST_Distance(n.start_pt, b.end_pt)
                            + ST_Distance(n.end_pt,   b.end_pt)
        ))[1]                                                                    AS end_nearest_id,

        MIN(LEAST(
            ST_Distance(n.start_pt, b.end_pt),
            ST_Distance(n.end_pt,   b.end_pt)
        ))                                                                       AS end_nearest_dist_m

    FROM way_endpoints b
    LEFT JOIN way_endpoints n
           ON n.way_id <> b.way_id
          AND (   ST_DWithin(n.start_pt, b.end_pt, (SELECT m FROM tol))
               OR ST_DWithin(n.end_pt,   b.end_pt, (SELECT m FROM tol)))
    GROUP BY b.way_id
)

INSERT INTO merge_candidates (
    way_id,
    start_pt, start_cnt, start_neighbor_ids, start_nearest_id, start_nearest_dist_m,
    end_pt,   end_cnt,   end_neighbor_ids,   end_nearest_id,   end_nearest_dist_m,
    tol_m
)
SELECT
    b.way_id,

    b.start_pt,
    COALESCE(sa.start_cnt, 0)::integer,
    COALESCE(sa.start_neighbor_ids, ARRAY[]::text[]),
    sa.start_nearest_id,
    sa.start_nearest_dist_m,

    b.end_pt,
    COALESCE(ea.end_cnt, 0)::integer,
    COALESCE(ea.end_neighbor_ids, ARRAY[]::text[]),
    ea.end_nearest_id,
    ea.end_nearest_dist_m,

    (SELECT m FROM tol)
FROM way_endpoints b
LEFT JOIN start_agg sa ON sa.way_id = b.way_id
LEFT JOIN end_agg   ea ON ea.way_id = b.way_id;


-- ── 4) Cleanup helper table ───────────────────────────────────
DROP TABLE IF EXISTS way_endpoints;
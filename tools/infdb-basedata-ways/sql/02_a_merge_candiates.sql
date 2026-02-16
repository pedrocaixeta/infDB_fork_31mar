-- ============================================================
-- 04_a_build_merge_candidates.sql
-- Build temporary merge-candidates table for debugging
-- Assumes: ways_tem.geom is LINESTRING (not MULTILINESTRING)
-- Note: Designed for parallel processing - each AGS in separate container
-- ============================================================


-- 1) Create temporary candidates table
DROP TABLE IF EXISTS merge_candidates;
CREATE TEMP TABLE merge_candidates (
    way_id text PRIMARY KEY,

    start_pt geometry(Point),
    start_cnt integer NOT NULL,
    start_neighbor_ids text[] NOT NULL,
    start_nearest_id text,
    start_nearest_dist_m double precision,

    end_pt geometry(Point),
    end_cnt integer NOT NULL,
    end_neighbor_ids text[] NOT NULL,
    end_nearest_id text,
    end_nearest_dist_m double precision,

    tol_m double precision NOT NULL,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS merge_candidates_start_pt_gix
ON merge_candidates USING gist (start_pt);

CREATE INDEX IF NOT EXISTS merge_candidates_end_pt_gix
ON merge_candidates USING gist (end_pt);

-- 2) Fill from current snapshot of ways_tem
WITH params AS (
    SELECT 0.20::double precision AS tol_m  -- <-- tune if needed (meters; EPSG:25832)
),
base AS (
    SELECT
        w.id::text AS way_id,
        ST_StartPoint(w.geom) AS start_pt,
        ST_EndPoint(w.geom)   AS end_pt
    FROM ways_tem w
    WHERE w.geom IS NOT NULL
      AND GeometryType(w.geom) = 'LINESTRING'
      AND NOT ST_IsEmpty(w.geom)
),
start_stats AS (
    SELECT
        b.way_id,

        (SELECT count(*)
         FROM ways_tem w2, params p
         WHERE w2.geom IS NOT NULL
           AND w2.id::text <> b.way_id
           AND ST_DWithin(w2.geom, b.start_pt, p.tol_m)
        ) AS start_cnt,

        COALESCE(
          (SELECT array_agg(w2.id::text ORDER BY w2.id::text)
           FROM ways_tem w2, params p
           WHERE w2.geom IS NOT NULL
             AND w2.id::text <> b.way_id
             AND ST_DWithin(w2.geom, b.start_pt, p.tol_m)
          ),
          ARRAY[]::text[]
        ) AS start_neighbor_ids,

        (SELECT w2.id::text
         FROM ways_tem w2
         WHERE w2.geom IS NOT NULL
           AND w2.id::text <> b.way_id
         ORDER BY ST_Distance(w2.geom, b.start_pt) ASC
         LIMIT 1
        ) AS start_nearest_id,

        (SELECT ST_Distance(w2.geom, b.start_pt)
         FROM ways_tem w2
         WHERE w2.geom IS NOT NULL
           AND w2.id::text <> b.way_id
         ORDER BY ST_Distance(w2.geom, b.start_pt) ASC
         LIMIT 1
        ) AS start_nearest_dist_m

    FROM base b
),
end_stats AS (
    SELECT
        b.way_id,

        (SELECT count(*)
         FROM ways_tem w2, params p
         WHERE w2.geom IS NOT NULL
           AND w2.id::text <> b.way_id
           AND ST_DWithin(w2.geom, b.end_pt, p.tol_m)
        ) AS end_cnt,

        COALESCE(
          (SELECT array_agg(w2.id::text ORDER BY w2.id::text)
           FROM ways_tem w2, params p
           WHERE w2.geom IS NOT NULL
             AND w2.id::text <> b.way_id
             AND ST_DWithin(w2.geom, b.end_pt, p.tol_m)
          ),
          ARRAY[]::text[]
        ) AS end_neighbor_ids,

        (SELECT w2.id::text
         FROM ways_tem w2
         WHERE w2.geom IS NOT NULL
           AND w2.id::text <> b.way_id
         ORDER BY ST_Distance(w2.geom, b.end_pt) ASC
         LIMIT 1
        ) AS end_nearest_id,

        (SELECT ST_Distance(w2.geom, b.end_pt)
         FROM ways_tem w2
         WHERE w2.geom IS NOT NULL
           AND w2.id::text <> b.way_id
         ORDER BY ST_Distance(w2.geom, b.end_pt) ASC
         LIMIT 1
        ) AS end_nearest_dist_m

    FROM base b
)
INSERT INTO merge_candidates (
    way_id,
    start_pt, start_cnt, start_neighbor_ids, start_nearest_id, start_nearest_dist_m,
    end_pt, end_cnt, end_neighbor_ids, end_nearest_id, end_nearest_dist_m,
    tol_m
)
SELECT
    b.way_id,

    b.start_pt,
    ss.start_cnt,
    ss.start_neighbor_ids,
    ss.start_nearest_id,
    ss.start_nearest_dist_m,

    b.end_pt,
    es.end_cnt,
    es.end_neighbor_ids,
    es.end_nearest_id,
    es.end_nearest_dist_m,

    (SELECT tol_m FROM params)
FROM base b
JOIN start_stats ss ON ss.way_id = b.way_id
JOIN end_stats   es ON es.way_id = b.way_id;
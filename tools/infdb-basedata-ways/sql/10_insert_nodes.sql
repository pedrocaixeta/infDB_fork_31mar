-- ============================================================
-- 10_build_nodes.sql
-- Build nodes at connection points between ways in ways_tem
-- A node is created wherever 2+ way endpoints meet (within tol)
-- Inserts into {output_schema}.nodes (ags, id, geom, way_ids)
-- Designed for parallel processing - each AGS in separate container
-- ============================================================

-- ── Clean the table ──────────────────────────────────────────
DELETE FROM {output_schema}.nodes
WHERE ags = '{ags}';

-- ── Insert nodes ──────────────────────────────────────────────
WITH params AS (
    SELECT 0.20::double precision AS tol_m
),

-- ── 1) Precompute endpoints ───────────────────────────────────
endpoints AS (
    SELECT
        w.id::text            AS way_id,
        w.ags                 AS ags,
        ST_StartPoint(w.geom) AS pt
    FROM ways_tem w
    WHERE w.geom IS NOT NULL
      AND GeometryType(w.geom) = 'LINESTRING'
      AND NOT ST_IsEmpty(w.geom)
      AND w.klasse <> 'connection_line' -- exclude connection lines to avoid creating nodes at their endpoints

    UNION ALL

    SELECT
        w.id::text           AS way_id,
        w.ags                AS ags,
        ST_EndPoint(w.geom)  AS pt
    FROM ways_tem w
    WHERE w.geom IS NOT NULL
      AND GeometryType(w.geom) = 'LINESTRING'
      AND NOT ST_IsEmpty(w.geom)
      AND w.klasse <> 'connection_line' -- exclude connection lines to avoid creating nodes at their endpoints
),

-- ── 2) Cluster endpoints within tol of each other ────────────
clustered AS (
    SELECT
        way_id,
        ags,
        pt,
        ST_ClusterDBSCAN(pt, (SELECT tol_m FROM params), 1)
            OVER () AS cluster_id
    FROM endpoints
),

-- ── 3) Aggregate: keep only clusters where 2+ distinct ways meet
cluster_stats AS (
    SELECT
        cluster_id,
        array_agg(DISTINCT way_id ORDER BY way_id) AS way_ids,
        MIN(ags)                                    AS ags,
        ST_Centroid(ST_Collect(pt))                 AS node_pt
    FROM clustered
    WHERE cluster_id IS NOT NULL
    GROUP BY cluster_id
    HAVING COUNT(DISTINCT way_id) >= 2
)

-- ── 4) Insert into nodes ──────────────────────────────────────
INSERT INTO {output_schema}.nodes (ags, id, geom, way_ids)
SELECT
    ags,
    md5(node_pt::text || cluster_id::text) AS id,
    node_pt                                AS geom,
    way_ids
FROM cluster_stats;
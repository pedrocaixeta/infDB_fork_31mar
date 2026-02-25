-- ============================================================
-- Build nodes at connection points between ways in ways_tem
--
-- Notes:
-- - A node is created wherever 2+ way endpoints meet within a tolerance
-- - Deletes existing nodes for the target `{ags}` and reinserts computed nodes
-- - Computes endpoints (start/end) for LINESTRING ways in ways_tem
-- - Clusters endpoints using ST_ClusterDBSCAN with a fixed tolerance
-- - Aggregates clusters into node geometries and associated way id arrays
-- - Inserts results into {output_schema}.nodes (ags, id, geom, way_ids)
-- ============================================================

-- Clean existing nodes for this AGS scope
DELETE FROM {output_schema}.nodes
WHERE ags = '{ags}'; -- restrict delete to current AGS

-- Insert nodes
WITH params AS (
    SELECT 0.20::double precision AS tol_m -- clustering tolerance (units depend on SRID)
),

-- Precompute endpoints (start and end points for each way)
endpoints AS (
    SELECT
        w.id::text            AS way_id, -- way id as text
        w.ags                 AS ags,    -- municipality/region id (AGS) as text
        ST_StartPoint(w.geom) AS pt      -- start endpoint point
    FROM ways_tem w
    WHERE w.geom IS NOT NULL
      AND GeometryType(w.geom) = 'LINESTRING' -- require LINESTRING
      AND NOT ST_IsEmpty(w.geom)

    UNION ALL

    SELECT
        w.id::text           AS way_id, -- way id as text
        w.ags                AS ags,    -- municipality/region id (AGS) as text
        ST_EndPoint(w.geom)  AS pt      -- end endpoint point
    FROM ways_tem w
    WHERE w.geom IS NOT NULL
      AND GeometryType(w.geom) = 'LINESTRING' -- require LINESTRING
      AND NOT ST_IsEmpty(w.geom)
),

-- Cluster endpoints that lie within tol_m of each other
clustered AS (
    SELECT
        way_id, -- way id
        ags,    -- AGS tag
        pt,     -- endpoint geometry
        ST_ClusterDBSCAN(pt, (SELECT tol_m FROM params), 1)
            OVER () AS cluster_id -- cluster id across all endpoints
    FROM endpoints
),

-- Aggregate clusters into node points and associated way ids
cluster_stats AS (
    SELECT
        cluster_id,                                  -- cluster identifier
        array_agg(DISTINCT way_id ORDER BY way_id) AS way_ids, -- distinct way ids in this cluster
        MIN(ags)                                    AS ags,    -- AGS tag for the node
        ST_Centroid(ST_Collect(pt))                 AS node_pt -- representative node point
    FROM clustered
    WHERE cluster_id IS NOT NULL                    -- ignore unclustered points
    GROUP BY cluster_id
    HAVING COUNT(DISTINCT way_id) >= 1              -- cluster size threshold (distinct ways)
)

-- Insert aggregated clusters as nodes
INSERT INTO {output_schema}.nodes (ags, id, geom, way_ids)
SELECT
    ags,                                     -- municipality/region id (AGS) as text
    md5(node_pt::text || cluster_id::text) AS id, -- deterministic-ish node id from geometry+cluster
    node_pt                                AS geom, -- node point geometry
    way_ids                                AS way_ids -- associated way ids
FROM cluster_stats;
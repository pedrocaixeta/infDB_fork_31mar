-- ============================================================
-- 04_b_build_chain_candidates.sql
-- Builds connected components ("chains") based on merge_candidates
-- Output tables (temporary):
--   - chain_edges
--   - chain_candidates
-- Note: Designed for parallel processing - each AGS in separate container
-- ============================================================

-- 1) Edges from merge_candidates (only cnt=1 endpoints)
DROP TABLE IF EXISTS chain_edges;
CREATE TEMP TABLE chain_edges AS
WITH raw_edges AS (
    -- start endpoint link
    SELECT
        mc.way_id AS a,
        mc.start_nearest_id AS b
    FROM merge_candidates mc
    WHERE mc.start_cnt = 1
      AND mc.start_nearest_id IS NOT NULL

    UNION ALL

    -- end endpoint link
    SELECT
        mc.way_id AS a,
        mc.end_nearest_id AS b
    FROM merge_candidates mc
    WHERE mc.end_cnt = 1
      AND mc.end_nearest_id IS NOT NULL
),
-- make edges undirected + dedup (store as (u,v) with u < v)
norm AS (
    SELECT
        LEAST(a,b) AS u,
        GREATEST(a,b) AS v
    FROM raw_edges
    WHERE a <> b
)
SELECT DISTINCT u, v
FROM norm;

CREATE INDEX IF NOT EXISTS chain_edges_u_idx
ON chain_edges (u);

CREATE INDEX IF NOT EXISTS chain_edges_v_idx
ON chain_edges (v);

-- 2) Connected components via recursive expansion
-- This assigns each node to a "chain_root" (smallest id in its component)
DROP TABLE IF EXISTS chain_membership;
CREATE TEMP TABLE chain_membership AS
WITH RECURSIVE
nodes AS (
    SELECT u AS node FROM chain_edges
    UNION
    SELECT v AS node FROM chain_edges
),
seed AS (
    SELECT node AS chain_root, node
    FROM nodes
),
walk AS (
    -- start from each node as root
    SELECT chain_root, node
    FROM seed

    UNION

    -- expand along edges
    SELECT
        w.chain_root,
        CASE
            WHEN e.u = w.node THEN e.v
            ELSE e.u
        END AS node
    FROM walk w
    JOIN chain_edges e
      ON (e.u = w.node OR e.v = w.node)
),
-- for each node, choose the smallest reachable root as its component id
comp AS (
    SELECT
        node,
        MIN(chain_root) AS component_id
    FROM walk
    GROUP BY node
)
SELECT * FROM comp;

CREATE INDEX IF NOT EXISTS chain_membership_component_idx
ON chain_membership (component_id);

CREATE INDEX IF NOT EXISTS chain_membership_node_idx
ON chain_membership (node);

-- 3) Final chain_candidates table: one row per component with array of way ids
DROP TABLE IF EXISTS chain_candidates;
CREATE TEMP TABLE chain_candidates AS
SELECT
    component_id AS chain_id,
    array_agg(node ORDER BY node) AS way_ids,
    count(*) AS way_count
FROM chain_membership
GROUP BY component_id
HAVING count(*) >= 2;

CREATE INDEX IF NOT EXISTS chain_candidates_way_count_idx
ON chain_candidates (way_count);
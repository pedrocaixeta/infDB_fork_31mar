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
    SELECT
        mc.way_id          AS a,
        mc.start_nearest_id AS b
    FROM merge_candidates mc
    WHERE mc.start_cnt = 1
      AND mc.start_nearest_id IS NOT NULL

    UNION ALL

    SELECT
        mc.way_id        AS a,
        mc.end_nearest_id AS b
    FROM merge_candidates mc
    WHERE mc.end_cnt = 1
      AND mc.end_nearest_id IS NOT NULL
),
norm AS (
    SELECT
        LEAST(a, b)    AS u,
        GREATEST(a, b) AS v
    FROM raw_edges
    WHERE a <> b
)
SELECT DISTINCT u, v
FROM norm;

CREATE INDEX IF NOT EXISTS chain_edges_u_idx ON chain_edges (u);
CREATE INDEX IF NOT EXISTS chain_edges_v_idx ON chain_edges (v);


-- 2) Connected components via union-find (replaces recursive CTE)
--    Much faster than recursive expansion which was O(n^2) in node count.

DROP TABLE IF EXISTS chain_membership;

CREATE OR REPLACE FUNCTION _build_chain_membership()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    r       RECORD;
    root_a  text;
    root_b  text;
    tmp     text;
    v_rows  integer;  -- was: changed boolean
BEGIN
    -- ── a) Initialize: every node is its own parent ───────────
    CREATE TEMP TABLE _uf_parent (
        node   text PRIMARY KEY,
        parent text NOT NULL
    ) ON COMMIT DROP;

    INSERT INTO _uf_parent (node, parent)
    SELECT DISTINCT node, node
    FROM (
        SELECT u AS node FROM chain_edges
        UNION
        SELECT v         FROM chain_edges
    ) x;

    -- ── b) Union pass: iterate edges, merge components ────────
    FOR r IN SELECT u, v FROM chain_edges LOOP

        -- find root of u (path traversal)
        root_a := r.u;
        LOOP
            SELECT parent INTO tmp FROM _uf_parent WHERE node = root_a;
            EXIT WHEN tmp = root_a;
            root_a := tmp;
        END LOOP;

        -- find root of v (path traversal)
        root_b := r.v;
        LOOP
            SELECT parent INTO tmp FROM _uf_parent WHERE node = root_b;
            EXIT WHEN tmp = root_b;
            root_b := tmp;
        END LOOP;

        -- union: smaller text id becomes root (deterministic, matches original MIN logic)
        IF root_a <> root_b THEN
            IF root_a < root_b THEN
                UPDATE _uf_parent SET parent = root_a WHERE node = root_b;
            ELSE
                UPDATE _uf_parent SET parent = root_b WHERE node = root_a;
            END IF;
        END IF;

    END LOOP;

    -- ── c) Flatten: iteratively point every node to its true root ──
    --    Repeat until no more updates needed (handles deep chains)
    LOOP
        UPDATE _uf_parent p
        SET    parent = g.parent
        FROM   _uf_parent g
        WHERE  p.parent = g.node
          AND  g.parent <> g.node;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        EXIT WHEN v_rows = 0;
    END LOOP;

    -- ── d) Materialize into chain_membership ──────────────────
    CREATE TEMP TABLE chain_membership AS
    SELECT
        node,
        parent AS component_id
    FROM _uf_parent;

    -- _uf_parent dropped automatically at end of transaction (ON COMMIT DROP)
END $$;

SELECT _build_chain_membership();
DROP FUNCTION IF EXISTS _build_chain_membership();

CREATE INDEX IF NOT EXISTS chain_membership_component_idx ON chain_membership (component_id);
CREATE INDEX IF NOT EXISTS chain_membership_node_idx      ON chain_membership (node);


-- 3) Final chain_candidates: one row per component with array of way_ids
DROP TABLE IF EXISTS chain_candidates;
CREATE TEMP TABLE chain_candidates AS
SELECT
    component_id                          AS chain_id,
    array_agg(node ORDER BY node)         AS way_ids,
    count(*)                              AS way_count
FROM chain_membership
GROUP BY component_id
HAVING count(*) >= 2;

CREATE INDEX IF NOT EXISTS chain_candidates_way_count_idx ON chain_candidates (way_count);
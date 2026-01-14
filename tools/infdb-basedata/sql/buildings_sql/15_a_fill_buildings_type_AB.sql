-- Requires pgrouting:
-- CREATE EXTENSION pgrouting CASCADE;

-- fill building_type
-- Step 1: Apartment Buildings (AB):
-- Step 1a: identify buildings which are typically apartment buildings based on their own attributes
-- Typically have <4+ floors and many neighbors> or <3+ floors and 3+ neighbors> or <floor area > 1500>
UPDATE {output_schema}.buildings b
SET building_type = 'AB'
WHERE b.building_use = 'Residential'
  AND b.building_type IS NULL
  AND (
        b.floor_number >= 4
     OR b.floor_area > 1500
     OR (
          b.floor_number >= 3
          AND EXISTS (
                SELECT 1
                FROM temp_touching_neighbor_counts tnc
                WHERE tnc.id = b.id
                  AND tnc.count >= 3
          )
        )
      )
;

-- Reference implementation with semi-procedural approach for graph based solution below.
-- -- Buildings adjacent to AB with 3+ floors and similar height become AB
-- DO
-- $$
--     DECLARE
--         updated_count INTEGER := 1;
--     BEGIN
--         WHILE updated_count > 0
--             LOOP
--                 WITH candidates AS (SELECT DISTINCT n.a_id
--                                     FROM temp_touching_neighbors n
--                                              JOIN {output_schema}.buildings nb ON n.b_id = nb.id
--                                              JOIN {output_schema}.buildings b1 ON n.a_id = b1.id
--                                     WHERE nb.building_type = 'AB'
--                                       --AND b1.floor_number >= 3
--                                       --AND ABS(b1.height - nb.height)/GREATEST(b1.height, nb.height) < 0.2
--                                       AND b1.building_use = 'Residential'
--                                     -- AND nb.gemeindeschluessel IN ({list_gemeindeschluessel})
--                                     -- AND b1.gemeindeschluessel IN ({list_gemeindeschluessel})
--                                       AND b1.building_type IS NULL
--                                     )
--                 UPDATE {output_schema}.buildings b
--                 SET building_type = 'AB'
--                 FROM candidates
--                 WHERE b.id = candidates.a_id;
--
--                 GET DIAGNOSTICS updated_count = ROW_COUNT;
--                 -- RAISE NOTICE 'Rule 1 iteration: % buildings updated', updated_count;
--             END LOOP;
--     END
-- $$;

-- Propagate building_type 'AB' to neighbours which have no building_type yet
-- Step 1b: build graph of buildings which are neighbours and fulfilling 'AB'-neighbour criteria

-- Create Vertex set of buildings which could be of type 'AB'
DROP TABLE IF EXISTS filtered_buildings;
CREATE TEMP TABLE filtered_buildings AS (
  SELECT id, geom, height, gemeindeschluessel
  FROM {output_schema}.buildings
  WHERE building_use = 'Residential'
    AND (building_type IS NULL OR building_type = 'AB')
  --AND b1.floor_number >= 3
)
;

CREATE INDEX IF NOT EXISTS idx_filtered_buildings_geom ON filtered_buildings USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_filtered_buildings_id ON filtered_buildings(id);
CREATE INDEX IF NOT EXISTS idx_filtered_buildings_ags ON filtered_buildings(gemeindeschluessel);

-- edges in graph show neighbourhood relation
DROP TABLE IF EXISTS building_edges;
CREATE TEMP TABLE building_edges(
    id SERIAL PRIMARY KEY,
    source INT,
    target INT,
    cost DOUBLE PRECISION,
    reverse_cost DOUBLE PRECISION
);
INSERT INTO building_edges(source, target, cost, reverse_cost)
SELECT  b1.id AS source,
        b2.id AS target,
        1.0   AS cost, -- undirected graph, therefore both direction are 1.0
        1.0   AS reverse_cost
FROM filtered_buildings b1
JOIN LATERAL (
    SELECT id
    FROM filtered_buildings b2
    WHERE b1.id < b2.id
      AND b1.gemeindeschluessel = b2.gemeindeschluessel
      AND b2.geom && ST_Expand(b1.geom, 0.01)
      AND ST_DWithin(b1.geom, b2.geom, 0.01)
-- AND ABS(b1.height - b2.height) / GREATEST(b1.height, b2.height) < 0.2
) b2 ON true;
-- JOIN filtered_buildings b2
--     ON b1.id < b2.id -- avoid double edges and self-joins in undirected graph
--     AND b1.gemeindeschluessel = b2.gemeindeschluessel
--     AND b1.geom && ST_Expand(b2.geom, 0.01) -- prefilter
--     AND ST_DWithin(b1.geom, b2.geom, 0.01)
-- AND ABS(b1.height - b2.height) / GREATEST(b1.height, b2.height) < 0.2
;

CREATE INDEX IF NOT EXISTS building_edges_source_idx ON building_edges(source);
CREATE INDEX IF NOT EXISTS building_edges_target_idx ON building_edges(target);

-- Step 2: find connected components in graph which build a neighbourhood cluster
DROP TABLE IF EXISTS building_components;
CREATE TEMP TABLE IF NOT EXISTS building_components AS
SELECT component,
       node AS id
FROM pgr_connectedComponents(
  'SELECT id, source, target, cost, reverse_cost FROM building_edges'::text
);

CREATE INDEX IF NOT EXISTS building_components_id_idx
    ON building_components(id);
CREATE INDEX IF NOT EXISTS building_components_component_idx
    ON building_components(component);

-- find all neighbourhood components which already have building_type 'AB' from 1a and mark them as 'AB' too.
WITH seed_components AS (
  SELECT DISTINCT bc.component
  FROM building_components bc
  JOIN {output_schema}.buildings b
    ON b.id = bc.id
  WHERE b.building_type = 'AB'
)
UPDATE {output_schema}.buildings b
SET building_type = 'AB'
FROM building_components bc
JOIN seed_components sc
  ON sc.component = bc.component
WHERE b.id = bc.id;

-- release memory
DROP TABLE IF EXISTS filtered_buildings;
DROP TABLE IF EXISTS building_edges;
DROP TABLE IF EXISTS building_components;

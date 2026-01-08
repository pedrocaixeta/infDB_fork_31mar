-- Step 4: Multi-Family Houses (MFH):
-- Buildings with 2-3 floors, multiple units but smaller than apartment buildings
-- Often have some neighbors but not as many as apartment buildings
UPDATE {output_schema}.buildings
SET building_type = 'MFH'
WHERE building_use = 'Residential'
  AND building_type IS NULL
  AND ((floor_number BETWEEN 2 AND 3 OR
        (floor_area > 150 AND
         EXISTS (SELECT 1
                 FROM temp_touching_neighbor_counts
                 WHERE temp_touching_neighbor_counts.id = {output_schema}.buildings.id
                   AND count BETWEEN 1 AND 3))
    ));
-- Reference implementation with semi-procedural approach for graph based solution below.
-- -- Buildings with 2-3 floors adjacent to MFH likely also MFH
-- DO
-- $$
--     DECLARE
--         updated_count INTEGER := 1;
--     BEGIN
--         WHILE updated_count > 0
--             LOOP
--                 WITH candidates AS (SELECT DISTINCT n.a_id
--                                     FROM temp_touching_neighbors n
--                                              JOIN {output_schema}.buildings b1 ON n.a_id = b1.id
--                                              JOIN {output_schema}.buildings b2 ON n.b_id = b2.id
--                                     WHERE b2.building_type = 'MFH'
--                                       --AND b1.floor_number BETWEEN 2 AND 3
--                                       AND b1.building_use = 'Residential'
--                                       AND b1.building_type IS NULL
--                                       AND b1.gemeindeschluessel = b2.gemeindeschluessel
--                                     )
--                 UPDATE {output_schema}.buildings b
--                 SET building_type = 'MFH'
--                 FROM candidates
--                 WHERE b.id = candidates.a_id;
--
--                 GET DIAGNOSTICS updated_count = ROW_COUNT;
--                 -- RAISE NOTICE 'Rule 5 iteration: % buildings updated', updated_count;
--             END LOOP;
--     END
-- $$;


-- Create Vertex set of buildings which could be of type 'MFH'
CREATE TEMP TABLE filtered_buildings AS (
    SELECT id, geom, height, gemeindeschluessel
    FROM {output_schema}.buildings
    WHERE building_use = 'Residential'
    AND (building_type = 'MFH'
        OR( building_type IS NULL
    --AND floor_number BETWEEN 2 AND 3
        )
    )
)
;

CREATE INDEX IF NOT EXISTS idx_filtered_buildings_geom ON filtered_buildings USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_filtered_buildings_id ON filtered_buildings(id);
CREATE INDEX IF NOT EXISTS idx_filtered_buildings_ags ON filtered_buildings(gemeindeschluessel);

-- edges in graph show neighbourhood relation
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
) b2 ON true;

CREATE INDEX IF NOT EXISTS building_edges_source_idx ON building_edges(source);
CREATE INDEX IF NOT EXISTS building_edges_target_idx ON building_edges(target);

-- Step 2: find connected components in graph which build a neighbourhood cluster
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

-- find all neighbourhood components which already have building_type 'MFH' from 1a and mark them as 'MFH' too.
WITH seed_components AS (
  SELECT DISTINCT bc.component
  FROM building_components bc
  JOIN {output_schema}.buildings b
    ON b.id = bc.id
  WHERE b.building_type = 'MFH'
)
UPDATE {output_schema}.buildings b
SET building_type = 'MFH'
FROM building_components bc
JOIN seed_components sc
  ON sc.component = bc.component
WHERE b.id = bc.id;

-- release memory
DROP TABLE IF EXISTS filtered_buildings;
DROP TABLE IF EXISTS building_edges;
DROP TABLE IF EXISTS building_components;
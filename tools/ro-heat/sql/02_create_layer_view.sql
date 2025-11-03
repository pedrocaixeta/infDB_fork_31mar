-- Create view with joined element data
DROP VIEW IF EXISTS v_element_layer_data;

CREATE VIEW v_element_layer_data AS
WITH element_vars AS (SELECT 'OuterWall'::text  AS element_name,
                             'outer_wall'::text AS year_col,
                             'wall_area'::text  AS area_col
                      UNION ALL
                      SELECT 'Rooftop', 'rooftop', 'roof_area'
                      UNION ALL
                      SELECT 'Window', 'window', 'window_area'
                      UNION ALL
                      SELECT 'GroundFloor', 'construction_year', 'floor_area'
                      UNION ALL
                      SELECT 'Ceiling', 'construction_year', 'floor_area'
                      UNION ALL
                      SELECT 'Floor', 'construction_year', 'floor_area'),
-- Precompute only rows that have nonzero area
     nonzero_elements AS (SELECT b.building_objectid,
                                 v.element_name,
                                 (to_jsonb(b) ->> v.year_col)::int     AS year_val,
                                 (to_jsonb(b) ->> v.area_col)::numeric AS base_area,
                                 b.floor_number,
                                 b.construction_year,
                                 b.building_type
                          FROM element_vars v
                                   JOIN ro_heat.buildings_rc b ON TRUE
                          WHERE (
                                    CASE
                                        -- Calculate Ceiling and Floor area as floor_area * max((b.floor_number - 1), 0)
                                        WHEN v.element_name IN ('Ceiling', 'Floor')
                                            THEN ((to_jsonb(b) ->> v.area_col)::numeric *
                                                  GREATEST(b.floor_number - 1, 0))
                                        -- Use area_col for all other constructions
                                        ELSE (to_jsonb(b) ->> v.area_col)::numeric
                                        END
                                    ) > 0)
SELECT n.building_objectid,
       n.element_name,
       CASE
           WHEN n.element_name IN ('Ceiling', 'Floor')
               THEN (n.base_area * GREATEST(n.floor_number - 1, 0))
           ELSE n.base_area
           END AS area,
       l.thickness,
       m.name,
       m.density,
       m.thermal_conduc,
       m.heat_capac,
       l.layer_index
FROM nonzero_elements n
         JOIN opendata.tabula_type_elements t
              ON t.element_name = n.element_name
         JOIN LATERAL (
    SELECT CASE
               -- Lookup 'Ceiling' and 'Floor' in 'tabula_de_standard'
               WHEN n.element_name IN ('Ceiling', 'Floor') THEN 'tabula_de_standard'
               -- Lookup unrefurbished constructions in 'tabula_de_standard_1_' || n.building_type
               WHEN n.year_val = n.construction_year
                   THEN 'tabula_de_standard_1_' || n.building_type
               -- Lookup refurbished constructions in 'tabula_de_retrofit_1_' || n.building_type
               ELSE 'tabula_de_retrofit_1_' || n.building_type
               END AS construction_match
    ) vals ON TRUE
         JOIN opendata.tabula_layers l
              ON t.element_id = l.element_id
         JOIN opendata.tabula_materials m
              ON l.material_id = m.material_id
WHERE n.year_val BETWEEN t.start_year AND t.end_year
  AND t.construction_data = vals.construction_match;

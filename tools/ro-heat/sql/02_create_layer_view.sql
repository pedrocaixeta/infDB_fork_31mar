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
                      SELECT 'GroundFloor', 'construction_year', 'floor_area')
SELECT b.building_id,
       v.element_name,
       vals.area_val::numeric AS area,
       l.thickness,
       m.name,
       m.density,
       m.thermal_conduc,
       m.heat_capac,
       l.layer_index
FROM element_vars v
         JOIN opendata.tabula_type_elements t
              ON t.element_name = v.element_name
         JOIN ro_heat.buildings_rc b ON TRUE
         JOIN LATERAL (
    SELECT (to_jsonb(b) ->> v.year_col)::int     AS year_val,
           (to_jsonb(b) ->> v.area_col)::numeric AS area_val,
           CASE
               -- If the construction year matches year_col then lookup data in tabula_de_standard,
               -- else lookup data in tabula_de_retrofit
               WHEN (to_jsonb(b) ->> v.year_col)::int = b.construction_year::int
                   THEN 'tabula_de_standard_1_' || b.building_type
               ELSE 'tabula_de_retrofit_1_' || b.building_type
               END                               AS construction_match
    ) vals ON TRUE
         JOIN opendata.tabula_layers l
              ON t.element_id = l.element_id
         JOIN opendata.tabula_materials m
              ON l.material_id = m.material_id
WHERE vals.year_val BETWEEN t.start_year AND t.end_year
  AND t.construction_data = vals.construction_match;
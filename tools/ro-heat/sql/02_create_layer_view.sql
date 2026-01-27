DROP VIEW IF EXISTS v_element_layer_data;

CREATE VIEW v_element_layer_data AS
WITH element_defs AS (SELECT *
                      FROM (VALUES ('OuterWall', 'outer_wall', 'wall_area'),
                                   ('Rooftop', 'rooftop', 'roof_area'),
                                   ('Window', 'window', 'window_area'),
                                   ('GroundFloor', 'construction_year', 'floor_area'),
                                   ('Ceiling', 'construction_year', 'floor_area'),
                                   ('Floor', 'construction_year', 'floor_area'),
                                   ('InnerWall', 'construction_year', 'floor_area'))
                               AS v(element_name, year_col, area_col)),

-- Extract required building data
     building_data AS (SELECT b.building_objectid,
                              b.floor_number,
                              b.construction_year,
                              b.building_type,
                              to_jsonb(b) AS bjson
                       FROM ro_heat.buildings_rc b),

-- Expand buildings × elements and compute area once
     element_areas AS (SELECT b.building_objectid,
                              e.element_name,
                              b.floor_number,
                              b.construction_year,
                              b.building_type,

                              NULLIF(b.bjson ->> e.year_col, '')::int     AS year_val,
                              NULLIF(b.bjson ->> e.area_col, '')::numeric AS base_area,

                              CASE
                                  -- Calculate 'Ceiling' and 'Floor' area as floor_area * (floor_number - 1)
                                  WHEN e.element_name IN ('Ceiling', 'Floor')
                                      THEN NULLIF(b.bjson ->> e.area_col, '')::numeric
                                      * GREATEST(b.floor_number - 1, 0)
                                  -- Calculate 'InnerWall' area as floor_number * 2.5
                                  WHEN e.element_name = 'InnerWall'
                                      THEN NULLIF(b.bjson ->> e.area_col, '')::numeric
                                      * GREATEST(b.floor_number, 0) * 2.5

                                  ELSE
                                      NULLIF(b.bjson ->> e.area_col, '')::numeric
                                  END                                     AS area
                       FROM building_data b
                                CROSS JOIN element_defs e),

-- Keep only valid, nonzero elements
     valid_elements AS (SELECT *
                        FROM element_areas
                        WHERE area > 0
                          AND year_val IS NOT NULL),

-- Decide which construction dataset applies, either standard or retrofit
     element_construction AS (SELECT v.*,
                                     CASE
                                         -- Lookup 'Ceiling' and 'Floor' in 'tabula_de_standard'
                                         WHEN v.element_name IN ('Ceiling', 'Floor', 'InnerWall')
                                             THEN 'tabula_de_standard'
                                         -- Lookup unrefurbished in 'tabula_de_standard_1_type'
                                         WHEN v.year_val = v.construction_year
                                             THEN 'tabula_de_standard_1_' || v.building_type
                                         -- Lookup refurbished in 'tabula_de_retrofit_1_type'
                                         ELSE
                                             'tabula_de_retrofit_1_' || v.building_type
                                         END AS construction_match
                              FROM valid_elements v),
     -- All available TABULA constructions with ranking per building type
     tabula_ranked AS (SELECT t.*,
                              ROW_NUMBER() OVER (
                                  PARTITION BY
                                      t.element_name,
                                      t.construction_data
                                  ORDER BY t.end_year DESC
                                  ) AS rn_latest
                       FROM opendata.tabula_type_elements t),

-- Try to find a year-based match first
     tabula_year_match AS (SELECT e.building_objectid,
                                  e.element_name,
                                  e.area,
                                  e.year_val,
                                  e.construction_match,
                                  e.building_type,
                                  t.element_id
                           FROM element_construction e
                                    JOIN tabula_ranked t
                                         ON t.element_name = e.element_name
                                             AND t.construction_data = e.construction_match
                                             AND e.year_val BETWEEN t.start_year AND t.end_year),

-- Fallback: use latest available dataset if no year-based match exists
     tabula_fallback_match AS (SELECT e.building_objectid,
                                      e.element_name,
                                      e.area,
                                      e.year_val,
                                      e.construction_match,
                                      e.building_type,
                                      t.element_id
                               FROM element_construction e
                                        JOIN tabula_ranked t
                                             ON t.element_name = e.element_name
                                                 AND t.construction_data = e.construction_match
                                                 AND t.rn_latest = 1
                               WHERE NOT EXISTS (SELECT 1
                                                 FROM tabula_year_match ym
                                                 WHERE ym.building_objectid = e.building_objectid
                                                   AND ym.element_name = e.element_name)),

-- Union preferred + fallback matches
     resolved_tabula_elements AS (SELECT *
                                  FROM tabula_year_match
                                  UNION ALL
                                  SELECT *
                                  FROM tabula_fallback_match)


SELECT r.building_objectid,
       r.element_name,
       r.area,
       l.thickness,
       m.name,
       m.density,
       m.thermal_conduc,
       m.heat_capac,
       l.layer_index
FROM resolved_tabula_elements r
         JOIN opendata.tabula_layers l
              ON r.element_id = l.element_id
         JOIN opendata.tabula_materials m
              ON l.material_id = m.material_id;


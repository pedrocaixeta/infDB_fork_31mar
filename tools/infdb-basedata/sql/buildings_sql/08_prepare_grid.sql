-- Create temp table joining grid cells with buildings based on geometry
-- Only keeps grid cells that contain at least one building centroid
-- Optimized for later joins on x_mp and y_mp coordinates
DROP TABLE IF EXISTS temp_grid_transformed;
CREATE TEMP TABLE temp_grid_transformed AS
SELECT
    g.id,
    g.x_mp,
    g.y_mp,
    g.geom
FROM (
    SELECT
        id,
        x_mp,
        y_mp,
        ST_Transform(geom, {EPSG}) as geom
    FROM {input_schema}.grid_cells
    WHERE name='DE_Grid_ETRS89_LAEA_100m'
) AS g
WHERE EXISTS (
    SELECT 1
    FROM {input_schema}.buildings_lod2 b
    WHERE g.geom && b.geom -- prefilter with bounding box &&
      AND ST_Contains(g.geom, ST_Centroid(b.geom))
--        AND b.gemeindeschluessel IN ({list_gemeindeschluessel});
);
CREATE INDEX ON temp_grid_transformed (id);

DELETE FROM {output_schema}.buildings_grid target
--WHERE target.gemeindeschluessel IN ({list_gemeindeschluessel})
  WHERE target.id NOT IN (
    SELECT src.id
    FROM temp_grid_transformed src
  );

INSERT INTO {output_schema}.buildings_grid (id, x_mp, y_mp, geom)
SELECT
    src.id,
    src.x_mp,
    src.y_mp,
    src.geom
FROM temp_grid_transformed src
LEFT JOIN {output_schema}.buildings_grid target
       ON target.geom = src.geom
WHERE target.geom IS NULL
ON CONFLICT (id) DO UPDATE
SET id   = EXCLUDED.id,
    x_mp = EXCLUDED.x_mp,
    y_mp = EXCLUDED.y_mp;

-- Update with population data
UPDATE {output_schema}.buildings_grid
SET einwohner = pop.einwohner
FROM {input_schema}.zensus_2022_100m_bevoelkerungszahl pop
WHERE buildings_grid.x_mp = pop.x_mp_100m
  AND buildings_grid.y_mp = pop.y_mp_100m;

-- Update with household size data
UPDATE {output_schema}.buildings_grid
SET durchschnhhgroesse = hh.durchschnhhgroesse,
    werterlaeuternde_zeichen = hh.werterlaeuternde_zeichen
FROM {input_schema}.zensus_2022_100m_durchschn_haushaltsgroesse hh
WHERE buildings_grid.x_mp = hh.x_mp_100m
  AND buildings_grid.y_mp = hh.y_mp_100m;

-- Update with building type data
UPDATE {output_schema}.buildings_grid
SET insgesamt_gebaeude = bld.insgesamt_gebaeude,
  freiefh = bld.freiefh,
  efh_dhh = bld.efh_dhh,
  efh_reihenhaus = bld.efh_reihenhaus,
  freist_zfh = bld.freist_zfh,
  zfh_dhh = bld.zfh_dhh,
  zfh_reihenhaus = bld.zfh_reihenhaus,
  mfh_3bis6wohnungen = bld.mfh_3bis6wohnungen,
  mfh_7bis12wohnungen = bld.mfh_7bis12wohnungen,
  mfh_13undmehrwohnungen = bld.mfh_13undmehrwohnungen,
  anderergebaeudetyp = bld.anderergebaeudetyp
FROM {input_schema}.zensus_2022_100m_gebaeude_typ_groesse bld
WHERE buildings_grid.x_mp = bld.x_mp_100m
  AND buildings_grid.y_mp = bld.y_mp_100m;

-- Update with construction year data
UPDATE {output_schema}.buildings_grid
SET vor1919 = bauj.vor1919,
  a1919bis1948 = bauj.a1919bis1948,
  a1949bis1978 = bauj.a1949bis1978,
  a1979bis1990 = bauj.a1979bis1990,
  a1991bis2000 = bauj.a1991bis2000,
  a2001bis2010 = bauj.a2001bis2010,
  a2011bis2019 = bauj.a2011bis2019,
  a2020undspaeter = bauj.a2020undspaeter
FROM {input_schema}.zensus_2022_100m_gebaeude_baujahr_mikrozensus bauj
WHERE buildings_grid.x_mp = bauj.x_mp_100m
  AND buildings_grid.y_mp = bauj.y_mp_100m;


-- release memory
DROP TABLE IF EXISTS temp_grid_transformed;
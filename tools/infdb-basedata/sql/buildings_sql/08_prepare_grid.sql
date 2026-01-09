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
SET einwohner = pop.einwohner::bigint
FROM {input_schema}.zensus_2022_100m_bevoelkerungszahl pop
WHERE buildings_grid.x_mp = pop.x_mp_100m
  AND buildings_grid.y_mp = pop.y_mp_100m;

-- Update with household size data
UPDATE {output_schema}.buildings_grid
SET durchschnhhgroesse = REPLACE(hh.durchschnhhgroesse, ',', '.')::double precision,
    werterlaeuternde_zeichen = hh.werterlaeuternde_zeichen::text
FROM {input_schema}.zensus_2022_100m_durchschn_haushaltsgroesse hh
WHERE buildings_grid.x_mp = hh.x_mp_100m
  AND buildings_grid.y_mp = hh.y_mp_100m;

-- Update with building type data
UPDATE {output_schema}.buildings_grid
SET insgesamt_gebaeude = (CASE WHEN bld.insgesamt_gebaeude IN ('-', '–', '') THEN '0' ELSE bld.insgesamt_gebaeude END)::bigint,
    freiefh = (CASE WHEN bld.freiefh IN ('-', '–', '') THEN '0' ELSE bld.freiefh END)::bigint,
    efh_dhh = (CASE WHEN bld.efh_dhh IN ('-', '–', '') THEN '0' ELSE bld.efh_dhh END)::bigint,
    efh_reihenhaus = (CASE WHEN bld.efh_reihenhaus IN ('-', '–', '') THEN '0' ELSE bld.efh_reihenhaus END)::bigint,
    freist_zfh = (CASE WHEN bld.freist_zfh IN ('-', '–', '') THEN '0' ELSE bld.freist_zfh END)::bigint,
    zfh_dhh = (CASE WHEN bld.zfh_dhh IN ('-', '–', '') THEN '0' ELSE bld.zfh_dhh END)::bigint,
    zfh_reihenhaus = (CASE WHEN bld.zfh_reihenhaus IN ('-', '–', '') THEN '0' ELSE bld.zfh_reihenhaus END)::bigint,
    mfh_3bis6wohnungen = (CASE WHEN bld.mfh_3bis6wohnungen IN ('-', '–', '') THEN '0' ELSE bld.mfh_3bis6wohnungen END)::bigint,
    mfh_7bis12wohnungen = (CASE WHEN bld.mfh_7bis12wohnungen IN ('-', '–', '') THEN '0' ELSE bld.mfh_7bis12wohnungen END)::bigint,
    mfh_13undmehrwohnungen = (CASE WHEN bld.mfh_13undmehrwohnungen IN ('-', '–', '') THEN '0' ELSE bld.mfh_13undmehrwohnungen END)::bigint,
    anderergebaeudetyp = (CASE WHEN bld.anderergebaeudetyp IN ('-', '–', '') THEN '0' ELSE bld.anderergebaeudetyp END)::bigint
FROM {input_schema}.zensus_2022_100m_gebaeude_typ_groesse bld
-- WHERE buildings_grid.gemeindeschluessel IN ({list_gemeindeschluessel})
WHERE buildings_grid.x_mp = bld.x_mp_100m
  AND buildings_grid.y_mp = bld.y_mp_100m;

-- Update with construction year data
UPDATE {output_schema}.buildings_grid
SET vor1919 = (CASE WHEN bauj.vor1919 IN ('-', '–', '') THEN '0' ELSE bauj.vor1919 END)::bigint,
    a1919bis1948 = (CASE WHEN bauj.a1919bis1948 IN ('-', '–', '') THEN '0' ELSE bauj.a1919bis1948 END)::bigint,
    a1949bis1978 = (CASE WHEN bauj.a1949bis1978 IN ('-', '–', '') THEN '0' ELSE bauj.a1949bis1978 END)::bigint,
    a1979bis1990 = (CASE WHEN bauj.a1979bis1990 IN ('-', '–', '') THEN '0' ELSE bauj.a1979bis1990 END)::bigint,
    a1991bis2000 = (CASE WHEN bauj.a1991bis2000 IN ('-', '–', '') THEN '0' ELSE bauj.a1991bis2000 END)::bigint,
    a2001bis2010 = (CASE WHEN bauj.a2001bis2010 IN ('-', '–', '') THEN '0' ELSE bauj.a2001bis2010 END)::bigint,
    a2011bis2019 = (CASE WHEN bauj.a2011bis2019 IN ('-', '–', '') THEN '0' ELSE bauj.a2011bis2019 END)::bigint,
    a2020undspaeter = (CASE WHEN bauj.a2020undspaeter IN ('-', '–', '') THEN '0' ELSE bauj.a2020undspaeter END)::bigint
FROM {input_schema}.zensus_2022_100m_gebaeude_baujahr_mikrozensus bauj
-- WHERE buildings_grid.gemeindeschluessel IN ({list_gemeindeschluessel})
WHERE buildings_grid.x_mp = bauj.x_mp_100m
  AND buildings_grid.y_mp = bauj.y_mp_100m;


-- release memory
DROP TABLE IF EXISTS temp_grid_transformed;
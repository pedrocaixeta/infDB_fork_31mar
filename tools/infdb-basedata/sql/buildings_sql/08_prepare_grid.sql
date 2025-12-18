-- Create a temporary table with transformed geometry and spatial index
DROP TABLE IF EXISTS temp_grid_transformed;
CREATE TEMPORARY TABLE temp_grid_transformed AS
SELECT
    id,
    x_mp,
    y_mp,
    ST_Transform(geom, {EPSG}) as geom
FROM {input_schema}.grid_cells
WHERE name='DE_Grid_ETRS89_LAEA_100m';

-- Create spatial index on transformed geometry (fixed column name)
CREATE INDEX temp_grid_geom_idx ON temp_grid_transformed USING GIST (geom);

-- Create table joining grid cells with buildings based on geometry
-- Only keeps grid cells that contain at least one building centroid
-- Optimized for later joins on x_mp and y_mp coordinates
DROP TABLE IF EXISTS {output_schema}.buildings_grid;
CREATE TABLE IF NOT EXISTS {output_schema}.buildings_grid AS
SELECT DISTINCT g.*
FROM temp_grid_transformed g
INNER JOIN {input_schema}.buildings_lod2 b ON ST_Contains(g.geom, ST_Centroid(b.geom));

-- Create composite index on x_mp and y_mp for efficient joins
CREATE INDEX grid_buildings_spatial_coords_idx
    ON {output_schema}.buildings_grid (x_mp, y_mp);

-- Add all census columns to the existing buildings_grid table
ALTER TABLE {output_schema}.buildings_grid
-- zensus_2022_100m_bevoelkerungszahl
ADD COLUMN einwohner bigint,
-- zensus_2022_100m_durchschn_haushaltsgroesse
ADD COLUMN durchschnhhgroesse double precision,
ADD COLUMN werterlaeuternde_zeichen text,
-- zensus_2022_100m_gebaeude_typ_groesse
ADD COLUMN insgesamt_gebaeude bigint,
ADD COLUMN freiefh double precision,
ADD COLUMN efh_dhh double precision,
ADD COLUMN efh_reihenhaus double precision,
ADD COLUMN freist_zfh double precision,
ADD COLUMN zfh_dhh double precision,
ADD COLUMN zfh_reihenhaus double precision,
ADD COLUMN mfh_3bis6wohnungen double precision,
ADD COLUMN mfh_7bis12wohnungen double precision,
ADD COLUMN mfh_13undmehrwohnungen double precision,
ADD COLUMN anderergebaeudetyp double precision,
-- zensus_2022_100m_gebaeude_baujahr_mikrozensus
ADD COLUMN vor1919 double precision,
ADD COLUMN a1919bis1948 double precision,
ADD COLUMN a1949bis1978 double precision,
ADD COLUMN a1979bis1990 double precision,
ADD COLUMN a1991bis2000 double precision,
ADD COLUMN a2001bis2010 double precision,
ADD COLUMN a2011bis2019 double precision,
ADD COLUMN a2020undspaeter double precision;

-- Update with population data
UPDATE {output_schema}.buildings_grid
SET einwohner = pop.einwohner::bigint
FROM {input_schema}.zensus_2022_100m_bevoelkerungszahl pop
WHERE buildings_grid.x_mp = pop.x_mp_100m
  AND buildings_grid.y_mp = pop.y_mp_100m;

-- Update with household size data
UPDATE {output_schema}.buildings_grid
SET durchschnhhgroesse = hh.durchschnhhgroesse::double precision,
    werterlaeuternde_zeichen = hh.werterlaeuternde_zeichen
FROM {input_schema}.zensus_2022_100m_durchschn_haushaltsgroesse hh
WHERE buildings_grid.x_mp = hh.x_mp_100m
  AND buildings_grid.y_mp = hh.y_mp_100m;

-- Update with building type data
UPDATE {output_schema}.buildings_grid
SET insgesamt_gebaeude = bld.insgesamt_gebaeude::bigint,
    freiefh = (CASE WHEN bld.freiefh IN ('-', '–') THEN 'NaN' ELSE bld.freiefh END)::double precision,
    efh_dhh = (CASE WHEN bld.efh_dhh IN ('-', '–') THEN 'NaN' ELSE bld.efh_dhh END)::double precision,
    efh_reihenhaus = (CASE WHEN bld.efh_reihenhaus IN ('-', '–') THEN 'NaN' ELSE bld.efh_reihenhaus END)::double precision,
    freist_zfh = (CASE WHEN bld.freist_zfh IN ('-', '–') THEN 'NaN' ELSE bld.freist_zfh END)::double precision,
    zfh_dhh = (CASE WHEN bld.zfh_dhh IN ('-', '–') THEN 'NaN' ELSE bld.zfh_dhh END)::double precision,
    zfh_reihenhaus = (CASE WHEN bld.zfh_reihenhaus IN ('-', '–') THEN 'NaN' ELSE bld.zfh_reihenhaus END)::double precision,
    mfh_3bis6wohnungen = (CASE WHEN bld.mfh_3bis6wohnungen IN ('-', '–') THEN 'NaN' ELSE bld.mfh_3bis6wohnungen END)::double precision,
    mfh_7bis12wohnungen = (CASE WHEN bld.mfh_7bis12wohnungen IN ('-', '–') THEN 'NaN' ELSE bld.mfh_7bis12wohnungen END)::double precision,
    mfh_13undmehrwohnungen = (CASE WHEN bld.mfh_13undmehrwohnungen IN ('-', '–') THEN 'NaN' ELSE bld.mfh_13undmehrwohnungen END)::double precision,
    anderergebaeudetyp = (CASE WHEN bld.anderergebaeudetyp IN ('-', '–') THEN 'NaN' ELSE bld.anderergebaeudetyp END)::double precision
FROM {input_schema}.zensus_2022_100m_gebaeude_typ_groesse bld
WHERE buildings_grid.x_mp = bld.x_mp_100m
  AND buildings_grid.y_mp = bld.y_mp_100m;

-- Update with construction year data
UPDATE {output_schema}.buildings_grid
SET vor1919 = (CASE WHEN bauj.vor1919 IN ('-', '–', '') THEN '0' ELSE bauj.vor1919 END)::double precision,
    a1919bis1948 = (CASE WHEN bauj.a1919bis1948 IN ('-', '–', '') THEN '0' ELSE bauj.a1919bis1948 END)::double precision,
    a1949bis1978 = (CASE WHEN bauj.a1949bis1978 IN ('-', '–', '') THEN '0' ELSE bauj.a1949bis1978 END)::double precision,
    a1979bis1990 = (CASE WHEN bauj.a1979bis1990 IN ('-', '–', '') THEN '0' ELSE bauj.a1979bis1990 END)::double precision,
    a1991bis2000 = (CASE WHEN bauj.a1991bis2000 IN ('-', '–', '') THEN '0' ELSE bauj.a1991bis2000 END)::double precision,
    a2001bis2010 = (CASE WHEN bauj.a2001bis2010 IN ('-', '–', '') THEN '0' ELSE bauj.a2001bis2010 END)::double precision,
    a2011bis2019 = (CASE WHEN bauj.a2011bis2019 IN ('-', '–', '') THEN '0' ELSE bauj.a2011bis2019 END)::double precision,
    a2020undspaeter = (CASE WHEN bauj.a2020undspaeter IN ('-', '–', '') THEN '0' ELSE bauj.a2020undspaeter END)::double precision
FROM {input_schema}.zensus_2022_100m_gebaeude_baujahr_mikrozensus bauj
WHERE buildings_grid.x_mp = bauj.x_mp_100m
  AND buildings_grid.y_mp = bauj.y_mp_100m;
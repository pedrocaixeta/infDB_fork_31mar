-- Create a temporary table with transformed geometry and spatial index
DROP TABLE IF EXISTS temp_grid_transformed;
CREATE TEMPORARY TABLE temp_grid_transformed AS
SELECT
    id,
    x_mp,
    y_mp,
    ST_Transform(geom, 3035) as geom
FROM opendata.bkg_de_grid_etrs89_laea_100m; -- todo move bkg_de_grid to input_sche

-- Create spatial index on transformed geometry (fixed column name)
CREATE INDEX temp_grid_geom_idx ON temp_grid_transformed USING GIST (geom);

-- Create table joining grid cells with buildings based on geometry
-- Only keeps grid cells that contain at least one building centroid
-- Optimized for later joins on x_mp and y_mp coordinates
DROP TABLE IF EXISTS {output_schema}.buildings_pylovo_grid;
CREATE TABLE IF NOT EXISTS {output_schema}.buildings_pylovo_grid AS
SELECT DISTINCT g.*
FROM temp_grid_transformed g
INNER JOIN {input_schema}.buildings_lod2 b ON ST_Contains(g.geom, ST_Centroid(b.geometry));

-- Create composite index on x_mp and y_mp for efficient joins
CREATE INDEX grid_buildings_spatial_coords_idx
    ON {output_schema}.buildings_pylovo_grid (x_mp, y_mp);

-- Add all census columns to the existing buildings_pylovo_grid table
ALTER TABLE {output_schema}.buildings_pylovo_grid
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
UPDATE {output_schema}.buildings_pylovo_grid
SET einwohner = pop.einwohner
FROM {input_schema}.zensus_2022_100m_bevoelkerungszahl pop
WHERE buildings_pylovo_grid.x_mp = pop.x_mp_100m
  AND buildings_pylovo_grid.y_mp = pop.y_mp_100m;

-- Update with household size data
UPDATE {output_schema}.buildings_pylovo_grid
SET durchschnhhgroesse = hh.durchschnhhgroesse,
    werterlaeuternde_zeichen = hh.werterlaeuternde_zeichen
FROM {input_schema}.zensus_2022_100m_durschn_haushaltsgroesse hh
WHERE buildings_pylovo_grid.x_mp = hh.x_mp_100m
  AND buildings_pylovo_grid.y_mp = hh.y_mp_100m;

-- Update with building type data
UPDATE {output_schema}.buildings_pylovo_grid
SET insgesamt_gebaeude = bld.insgesamt_gebaeude,
    --freiefh = CAST(bld.freiefh,
    freiefh = CAST(bld.freiefh AS double precision),
    efh_dhh = CAST(bld.efh_dhh AS double precision),
    efh_reihenhaus = CAST(bld.efh_reihenhaus AS double precision),
    freist_zfh = CAST(bld.freist_zfh AS double precision),
    zfh_dhh = CAST(bld.zfh_dhh AS double precision),
    zfh_reihenhaus = CAST(bld.zfh_reihenhaus AS double precision),
    mfh_3bis6wohnungen = CAST(bld.mfh_3bis6wohnungen AS double precision),
    mfh_7bis12wohnungen = CAST(bld.mfh_7bis12wohnungen AS double precision),
    mfh_13undmehrwohnungen = CAST(bld.mfh_13undmehrwohnungen AS double precision),
    anderergebaeudetyp = CAST(bld.anderergebaeudetyp AS double precision)
FROM {input_schema}.zensus_2022_100m_gebaeude_typ_groesse bld
WHERE buildings_pylovo_grid.x_mp = bld.x_mp_100m
  AND buildings_pylovo_grid.y_mp = bld.y_mp_100m;

-- Update with construction year data
UPDATE {output_schema}.buildings_pylovo_grid
SET vor1919 = bauj.vor1919,
    a1919bis1948 = bauj.a1919bis1948,
    a1949bis1978 = bauj.a1949bis1978,
    a1979bis1990 = bauj.a1979bis1990,
    a1991bis2000 = bauj.a1991bis2000,
    a2001bis2010 = bauj.a2001bis2010,
    a2011bis2019 = bauj.a2011bis2019,
    a2020undspaeter = bauj.a2020undspaeter
--FROM {input_schema}.zensus_2022_100m_gebaeude_baujahr_mikrozensus bauj
FROM opendata.zensus_2022_100m_gebaeude_baujahr_mikrozensus bauj
WHERE buildings_pylovo_grid.x_mp = bauj.x_mp_100m
  AND buildings_pylovo_grid.y_mp = bauj.y_mp_100m;
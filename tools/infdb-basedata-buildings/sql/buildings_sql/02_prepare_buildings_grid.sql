-- Create buildings grids
-- 100m
CREATE TABLE IF NOT EXISTS ${output_schema}.buildings_grid_100m(
	id text PRIMARY KEY,
	x_mp int4 NOT NULL,
	y_mp int4 NOT NULL,
	geom public.geometry UNIQUE NOT NULL,
-- zensus_2022_100m_bevoelkerungszahl
	einwohner bigint NULL,
-- zensus_2022_100m_durchschn_haushaltsgroesse
	durchschnhhgroesse float8 NULL,
	werterlaeuternde_zeichen text NULL,
-- zensus_2022_100m_gebaeude_typ_groesse
	insgesamt_gebaeude bigint NULL,
	freiefh bigint NULL,
	efh_dhh bigint NULL,
	efh_reihenhaus bigint NULL,
	freist_zfh bigint NULL,
	zfh_dhh bigint NULL,
	zfh_reihenhaus bigint NULL,
	mfh_3bis6wohnungen bigint NULL,
	mfh_7bis12wohnungen bigint NULL,
	mfh_13undmehrwohnungen bigint NULL,
	anderergebaeudetyp bigint NULL,
-- zensus_2022_100m_gebaeude_baujahr_mikrozensus
	vor1919 bigint NULL,
	a1919bis1948 bigint NULL,
	a1949bis1978 bigint NULL,
	a1979bis1990 bigint NULL,
	a1991bis2000 bigint NULL,
	a2001bis2010 bigint NULL,
	a2011bis2019 bigint NULL,
	a2020undspaeter bigint NULL
);

-- Create composite index on x_mp and y_mp for efficient joins
CREATE INDEX IF NOT EXISTS grid_buildings_spatial_coords_idx ON ${output_schema}.buildings_grid_100m USING btree (x_mp, y_mp);
-- Create unique spatial index on geom column for efficient update
CREATE INDEX IF NOT EXISTS idx_buildings_grid_geom ON ${output_schema}.buildings_grid_100m USING GIST (geom);

-- 1km
CREATE TABLE IF NOT EXISTS ${output_schema}.buildings_grid_1km(
	id text PRIMARY KEY,
	x_mp int4 NOT NULL,
	y_mp int4 NOT NULL,
	geom public.geometry UNIQUE NOT NULL,
-- zensus_2022_100m_bevoelkerungszahl
	einwohner bigint NULL,
-- zensus_2022_100m_durchschn_haushaltsgroesse
	durchschnhhgroesse float8 NULL,
	werterlaeuternde_zeichen text NULL,
-- zensus_2022_100m_gebaeude_typ_groesse
	insgesamt_gebaeude bigint NULL,
	freiefh bigint NULL,
	efh_dhh bigint NULL,
	efh_reihenhaus bigint NULL,
	freist_zfh bigint NULL,
	zfh_dhh bigint NULL,
	zfh_reihenhaus bigint NULL,
	mfh_3bis6wohnungen bigint NULL,
	mfh_7bis12wohnungen bigint NULL,
	mfh_13undmehrwohnungen bigint NULL,
	anderergebaeudetyp bigint NULL,
-- zensus_2022_100m_gebaeude_baujahr_mikrozensus
	vor1919 bigint NULL,
	a1919bis1948 bigint NULL,
	a1949bis1978 bigint NULL,
	a1979bis1990 bigint NULL,
	a1991bis2000 bigint NULL,
	a2001bis2010 bigint NULL,
	a2011bis2019 bigint NULL,
	a2020undspaeter bigint NULL
);

-- Create composite index on x_mp and y_mp for efficient joins
CREATE INDEX IF NOT EXISTS grid_buildings_spatial_coords_idx ON ${output_schema}.buildings_grid_1km USING btree (x_mp, y_mp);
-- Create unique spatial index on geom column for efficient update
CREATE INDEX IF NOT EXISTS idx_buildings_grid_geom ON ${output_schema}.buildings_grid_1km USING GIST (geom)

-- If AGS should be added, create a new changeset for the part below
-- ALTER TABLE ${output_schema}.buildings_grid_100m ADD COLUMN gemeindeschluessel text
-- CREATE INDEX IF NOT EXISTS idx_buildings_grid_gemeindeschluessel ON ${output_schema}.buildings_grid_100m (gemeindeschluessel);

-- Create building to grid cell mapping
CREATE TABLE IF NOT EXISTS ${output_schema}.bld2grid (
	objectid text NOT NULL,
	id text NOT NULL,
	resolution_meters int4 NULL,
    CONSTRAINT bld2grid_pkey PRIMARY KEY (objectid, id)
);

-- Find nearest time series for each building
CREATE TABLE IF NOT EXISTS ${output_schema}.bld2ts
(
    id serial PRIMARY KEY,
	bld_objectid text NOT NULL,
	ts_metadata_id int4 NULL,
	ts_metadata_name text NULL,
	dist float8 NULL,
	geom public.geometry NULL
);

-- Add geometry column for visualization
ALTER TABLE ${output_schema}.bld2ts
    ADD COLUMN IF NOT EXISTS geom geometry;
CREATE INDEX IF NOT EXISTS idx_bld2ts_objectid ON ${output_schema}.bld2ts (bld_objectid);
ALTER TABLE ${output_schema}.bld2ts ADD CONSTRAINT UNIQUE_bld_ts UNIQUE (bld_objectid, ts_metadata_name);
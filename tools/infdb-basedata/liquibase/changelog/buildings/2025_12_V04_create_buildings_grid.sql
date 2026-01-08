--liquibase formatted sql
--changeset marvin.huang:1.0.4.0 labels:infdb-basedata,infdb-basedata-buildings

CREATE TABLE IF NOT EXISTS ${output_schema}.buildings_grid(
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
CREATE INDEX IF NOT EXISTS grid_buildings_spatial_coords_idx ON ${output_schema}.buildings_grid USING btree (x_mp, y_mp);

-- Create unique spatial index on geom column for efficient update
CREATE INDEX IF NOT EXISTS idx_buildings_grid_geom ON ${output_schema}.buildings_grid USING GIST (geom)

-- If AGS should be added, create a new changeset for the part below
-- ALTER TABLE ${output_schema}.buildings_grid ADD COLUMN gemeindeschluessel text
-- CREATE INDEX IF NOT EXISTS idx_buildings_grid_gemeindeschluessel ON ${output_schema}.buildings_grid (gemeindeschluessel);

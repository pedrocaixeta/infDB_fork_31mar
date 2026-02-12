-- serialize this init section across containers
SELECT pg_advisory_lock(hashtext('infdb_basedata_init'));

--Create buildings table
CREATE TABLE IF NOT EXISTS {output_schema}.buildings
(
    id                serial PRIMARY KEY,
    feature_id        integer,
    objectid          text UNIQUE NOT NULL,
    height            double precision,
    floor_area        double precision,
    floor_number      int,
    building_use      text NOT NULL,
    building_use_id   text NOT NULL,
    building_type     text,
    occupants         int,
    households        int,
    construction_year text,
    postcode          int,
    address_street_id bigint,
    street            text,
    house_number      text,
    geom              geometry(MultiPolygon, {EPSG}),
    centroid          geometry(Point, {EPSG}),
    gemeindeschluessel text NOT NULL
);

-- Create indexes for performance optimization
CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.buildings USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_building_centroid ON {output_schema}.buildings USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_buildings_building_use ON {output_schema}.buildings (building_use);
CREATE INDEX IF NOT EXISTS idx_buildings_building_type ON {output_schema}.buildings (building_type);
CREATE INDEX IF NOT EXISTS idx_buildings_gemeindeschluessel ON {output_schema}.buildings (gemeindeschluessel);
CREATE INDEX IF NOT EXISTS idx_buildings_feature_id ON {output_schema}.buildings (feature_id);
CREATE INDEX IF NOT EXISTS idx_buildings_height ON {output_schema}.buildings (height);

-- unlock init section across containers
SELECT pg_advisory_unlock(hashtext('infdb_basedata_init'));
CREATE TABLE IF NOT EXISTS {output_schema}.buildings
(
    id                bigint PRIMARY KEY,
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


CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.buildings USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_building_centroid ON {output_schema}.buildings USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_buildings_building_use ON {output_schema}.buildings (building_use);
CREATE INDEX IF NOT EXISTS idx_buildings_building_type ON {output_schema}.buildings (building_type);
CREATE INDEX IF NOT EXISTS idx_building_type_check ON {output_schema}.buildings (id, building_type, building_use);
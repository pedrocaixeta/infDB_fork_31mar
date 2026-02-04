-- DROP TABLE IF EXISTS {output_schema}.{table_name};
CREATE TABLE IF NOT EXISTS {output_schema}.{table_name}
(
    id                SERIAL PRIMARY KEY,
    feature_id        integer,
    objectid          text,
    gemeindeschluessel text,
    ags_id TEXT GENERATED ALWAYS AS (substring(gemeindeschluessel, 1, 2)) STORED,
    objectclass_id    int,
    height            double precision,
    groundsurface_flaeche        double precision,
    storeysaboveground      integer,
    -- building_use      text NOT NULL,
    building_function_code   text NOT NULL,
    -- building_type     text,
    -- occupants         int,
    -- households        int,
    -- construction_year text,
    zip_code          text,
    street            text,
    house_number     text,
    city              text,
    country          text,
    state            text,
    geom              geometry,
    centroid          geometry
) PARTITION BY LIST (ags_id);

CREATE INDEX IF NOT EXISTS building_geom_idx ON {output_schema}.{table_name} USING GIST (geom);
CREATE INDEX IF NOT EXISTS building_centroid_idx ON {output_schema}.{table_name} USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_building_type_check ON {output_schema}.{table_name} (id, objectid, building_function_code);
CREATE INDEX IF NOT EXISTS buildings_lod2_feature_id_idx ON {output_schema}.{table_name} (feature_id);
CREATE INDEX IF NOT EXISTS buildings_lod2_gks_objectid_idx ON {output_schema}.{table_name} (gemeindeschluessel, objectid);
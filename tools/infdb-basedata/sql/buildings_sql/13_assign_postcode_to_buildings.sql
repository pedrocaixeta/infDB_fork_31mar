DROP TABLE IF EXISTS temp_postcode_{EPSG};
CREATE TEMP TABLE IF NOT EXISTS temp_postcode_{EPSG}
(
    plz int,
    geom geometry(Multipolygon, {EPSG})
);
INSERT INTO temp_postcode_{EPSG} (plz, geom)
SELECT plz::int, ST_Transform(geom, {EPSG})
FROM {input_schema}."postcode";
-- FROM opendata."postcode";

UPDATE {output_schema}.buildings b
SET postcode = plz::int
FROM temp_postcode_{EPSG} p
WHERE ST_Contains(p.geom, ST_Centroid(b.geom));
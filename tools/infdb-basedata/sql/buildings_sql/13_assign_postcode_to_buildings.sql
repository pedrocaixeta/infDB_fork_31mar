DROP TABLE IF EXISTS temp_postcode_3035;
CREATE TEMP TABLE IF NOT EXISTS temp_postcode_3035
(
    plz int,
    geom geometry(Multipolygon, 3035)
);
INSERT INTO temp_postcode_3035 (plz, geom)
SELECT plz::int, ST_Transform(geom, 3035)
-- FROM {input_schema}."plz_plz-5stellig";
FROM opendata."plz_plz-5stellig";

UPDATE {output_schema}.buildings b
SET postcode = plz::int
FROM temp_postcode_3035 p
WHERE ST_Contains(p.geom, ST_Centroid(b.geom));
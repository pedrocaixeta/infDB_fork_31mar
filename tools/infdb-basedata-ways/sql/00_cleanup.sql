DROP TABLE IF EXISTS {output_schema}.ways;

CREATE TABLE {output_schema}.ways AS
SELECT
    id::text AS id,
    klasse,
    objektart,
    geom
FROM {input_schema}.basemap_verkehrslinie;

-- add postcode column (nullable for now)
ALTER TABLE {output_schema}.ways
  ADD COLUMN IF NOT EXISTS postcode integer;

ANALYZE {output_schema}.ways;

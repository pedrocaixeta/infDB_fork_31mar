-- 0) Create target table with the SAME column types as ways_tem (creates empty table)
CREATE TABLE IF NOT EXISTS {output_schema}.ways_segmented AS
SELECT
  ags,
  id,
  klasse,
  objektart,
  geom,
  postcode
FROM ways_tem
WHERE false;

-- Optional: constraints (do not change types)
ALTER TABLE {output_schema}.ways_segmented
  ALTER COLUMN ags SET NOT NULL,
  ALTER COLUMN id  SET NOT NULL;

-- Optional but recommended indexes
CREATE INDEX IF NOT EXISTS ways_segmented_ags_idx
  ON {output_schema}.ways_segmented (ags);

CREATE INDEX IF NOT EXISTS ways_segmented_geom_gix
  ON {output_schema}.ways_segmented USING GIST (geom);

-- Optional: prevent duplicates per AGS+id
CREATE UNIQUE INDEX IF NOT EXISTS ways_segmented_ags_id_ux
  ON {output_schema}.ways_segmented (ags, id);

-- 1) Replace rows for this AGS
DELETE FROM {output_schema}.ways_segmented
WHERE ags = '{ags}';

INSERT INTO {output_schema}.ways_segmented (ags, id, klasse, objektart, geom, postcode)
SELECT
  ags,
  id,
  klasse,
  objektart,
  geom,
  postcode
FROM ways_tem
WHERE ags = '{ags}';

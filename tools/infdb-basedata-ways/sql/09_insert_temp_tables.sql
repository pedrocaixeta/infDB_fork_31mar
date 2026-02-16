
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

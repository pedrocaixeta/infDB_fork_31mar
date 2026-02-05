DROP TABLE IF EXISTS ways_tem;

CREATE TEMP TABLE ways_tem AS
SELECT
    v.ogc_fid AS id,
    v.klasse,
    v.objektart,
    v.geom,
    g.ags::text AS ags          -- <-- add AGS into ways_tem
FROM {input_schema}.basemap_verkehrslinie v
JOIN {input_schema}.bkg_vg5000_gem g
  ON g.ags = '{ags}'
 AND ST_Intersects(v.geom, g.geom);

ALTER TABLE ways_tem
  ADD COLUMN IF NOT EXISTS postcode integer;
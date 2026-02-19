DROP TABLE IF EXISTS ways_tem;

CREATE TEMP TABLE ways_tem AS
WITH src AS (
  SELECT
    v.ogc_fid::text AS src_id,
    v.klasse,
    v.objektart,
    v.geom,
    g.ags::text AS ags
  FROM {input_schema}.basemap_verkehrslinie v
  JOIN {input_schema}.bkg_vg5000_gem g
    ON g.ags = '{ags}'
   AND ST_Intersects(v.geom, g.geom)
),
dumped AS (
  SELECT
    src_id,
    klasse,
    objektart,
    ags,
    ST_SetSRID((ST_Dump(geom)).geom, ST_SRID(geom)) AS geom_part
  FROM src
),
ranked AS (
  SELECT
    src_id,
    klasse,
    objektart,
    ags,
    geom_part,
    -- pick exactly one part per src_id
    row_number() OVER (PARTITION BY src_id ORDER BY ST_Length(geom_part) DESC) AS rn
  FROM dumped
  WHERE geom_part IS NOT NULL
    AND NOT ST_IsEmpty(geom_part)
    AND GeometryType(geom_part) = 'LINESTRING'
)
SELECT
  src_id AS id,
  klasse,
  objektart,
  geom_part AS geom,
  ags
FROM ranked
WHERE rn = 1;

ALTER TABLE ways_tem
  ADD COLUMN IF NOT EXISTS postcode integer;


CREATE INDEX IF NOT EXISTS ways_tem_geom_gix ON ways_tem USING gist (geom);
CREATE INDEX IF NOT EXISTS ways_tem_id_bix ON ways_tem (id);
CREATE INDEX IF NOT EXISTS ways_tem_ags_bix ON ways_tem (ags);


-- Ensure column exists (optional if already added earlier)
ALTER TABLE ways_tem
  ADD COLUMN IF NOT EXISTS postcode integer;

WITH pc_srid AS (
  SELECT COALESCE(NULLIF(ST_SRID(geom), 0), {epsg}) AS srid
  FROM {input_schema}."postcodes_germany"
  WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
  LIMIT 1
),
ways_geom AS (
  SELECT
    w.ctid AS rid,
    w.geom AS geom,
    ST_LineMerge(w.geom) AS merged
  FROM ways_tem w
  WHERE w.postcode IS NULL
    AND w.geom IS NOT NULL
    AND NOT ST_IsEmpty(w.geom)
),
ways_pts AS (
  SELECT
    rid,
    CASE
      WHEN ST_GeometryType(merged) = 'ST_LineString'
        THEN ST_LineInterpolatePoint(merged, 0.5)
      ELSE ST_PointOnSurface(geom)
    END AS pt
  FROM ways_geom
)
UPDATE ways_tem w
SET postcode = (
  SELECT pc.plz::int
  FROM pc_srid, {input_schema}."postcodes_germany" pc
  WHERE ways_pts.pt IS NOT NULL
    AND pc.geom && ST_Transform(ways_pts.pt, pc_srid.srid)
    AND ST_Intersects(
      pc.geom,
      ST_Transform(ways_pts.pt, pc_srid.srid)
    )
  ORDER BY pc.plz
  LIMIT 1
)
FROM ways_pts
WHERE w.ctid = ways_pts.rid;

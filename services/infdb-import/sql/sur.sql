CREATE OR REPLACE FUNCTION public.safe_area_fallback(geom geometry) 
RETURNS double precision AS $$
BEGIN
    -- VERSUCH 1: Exakte 3D-Berechnung (Wissenschaftlich korrekt)
    -- Versucht, das Polygon in Dreiecke zu zerlegen.
    RETURN GC_3DArea(ST_Tesselate(ST_MakeValid(geom)));

EXCEPTION WHEN OTHERS THEN
    -- NOTFALL-FALLBACK: Wenn 3D crasht (InternalError), nehmen wir die 2D-Fläche.
    -- ST_Area(geom) ignoriert Z-Werte, stürzt aber NIEMALS ab.
    -- Das ist besser als gar kein Wert.
    RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- 1. Extension (Syntax-Korrektur: IF NOT EXISTS muss NACH EXTENSION stehen)
CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;

CREATE SCHEMA IF NOT EXISTS tmp_bld;

-- -- 2. Performance-Tuning für die Session (kritisch für große Imports)
-- -- Erlaubt größere Hash-Tabellen im RAM, verhindert Spill-to-Disk
-- SET work_mem = '1GB'; 
-- -- Parallelisierung erzwingen (Anpassen je nach CPU-Kernen, z.B. 4 oder 8)
-- SET max_parallel_workers_per_gather = 4;

-- 3. Temporäre Tabelle: UNLOGGED ist der Schlüssel für Speed
DROP TABLE IF EXISTS tmp_bld.{table_name}_ids;

-- Wir extrahieren direkt den Hash der ID für den Join, um RAM zu sparen
CREATE UNLOGGED TABLE tmp_bld.{table_name}_ids AS
SELECT
    f.objectid as building_objectid,
    child ->> 'objectId' AS child_object_id_text,
    -- HASHING: Verwandelt den 60-Byte String in einen 4-Byte Integer für den Join
    hashtext(child ->> 'objectId') AS child_hash, 
    gd.id AS geometry_data_id,
    f.objectclass_id
FROM feature f
    JOIN geometry_data gd ON f.id = gd.feature_id
    CROSS JOIN LATERAL jsonb_array_elements(gd.geometry_properties -> 'children') AS child
WHERE f.objectclass_id IN (709, 710, 712, 901)
    AND (child ->> 'objectId') IS NOT NULL;

-- 4. Indizes: Nur die nötigsten. Hash-Index für den Join ist nicht mehr nötig, 
-- da wir bereits gehasht haben. Ein B-Tree auf dem Integer-Hash ist extrem schnell.
CREATE INDEX IF NOT EXISTS idx_tmp_join_hash ON tmp_bld.{table_name}_ids (child_hash);
-- Dieser Index hilft beim Filtern nach Objectclass im Join
CREATE INDEX IF NOT EXISTS idx_tmp_obj_class ON tmp_bld.{table_name}_ids (objectclass_id);

-- Analyse hilft dem Query Planner, die Statistik für den Join zu verstehen
ANALYZE tmp_bld.{table_name}_ids;

-- 5. Erstellen der Resultat-Tabelle (Ebenfalls UNLOGGED wenn es nur ein Zwischenschritt ist, 
-- sonst LOGGED lassen für Datensicherheit nach Import)
DROP TABLE IF EXISTS {output_schema}.{table_name} CASCADE;

CREATE UNLOGGED TABLE {output_schema}.{table_name} AS
SELECT
    sid2.building_objectid,
    sid.objectclass_id,
    oc.classname,
    -- NULLIF verhindert Absturz bei leeren Strings, sicherheitshalber
    -- NULLIF(pd.val_string, '')::double precision AS area, 
    -- safe_area_fallback(gd.geometry) AS area,
    pd.val_string::double precision AS area,
    ST_Multi(gd.geometry) AS geom
FROM tmp_bld.{table_name}_ids sid
    -- Join über Integer-Hash statt String (Massiver Speedup)
    JOIN tmp_bld.{table_name}_ids sid2 
        ON sid.child_hash = sid2.child_hash 
        -- Sicherheits-Check: Falls Hash-Kollision (extrem unwahrscheinlich), prüfen wir den Text
        AND sid.child_object_id_text = sid2.child_object_id_text 
        AND sid2.objectclass_id = 901 -- sid2 ist die Fläche (Surface)
    JOIN objectclass oc ON oc.id = sid.objectclass_id
    JOIN geometry_data gd ON gd.id = sid.geometry_data_id
    JOIN property pd ON pd.feature_id = gd.feature_id
WHERE sid.objectclass_id IN (709, 710, 712) -- sid ist das Gebäude;
  AND pd.name = 'Flaeche';

-- Indizes auf der Zieltabelle
CREATE INDEX IF NOT EXISTS {table_name}_building_objectid_idx ON {output_schema}.{table_name} (building_objectid);
-- Spatial Index ist teuer, erst am Ende erstellen
CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {output_schema}.{table_name} USING GIST (geom);

-- 6. View Erstellung
-- WARNUNG: Materialized Views verdoppeln den Speicherbedarf. 
-- Wenn diese Daten nicht ständig aktualisiert werden, ist eine "CREATE TABLE AS" besser.
DROP MATERIALIZED VIEW IF EXISTS {output_schema}.{bld_table_name}_view; -- Drop View statt Table
DROP TABLE IF EXISTS {output_schema}.{bld_table_name}_view;

-- Wir nutzen CREATE TABLE statt Materialized View für bessere Performance beim Erstellen
CREATE UNLOGGED TABLE {output_schema}.{bld_table_name}_view AS
SELECT 
    bld.*,
    sur.area AS groundsurface_flaeche,
    ST_Multi(sur.geom) AS geom,
    -- ST_PointOnSurface ist oft schneller und sicherer (garantiert im Polygon) als Centroid für Building-Footprints
    ST_PointOnSurface(sur.geom) AS centroid 
FROM {output_schema}.building_lod2 bld
JOIN {output_schema}.{table_name} sur ON bld.objectid = sur.building_objectid
WHERE sur.objectclass_id = 710; -- 710 = ground surface

-- Indizes für den View (wie in Ihrem Original, aber GIST für Centroid hinzugefügt)
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_objectid_idx ON {output_schema}.{bld_table_name}_view (objectid);
CREATE INDEX IF NOT EXISTS {bld_table_name}_view_geom_idx ON {output_schema}.{bld_table_name}_view USING GIST (geom);
-- ... (restliche Indizes hier einfügen)
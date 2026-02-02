/*
 * SCRIPT PURPOSE AND OVERVIEW:
 * ===========================
 * Assign each building that requires an electrical connection (peak_load_in_kw <> 0)
 * to a suitable way from the existing ways network, and write the chosen way_id into:
 *
 *   {output_schema}.buildings.assigned_way_id
 *
 * DATA SOURCES:
 * - Buildings: {output_schema}.buildings  (expects: id, centroid, peak_load_in_kw, address_street_id)
 * - Ways:      {output_schema}.ways       (expects: way_id, geom, clazz)
 *
 * FLAG:
 * - {use_address_information}::boolean
 *
 *   If TRUE:
 *     1) Try direct match via buildings.address_street_id == ways.way_id (exclude clazz=110)
 *     2) If no direct match, choose nearest suitable way within 2000 units
 *
 *   If FALSE:
 *     - Always choose nearest suitable way within 2000 units (address ignored)
 *
 * NEAREST-WAY LOGIC:
 * - Exclude ways with clazz = 110
 * - Search radius: 2000 units via ST_DWithin
 * - Avoid extremely-close geometries: ST_Distance > 0.1
 * - KNN ordering: centroid <-> w.geom, take closest (LIMIT 1)
 *
 * OUTPUT:
 * - Adds column assigned_way_id (bigint) if missing
 * - Updates assigned_way_id for buildings with peak_load_in_kw <> 0
 */

-- 0) Ensure the target column exists
ALTER TABLE {output_schema}.buildings
    ADD COLUMN IF NOT EXISTS assigned_way_id bigint;

-- 1) Compute best way per building, then update buildings table
WITH buildings_to_assign AS (
    SELECT
        b.id,
        b.address_street_id,
        b.centroid
    FROM {output_schema}.buildings b
    WHERE b.centroid IS NOT NULL
),

best_way AS (
    SELECT
        b.id,
        COALESCE(direct_way.way_id, nearest_way.way_id) AS assigned_way_id
    FROM buildings_to_assign b

    -- FIRST PRIORITY (only if flag is TRUE): address_street_id -> ways.way_id
    LEFT JOIN {output_schema}.ways direct_way
        ON (
            {use_address_information}::boolean
            AND b.address_street_id IS NOT NULL
            AND direct_way.way_id = b.address_street_id
            AND direct_way.clazz <> 110
        )

    -- FALLBACK (or primary if flag is FALSE): nearest suitable way
    LEFT JOIN LATERAL (
        SELECT w.way_id
        FROM {output_schema}.ways w
        WHERE w.clazz <> 110
          AND ST_DWithin(b.centroid, w.geom, 2000)
          AND ST_Distance(b.centroid, w.geom) > 0.1
        ORDER BY b.centroid <-> w.geom
        LIMIT 1
    ) nearest_way
        ON (
            direct_way.way_id IS NULL
            OR NOT {use_address_information}::boolean
        )

    -- ensure we found something
    WHERE COALESCE(direct_way.way_id, nearest_way.way_id) IS NOT NULL
)

UPDATE {output_schema}.buildings b
SET assigned_way_id = bw.assigned_way_id
FROM best_way bw
WHERE b.id = bw.id;

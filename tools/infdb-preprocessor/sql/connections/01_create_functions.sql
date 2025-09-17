/*
 * FUNCTION PURPOSE AND OVERVIEW:
 * =============================
 * This function anmblyzes buildings with electrical load requirements and identifies
 * the optimal connection points to the existing ways network using infdb data.
 * It creates a temporary table containing precomputed connection candidates that can be used
 * for infrastructure planning and ways network extension.
 *
 * The function performs spatial anmblysis with address-aware matching:
 * 1. Identify buildings that require way connections (non-zero peak load)
 * 2. First attempt to match buildings to ways using address_street_id information
 * 3. Fall back to nearest suitable way if no address match is found
 * 4. Calculate the optimal connection line from building to way
 * 5. Determine the precise connection point on the way geometry
 * 6. Store results in an indexed temporary table for efficient processing
 *
 * ALGORITHM OVERVIEW:
 * 1. Filter buildings that need connections (peak_load_in_kw != 0)
 * 2. For each building, first try to match using address_street_id
 * 3. If no address match, find the closest suitable way within 2000 units
 * 4. Generate shortest connection line from building center to way
 * 5. Calculate the exact connection point on the way geometry
 * 6. Create indexed temporary table for efficient downstream processing
 *
 * INPUT: Data from 'buildings_tem' and 'ways_tem' tables (INFDB database)
 * OUTPUT: Temporary table 'connections_buildings_to_ways' with connection anmblysis
 */

CREATE OR REPLACE FUNCTION {output_schema}.generate_connections_buildings_to_ways() RETURNS void AS $$
BEGIN
    -- Note: Indexes significantly improve performance when the temporary table
    -- is used in subsequent spatial operations or way segmentation processes
    
    -- 1) Create initial connections table by matching building street names to way names
    CREATE TABLE {output_schema}.connections_buildings_to_ways AS
        WITH building_addresses_to_ways AS (
            SELECT DISTINCT ON (b.id)
            b.id as building_id,
            w.way_id as way_way_id
            FROM {output_schema}.buildings_pylovo b
            LEFT JOIN {output_schema}.ways AS w
            ON b.street = w.name OR b.street = w.name_kurz 
            ORDER BY b.id, b.centroid <-> w.geom
        )
        SELECT * FROM building_addresses_to_ways;
        
        
    -- 2) Find buildings without assigned way from address matching and assign nearest way
    WITH not_matched_buildings AS (
            SELECT  buildings.id AS building_id,
                    streets.way_id as way_way_id
            FROM {output_schema}.buildings_pylovo buildings
            CROSS JOIN LATERAL (
                SELECT streets.way_id, streets.geom <-> ST_Centroid(buildings.geom) AS dist
                FROM {output_schema}.ways AS streets
                WHERE streets.geom <-> ST_Centroid(buildings.geom) > 0.1 AND streets.geom <-> ST_Centroid(buildings.geom) < 1000
                ORDER BY dist
                LIMIT 1
            ) streets
            WHERE buildings.id IN (
                SELECT building_id
                FROM {output_schema}.connections_buildings_to_ways
                WHERE way_way_id IS NULL
            )
    )
    UPDATE {output_schema}.connections_buildings_to_ways AS batw
    SET way_way_id = nmb.way_way_id
    FROM not_matched_buildings nmb
    WHERE batw.building_id = nmb.building_id;

    -- 3) Add connection geometry and distance information
    ALTER TABLE {output_schema}.connections_buildings_to_ways
    ADD COLUMN IF NOT EXISTS connection_geom   geometry,
    ADD COLUMN IF NOT EXISTS startpoint_geom   geometry,
    ADD COLUMN IF NOT EXISTS endpoint_geom     geometry,
    ADD COLUMN IF NOT EXISTS dist   double precision;

    UPDATE {output_schema}.connections_buildings_to_ways AS batw
    SET connection_geom = ST_ShortestLine(b.centroid, w.geom),
        startpoint_geom = ST_StartPoint(ST_ShortestLine(b.centroid, w.geom)),
        endpoint_geom = ST_EndPoint(ST_ShortestLine(b.centroid, w.geom)),
        dist = ST_Distance(b.centroid, w.geom)
    FROM {output_schema}.ways w, {output_schema}.buildings_pylovo b
    WHERE batw.way_way_id = w.way_id
      AND batw.building_id = b.id;

    -- 4) Add precise connection point on the way geometry
    CREATE INDEX connections_buildings_to_ways_building_id_idx ON {output_schema}.connections_buildings_to_ways (building_id);
    CREATE INDEX connections_buildings_to_ways_way_id_idx ON {output_schema}.connections_buildings_to_ways (way_way_id);

    -- For debugging: Create a test table to visualize connections
    DROP TABLE IF EXISTS {output_schema}.connections_debug;
    CREATE TABLE {output_schema}.connections_debug AS
    SELECT
    b.geom AS building_geom,
    w.geom AS way_geom,
    b.street AS building_street,
    w.name AS way_name,
    w.name_kurz AS way_name_kurz, 
    ST_ShortestLine(b.centroid, w.geom) AS connection_geom,
    ST_StartPoint(ST_ShortestLine(b.centroid, w.geom)) AS startpoint_geom,
    ST_EndPoint(ST_ShortestLine(b.centroid, w.geom)) AS endpoint_geom,
    ST_Distance(b.centroid, w.geom) AS dist
    FROM {output_schema}.connections_buildings_to_ways batw
    JOIN {output_schema}.buildings_pylovo b
      ON batw.building_id = b.id
    JOIN {output_schema}.ways w
      ON batw.way_way_id = w.way_id;

END;
$$ LANGUAGE plpgsql;
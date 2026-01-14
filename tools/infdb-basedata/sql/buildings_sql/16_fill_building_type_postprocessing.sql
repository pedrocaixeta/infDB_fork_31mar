-- Step 5: Set rest to AB
UPDATE {output_schema}.buildings b
SET building_type = 'AB'
WHERE b.building_use = 'Residential'
  AND b.building_type IS NULL;


-- fix wrong assignments
UPDATE {output_schema}.buildings b
SET building_type = 'SFH'
FROM temp_touching_neighbor_counts nc
WHERE b.id = nc.id
  AND building_type IN ('MFH', 'AB')
  AND households = 1
  AND nc.count = 0;

UPDATE {output_schema}.buildings b
SET building_type = 'TH'
FROM temp_touching_neighbor_counts nc
WHERE b.id = nc.id
  AND building_type IN ('MFH', 'AB')
  AND households = 1
  AND nc.count != 0;

UPDATE {output_schema}.buildings b
SET building_type = 'MFH'
FROM temp_touching_neighbor_counts nc
WHERE b.id = nc.id
  AND building_type IN ('SFH', 'TH')
  AND households BETWEEN 2 AND 4;

UPDATE {output_schema}.buildings b
SET building_type = 'AB'
FROM temp_touching_neighbor_counts nc
WHERE b.id = nc.id
  AND building_type IN ('SFH', 'TH')
  AND households >= 5;

-- Rebalance according to census data
-- This script rebalances residential building types according to reference data

-- Create a mapping between building types and reference columns
-- AB (Apartment Buildings) = mfh_13undmehrwohnungen + mfh_7bis12wohnungen
-- MFH (Multi-Family Houses) = mfh_3bis6wohnungen + freist_zfh + zfh_dhh
-- TH (Terraced Houses) = efh_reihenhaus + zfh_reihenhaus
-- SFH (Single Family Houses) = freiefh + efh_dhh

-- Step 1: Assign grid id for later use
ALTER TABLE {output_schema}.buildings ADD COLUMN grid_id text;
UPDATE {output_schema}.buildings
SET grid_id = g.id
FROM {output_schema}.buildings_grid g
WHERE ST_Contains(g.geom, centroid);

-- Step 2: Calculate current counts and target counts per grid
DROP TABLE IF EXISTS temp_grid_current;
CREATE TABLE temp_grid_current AS
WITH grid_current AS (
    SELECT
        g.id as grid_id,
        COUNT(CASE WHEN b.building_type = 'AB' THEN 1 END) as current_ab,
        COUNT(CASE WHEN b.building_type = 'MFH' THEN 1 END) as current_mfh,
        COUNT(CASE WHEN b.building_type = 'TH' THEN 1 END) as current_th,
        COUNT(CASE WHEN b.building_type = 'SFH' THEN 1 END) as current_sfh,
        COUNT(*) as total_buildings
    FROM {output_schema}.buildings b
    JOIN {output_schema}.buildings_grid g ON ST_Contains(g.geom, b.centroid)
    WHERE b.building_use = 'Residential' AND g.id IS NOT NULL
    GROUP BY g.id
)
SELECT * FROM grid_current;

DROP TABLE IF EXISTS temp_grid_target;
CREATE TABLE temp_grid_target AS (
    SELECT
        id as grid_id,
        -- Calculate target counts from reference data
            COALESCE(mfh_13undmehrwohnungen, 0)
          + COALESCE(mfh_7bis12wohnungen, 0) AS target_ab,

            COALESCE(mfh_3bis6wohnungen, 0)
          + COALESCE(freist_zfh, 0) AS target_mfh,

            COALESCE(efh_reihenhaus, 0)
          + COALESCE(zfh_reihenhaus, 0)
          + COALESCE(zfh_dhh, 0)
          + COALESCE(efh_dhh, 0) AS target_th,

            COALESCE(freiefh, 0) AS target_sfh,

            COALESCE(freiefh, 0)
          + COALESCE(efh_dhh, 0)
          + COALESCE(efh_reihenhaus, 0)
          + COALESCE(freist_zfh, 0)
          + COALESCE(zfh_dhh, 0)
          + COALESCE(zfh_reihenhaus, 0)
          + COALESCE(mfh_3bis6wohnungen, 0)
          + COALESCE(mfh_7bis12wohnungen, 0)
          + COALESCE(mfh_13undmehrwohnungen, 0) AS total_target
    FROM {output_schema}.buildings_grid g
    WHERE g.id IS NOT NULL
    AND EXISTS (
        SELECT 1
        FROM {output_schema}.buildings b
        WHERE b.grid_id = g.id
    )
);

DROP TABLE IF EXISTS temp_grid_comparison;
CREATE TABLE temp_grid_comparison AS
WITH grid_comparison AS (
    SELECT
        gc.grid_id,
        gc.current_ab,
        gc.current_mfh,
        gc.current_th,
        gc.current_sfh,
        gc.total_buildings,
        gt.target_ab,
        gt.target_mfh,
        gt.target_th,
        gt.target_sfh,
        gt.total_target,
        -- Calculate needed adjustments (scaled to current total)
        CASE WHEN gt.total_target > 0 THEN
            ROUND(gt.target_ab * gc.total_buildings / gt.total_target) - gc.current_ab
        ELSE 0 END as ab_adjustment,
        CASE WHEN gt.total_target > 0 THEN
            ROUND(gt.target_mfh * gc.total_buildings / gt.total_target) - gc.current_mfh
        ELSE 0 END as mfh_adjustment,
        CASE WHEN gt.total_target > 0 THEN
            ROUND(gt.target_th * gc.total_buildings / gt.total_target) - gc.current_th
        ELSE 0 END as th_adjustment,
        CASE WHEN gt.total_target > 0 THEN
            ROUND(gt.target_sfh * gc.total_buildings / gt.total_target) - gc.current_sfh
        ELSE 0 END as sfh_adjustment
    FROM temp_grid_current gc
    LEFT JOIN temp_grid_target gt ON gc.grid_id = gt.grid_id
)
SELECT * FROM grid_comparison;

-- Step 3: Create conversion plan
DROP TABLE IF EXISTS temp_building_rankings;
CREATE TABLE temp_building_rankings AS (
    SELECT
        b.id,
        b.building_type,
        b.households,
        b.occupants,
        b.floor_area,
        b.height,
        gc.grid_id,
        gc.ab_adjustment,
        gc.mfh_adjustment,
        gc.th_adjustment,
        gc.sfh_adjustment,

        -- Rankings for conversion priorities
        -- For AB increases: prioritize largest MFH, then largest TH
        ROW_NUMBER() OVER (
            PARTITION BY gc.grid_id, b.building_type
            ORDER BY
                CASE WHEN b.building_type = 'MFH' THEN b.floor_area * b.height END DESC NULLS LAST,
                CASE WHEN b.building_type = 'TH' THEN b.floor_area * b.height END DESC NULLS LAST
        ) as ab_conversion_rank,

        -- For MFH increases: prioritize largest TH, then smallest AB
        ROW_NUMBER() OVER (
            PARTITION BY gc.grid_id, b.building_type
            ORDER BY
                CASE WHEN b.building_type = 'TH' THEN b.floor_area * b.height END DESC NULLS LAST,
                CASE WHEN b.building_type = 'AB' AND b.households <= 4 THEN b.floor_area * b.height END ASC NULLS LAST
        ) as mfh_conversion_rank,

        -- For TH increases: prioritize smaller MFH
        ROW_NUMBER() OVER (
            PARTITION BY gc.grid_id, b.building_type
            ORDER BY
                CASE WHEN b.building_type = 'MFH' AND b.households <= 2 THEN b.floor_area * b.height END ASC NULLS LAST
        ) as th_conversion_rank,

        -- For SFH increases: prioritize smaller TH, then smaller MFH
        ROW_NUMBER() OVER (
            PARTITION BY gc.grid_id, b.building_type
            ORDER BY
                CASE WHEN b.building_type = 'TH' THEN b.floor_area * b.height END ASC NULLS LAST,
                CASE WHEN b.building_type = 'MFH' AND b.households <= 2 THEN b.floor_area * b.height END ASC NULLS LAST
        ) as sfh_conversion_rank

    FROM {output_schema}.buildings b
    JOIN {output_schema}.buildings_grid g ON ST_Contains(g.geom, b.centroid)
    JOIN temp_grid_comparison gc ON g.id = gc.grid_id
    WHERE b.building_use = 'Residential'
      AND gc.total_target > 0
);

-- Drop the helper column again
ALTER TABLE {output_schema}.buildings DROP COLUMN grid_id;

-- Pre-calculate the subquery values once per grid_id
DROP TABLE IF EXISTS temp_grid_counts;
CREATE TABLE temp_grid_counts AS (
    SELECT
        grid_id,
        COUNT(CASE WHEN building_type = 'MFH' AND households > 1 THEN 1 END) as mfh_multi_household_count,
        COUNT(CASE WHEN building_type = 'TH' THEN 1 END) as th_count
    FROM temp_building_rankings
    GROUP BY grid_id
);

-- Create the conversion decisions table with a single join
DROP TABLE IF EXISTS temp_conversion_decisions;
CREATE TABLE temp_conversion_decisions AS (
    SELECT
        br.id,
        br.building_type as original_type,
        br.households,
        br.occupants,
        br.grid_id,

        -- Determine new building type based on conversion needs and rankings
        CASE
            -- Convert to AB
            WHEN br.ab_adjustment > 0 AND (
                (br.building_type = 'MFH' AND br.households > 1 AND br.ab_conversion_rank <= br.ab_adjustment) OR
                (br.building_type = 'TH' AND br.ab_conversion_rank <= GREATEST(0, br.ab_adjustment - gc.mfh_multi_household_count))
            ) THEN 'AB'

            -- Convert to MFH
            WHEN br.mfh_adjustment > 0 AND (
                (br.building_type = 'TH' AND br.mfh_conversion_rank <= br.mfh_adjustment) OR
                (br.building_type = 'AB' AND br.households <= 4 AND br.mfh_conversion_rank <= GREATEST(0, br.mfh_adjustment - gc.th_count))
            ) THEN 'MFH'

            -- Convert to TH
            WHEN br.th_adjustment > 0 AND br.building_type = 'MFH' AND br.households <= 2 AND br.th_conversion_rank <= br.th_adjustment
            THEN 'TH'

            -- Convert to SFH
            WHEN br.sfh_adjustment > 0 AND (
                (br.building_type = 'TH' AND br.sfh_conversion_rank <= br.sfh_adjustment) OR
                (br.building_type = 'MFH' AND br.households <= 2 AND br.sfh_conversion_rank <= GREATEST(0, br.sfh_adjustment - gc.th_count))
            ) THEN 'SFH'

            ELSE br.building_type
        END as new_type,

        -- Calculate new household counts
        CASE
            -- AB conversions
            WHEN br.ab_adjustment > 0 AND (
                (br.building_type = 'MFH' AND br.households > 1 AND br.ab_conversion_rank <= br.ab_adjustment) OR
                (br.building_type = 'TH' AND br.ab_conversion_rank <= GREATEST(0, br.ab_adjustment - gc.mfh_multi_household_count))
            ) THEN GREATEST(br.households, 2)

            -- MFH conversions
            WHEN br.mfh_adjustment > 0 AND (
                (br.building_type = 'TH' AND br.mfh_conversion_rank <= br.mfh_adjustment) OR
                (br.building_type = 'AB' AND br.households <= 4 AND br.mfh_conversion_rank <= GREATEST(0, br.mfh_adjustment - gc.th_count))
            ) THEN GREATEST(br.households, 2)

            -- SFH conversions
            WHEN br.sfh_adjustment > 0 AND (
                (br.building_type = 'TH' AND br.sfh_conversion_rank <= br.sfh_adjustment) OR
                (br.building_type = 'MFH' AND br.households <= 2 AND br.sfh_conversion_rank <= GREATEST(0, br.sfh_adjustment - gc.th_count))
            ) THEN 1

            ELSE br.households
        END as new_households

    FROM temp_building_rankings br
    JOIN temp_grid_counts gc ON br.grid_id = gc.grid_id
);


DROP TABLE IF EXISTS temp_conversion_plan;
CREATE TABLE temp_conversion_plan AS
(
    SELECT
        id,
        original_type,
        new_type,
        households,
        new_households,
        GREATEST(occupants, new_households, CASE WHEN new_type = 'AB' THEN 2 ELSE 1 END) as new_occupants
    FROM temp_conversion_decisions
    WHERE original_type != new_type
);

-- Step 4: Apply all conversions
UPDATE {output_schema}.buildings
SET
    building_type = cp.new_type,
    households = cp.new_households,
    occupants = cp.new_occupants
FROM temp_conversion_plan cp
WHERE buildings.id = cp.id;

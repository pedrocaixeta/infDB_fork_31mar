-- fill floor_number column
-- Step 1: Use storeysAboveGround from LOD2 data where available and reasonable
WITH floor_number_data AS (
    SELECT
        b.feature_id,
        b.height,
        l.storeysAboveGround AS source_floors,
        -- Calculate if the source floors make sense given the height
        -- Typical floor height should be between 2.5m and 5m
        CASE
            WHEN l.storeysAboveGround IS NULL OR l.storeysAboveGround = 0 THEN NULL
            WHEN b.height / l.storeysAboveGround < 2.0 THEN NULL  -- Too short per floor, suspicious
            WHEN b.height / l.storeysAboveGround > 6.0 THEN NULL  -- Too tall per floor, suspicious
            ELSE l.storeysAboveGround
        END AS validated_floors
    FROM {output_schema}.buildings b
    LEFT JOIN {input_schema}.buildings_lod2 l ON b.feature_id = l.feature_id
)
UPDATE {output_schema}.buildings b
SET floor_number = fnd.validated_floors
FROM floor_number_data fnd
WHERE b.feature_id = fnd.feature_id
  AND fnd.validated_floors IS NOT NULL;

-- Step 2: Calculate average floor height per building use from validated data
-- This gives us reliable floor heights to use for estimation
WITH average_floor_height AS (
    SELECT
        building_use_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (height / NULLIF(floor_number, 0))) as median_height_per_floor,
        COUNT(*) as sample_count
    FROM {output_schema}.buildings
    WHERE floor_number IS NOT NULL AND floor_number > 0
    GROUP BY building_use_id
)
UPDATE {output_schema}.buildings b
SET floor_number = GREATEST(ROUND(b.height / afh.median_height_per_floor), 1)
FROM average_floor_height afh
WHERE b.floor_number IS NULL
  AND b.building_use_id = afh.building_use_id
  AND afh.sample_count >= 5  -- Only use if we have enough samples
  AND afh.median_height_per_floor IS NOT NULL;

-- Step 3: For remaining buildings, use overall floor height by building type
-- Residential: ~3.0m, Commercial: ~3.5m, Public: ~3.5m
-- This catches buildings where building_use_id had too few samples in Step 2
UPDATE {output_schema}.buildings
SET floor_number = GREATEST(
    ROUND(height /
        CASE
            WHEN building_use = 'Residential' THEN 3.0
            WHEN building_use = 'Commercial' THEN 3.5
            WHEN building_use = 'Public' THEN 3.5
            ELSE 3.2
        END
    ), 1)
WHERE floor_number IS NULL
  AND height IS NOT NULL;

-- Step 4: Final fallback for any remaining buildings (use 3.2m average)
UPDATE {output_schema}.buildings
SET floor_number = GREATEST(ROUND(height / 3.2), 1)
WHERE floor_number IS NULL
  AND height IS NOT NULL;

-- Step 5: Set minimum of 1 floor for buildings without height data
UPDATE {output_schema}.buildings
SET floor_number = 1
WHERE floor_number IS NULL;

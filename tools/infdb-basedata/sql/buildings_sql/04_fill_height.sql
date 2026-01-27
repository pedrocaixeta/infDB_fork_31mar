-- Summary: Updates the height column in the buildings table using data from
-- buildings_lod2. It also filters out invalid buildings by removing those
-- with a height less than 3.5 meters.

-- fill height column
WITH height_data AS (SELECT b.feature_id, 
                    height AS val_double
                     FROM {input_schema}.buildings_lod2 b)
                    -- WHERE b.gemeindeschluessel IN ({list_gemeindeschluessel})
                    --  WHERE p.name = 'value'
                    --    AND p.parent_id IN (SELECT id FROM property WHERE name = 'height'))
UPDATE {output_schema}.buildings b
SET height = hd.val_double
FROM height_data hd
-- WHERE b.gemeindeschluessel IN ({list_gemeindeschluessel})
WHERE b.feature_id = hd.feature_id;

-- delete buildings below a height threshold
DELETE
FROM {output_schema}.buildings b
-- WHERE b.gemeindeschluessel IN ({list_gemeindeschluessel})
WHERE height < 3.5;
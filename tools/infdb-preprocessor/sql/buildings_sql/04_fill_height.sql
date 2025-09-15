-- fill height column
WITH height_data AS (SELECT b.feature_id, 
                    height AS val_double
                     FROM {input_schema}.buildings_lod2 b)
                    --  WHERE p.name = 'value'
                    --    AND p.parent_id IN (SELECT id FROM property WHERE name = 'height'))
UPDATE {output_schema}.buildings_pylovo b
SET height = hd.val_double
FROM height_data hd
WHERE b.feature_id = hd.feature_id;

-- delete buildings below a height threshold
DELETE
FROM {output_schema}.buildings_pylovo
WHERE height < 3.5;
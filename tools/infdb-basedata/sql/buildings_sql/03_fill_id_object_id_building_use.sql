-- Fill id, objectid and building use columns
INSERT INTO {output_schema}.buildings (id, feature_id, objectid, building_use, building_use_id, street, house_number)
SELECT b.id,
       b.feature_id,
       b.objectid,
       {output_schema}.classify_building_use(b.building_function_code) as building_use,
       b.building_function_code                                     as building_use_id,
       b.street,
       b.house_number
FROM {input_schema}.buildings_lod2 b
--          JOIN property p ON f.id = p.feature_id
-- WHERE f.objectclass_id = 901 -- =building
--   AND p.namespace_id = 10 -- =bldg (redundant?)
--   AND p.name = 'function'
WHERE building_function_code LIKE '31001_%'  -- only allow buildings
  AND building_function_code <> '31001_2463' -- exclude garages
  AND building_function_code <> '31001_2513' -- exclude water containers
  -- AND gemeindeschluessel IS IN {list_gemeindeschluessel} -- todo
ORDER BY b.id;

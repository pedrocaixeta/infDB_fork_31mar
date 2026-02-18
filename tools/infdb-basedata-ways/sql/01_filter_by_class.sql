DELETE FROM ways_tem
WHERE
  (
    ({klasse_filter_enabled}::boolean) AND klasse NOT IN {klasse_filter_tuple}
  )
  OR
  (
    ({objektart_filter_enabled}::boolean)
    AND klasse IN {classes_with_obj_filter_tuple}
    AND NOT ( {objektart_filter_conditions} )
  );


DROP TABLE IF EXISTS ways;

-- create persistent table with same structure and indexes/constraints
CREATE TABLE IF NOT EXISTS {output_schema}.ways
(LIKE ways_tem INCLUDING ALL);

-- copy the data
INSERT INTO {output_schema}.ways
SELECT * FROM ways_tem;



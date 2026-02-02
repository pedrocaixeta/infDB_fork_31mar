DELETE FROM {output_schema}.ways
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

ANALYZE {output_schema}.ways;

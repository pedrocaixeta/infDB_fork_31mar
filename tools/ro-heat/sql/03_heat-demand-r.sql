-- DROP TABLE IF EXISTS {output_schema}.annual_heating_demand;

CREATE TABLE IF NOT EXISTS {output_schema}.annual_heating_demand (
    building_objectid text PRIMARY KEY,
    "heating:demand[Wh]" double precision
);

INSERT INTO {output_schema}.annual_heating_demand (building_objectid, "heating:demand[Wh]")
    SELECT
        bldrc.building_objectid,
        (count(ts.value)*{temp_in}-SUM(ts.value))/bldrc.resistance AS "heating:demand[Wh]"
    FROM
        {output_schema}.buildings_rc AS bldrc
    JOIN
        basedata.bld2ts
        ON bldrc.building_objectid = basedata.bld2ts.bld_objectid
    JOIN
        opendata.openmeteo_ts_data AS ts
        ON basedata.bld2ts.ts_metadata_id = ts.ts_metadata_id
    JOIN opendata.openmeteo_ts_metadata
        ON ts.ts_metadata_id = opendata.openmeteo_ts_metadata.id
    JOIN basedata.buildings
        ON bldrc.building_objectid = basedata.buildings.objectid
    WHERE
        opendata.openmeteo_ts_metadata.name = 'openmeteo_hourly_temperature_2m'
        AND ts.time  >= '{start_time}'
        AND ts.time <  '{end_time}'
        AND basedata.buildings.gemeindeschluessel LIKE '{ags}'
        AND ts.value < {temp_in}
    GROUP BY bldrc.building_objectid, bldrc.resistance
ON CONFLICT (building_objectid)
DO UPDATE SET
    "heating:demand[Wh]" = EXCLUDED."heating:demand[Wh]";
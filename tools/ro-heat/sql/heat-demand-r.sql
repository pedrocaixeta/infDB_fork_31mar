DROP TABLE IF EXISTS ro_heat.annual_heating_demand;
CREATE TABLE ro_heat.annual_heating_demand AS
    SElECT
        bldrc.building_objectid,
        bldrc.resistance,
        SUM(ts.value) as temp_sum,
        count(ts.value) as count_temp,
        (SUM(ts.value)-count(ts.value)*{temp_in})/bldrc.resistance as heating:demand[Wh]
    FROM
        ro_heat.buildings_rc AS bldrc
    JOIN
        basedata.bld2ts
        ON bldrc.building_objectid = basedata.bld2ts.bld_objectid
    JOIN
        opendata.openmeteo_ts_data AS ts
        ON basedata.bld2ts.ts_metadata_id = ts.ts_metadata_id
    JOIN opendata.openmeteo_ts_metadata
        ON ts.ts_metadata_id = opendata.openmeteo_ts_metadata.id
    JOIN opendata.buildings_lod2
        ON bldrc.building_objectid = opendata.buildings_lod2.objectid
    WHERE
        opendata.openmeteo_ts_metadata.name='openmeteo_hourly_temperature_2m'
        AND ts.time  >= '{start_time}'
        AND ts.time <  '{end_time}'
        AND opendata.buildings_lod2.gemeindeschluessel LIKE '{ags}'
        AND ts.value < {temp_in}
    GROUP BY bldrc.building_objectid, bldrc.resistance;
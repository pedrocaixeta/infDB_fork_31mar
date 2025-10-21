# Get time series data for a given location and time range

import pandas as pd


def get_hourly_temperature_2m(objectid, database_connection, start_time=None, end_time=None):
    query = f"""
        SELECT time, value from opendata.openmeteo_ts_data
        JOIN opendata.openmeteo_ts_metadata ON opendata.openmeteo_ts_data.ts_metadata_id = opendata.openmeteo_ts_metadata.id
        JOIN basedata.bld2grid ON opendata.openmeteo_ts_metadata.grid_id = basedata.bld2grid.id
        WHERE objectid='{objectid}' and
            openmeteo_ts_metadata.name='openmeteo_hourly_temperature_2m' and
                time >= '{start_time}'
            AND time <  '{end_time}'
        ORDER BY time ASC;
    """
    df = pd.read_sql(sql=query, con=database_connection)
    df.set_index('time', inplace=True)

    return df
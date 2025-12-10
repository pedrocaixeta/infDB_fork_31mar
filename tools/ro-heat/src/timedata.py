# Get time series data for a given location and time range
import asyncio
import io
from functools import partial

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


"""
        data = {
            "weather": pd.DataFrame(
                {
                    "temp_out": df_hourly_temperature2m["value"].values,
                    "datetime": df_hourly_temperature2m.index,
                }
            )
        }
"""


def get_distinct_building_ids(database_connection):
    query = """
            SELECT DISTINCT objectid
            FROM opendata.buildings_lod2 \
            """
    df = pd.read_sql(sql=query, con=database_connection)
    return df['objectid'].tolist()


def get_all_timeseries_data(database_connection):
    query = f"""
        SELECT *
        FROM opendata.openmeteo_ts_data
    """
    df = pd.read_sql(sql=query, con=database_connection)
    df.set_index('time', inplace=True)

    return df


def get_bld2ts(database_connection):
    query = f"""
        SELECT *
        FROM basedata.bld2ts
    """
    df = pd.read_sql(sql=query, con=database_connection)

    return df


def post_timeseries_data(database_connection, df_timeseries):
    df_timeseries.to_sql('openmeteo_ts_data', con=database_connection, schema='opendata', if_exists='append',
                         index=False)


def post_time_series(engine, infdblog, output_schema, table_name, hourly_dataframe):
    # Insert time series data using auto generated ts_metadata_id
    try:
        # write CSV to an in-memory buffer without header/index
        buf = io.StringIO()
        hourly_dataframe[['ts_metadata_id', 'time', 'value']].to_csv(buf, index=False, header=False)
        buf.seek(0)

        conn = engine.raw_connection()
        cur = conn.cursor()
        copy_sql = f"COPY {output_schema}.{table_name} (ts_metadata_id, time, value) FROM STDIN WITH (FORMAT csv)"
        cur.copy_expert(copy_sql, buf)
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        infdblog.error("Failed to COPY hourly data to Postgres: %s", e)


def add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, column: str):
    ts_metadata_id = None
    try:
        with engine.begin() as conn:
            result = pd.read_sql(insert_metadata_sql, con=conn)
            if not result.empty:
                ts_metadata_id = result['id'].iloc[0]
                infdblog.info("Created metadata record with id=%s", ts_metadata_id)
    except Exception as e:
        infdblog.error("Failed to insert/retrieve metadata record: %s", e)

    indoor_temp = row['hvac'].loc[:, 'indoor_temperature[C]']
    hourly_dataframe = pd.DataFrame({
        'ts_metadata_id': ts_metadata_id,
        'time': indoor_temp.index,
        'value': indoor_temp.values,
    })

    post_time_series(engine, infdblog, output_schema, table_name, hourly_dataframe)


async def add_metadata_and_ts_async(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, column: str):
    """Async version of add_metadata_and_ts"""
    ts_metadata_id = None
    try:
        # Run database operation in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        with engine.begin() as conn:
            result = await loop.run_in_executor(
                None,
                partial(pd.read_sql, insert_metadata_sql, con=conn)
            )
            if not result.empty:
                ts_metadata_id = result['id'].iloc[0]
                infdblog.info("Created metadata record with id=%s", ts_metadata_id)
    except Exception as e:
        infdblog.error("Failed to insert/retrieve metadata record: %s", e)
        return

    # Extract time series data based on column name
    if column in row['hvac'].columns:
        ts_data = row['hvac'].loc[:, column]
    else:
        infdblog.warning(f"Column {column} not found for objectid, skipping")
        return

    hourly_dataframe = pd.DataFrame({
        'ts_metadata_id': ts_metadata_id,
        'time': ts_data.index,
        'value': ts_data.values,
    })

    # Run post_time_series in thread pool
    await loop.run_in_executor(
        None,
        partial(post_time_series, engine, infdblog, output_schema, table_name, hourly_dataframe)
    )


async def process_building(engine, infdblog, output_schema, table_name, objectid, row):
    """Process a single building with all its time series"""
    infdblog.debug(f"Processing building {objectid}")

    # Define all time series to insert
    time_series_configs = [
        {
            'name': 'ro_heat_indoor_temperature',
            'description': 'Indoor temperature for building',
            'unit': '°C',
            'column': 'indoor_temperature[C]'
        },
        {
            'name': 'ro_heat_heating_load',
            'description': 'Heating load for building',
            'unit': 'W',
            'column': 'heating:load[W]'
        },
        {
            'name': 'ro_heat_cooling_load',
            'description': 'Cooling load for building',
            'unit': 'W',
            'column': 'cooling:load[W]'
        }
    ]

    # Process all time series for this building concurrently
    tasks = []
    for config in time_series_configs:
        insert_metadata_sql = f"""
            INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
            VALUES ('{config['name']}',
                '{config['description']}',
                'synthetic',
                '{config['unit']}',
                0,
                '{objectid}',
                'ro-heat'
            )
            ON CONFLICT (id) DO NOTHING
            RETURNING id;
        """
        task = add_metadata_and_ts_async(
            engine, infdblog, output_schema, table_name,
            insert_metadata_sql, row, config['column']
        )
        tasks.append(task)

    await asyncio.gather(*tasks)


async def process_all_buildings(engine, infdblog, output_schema, table_name, dict_df, max_concurrent=10):
    """Process all buildings with controlled concurrency"""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def bounded_process(objectid, row):
        async with semaphore:
            await process_building(engine, infdblog, output_schema, table_name, objectid, row)

    tasks = [bounded_process(objectid, row) for objectid, row in dict_df.items()]
    await asyncio.gather(*tasks)

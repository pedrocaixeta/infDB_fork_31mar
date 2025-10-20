import os
import shutil
from . import config, utils, logger, bkg
import logging

import openmeteo_requests

import pandas as pd
import geopandas as gpd
import requests_cache
from retry_requests import retry

import numpy as np
import io
from pyproj import Transformer

log = logging.getLogger(__name__)

def temperature_2m(pd_dataframe, engine):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)   
    
    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"


    # Fast upload via COPY FROM (CSV stream) into a plain Postgres table
    db_schema = config.get_value(["loader", "sources", "openmeteo", "schema"])
    db_prefix = config.get_value(["loader", "sources", "openmeteo", "prefix"])

    # Drop metadata table if exists
    drop_metadata_sql = f"""
    DROP TABLE IF EXISTS {db_schema}.{db_prefix}_ts_metadata;
    """
    try:
        utils.sql_query(drop_metadata_sql)
    except Exception as e:
        log.error("Failed to drop metadata table: %s", e)
    
    # Create metadata table if not exists
    metadata_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.{db_prefix}_ts_metadata (
        id SERIAL PRIMARY KEY,
        name text,
        decription text,
        grid_id text,
        type text,
        resolution text,
        unit text,
        changelog integer,
        group_id text,
        geom geometry
    );
    """
    try:
        utils.sql_query(metadata_sql)
    except Exception as e:
        log.error("Failed to create metadata table: %s", e)
    
    
    table_name = 'openmeteo_ts_data'
    drop_sql = f"""
    DROP TABLE IF EXISTS {db_schema}.{table_name};
    """
    utils.sql_query(drop_sql)

    # Ensure table exists with appropriate types (grid_id, date, temperature, ts_id)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
        ts_metadata_id integer,
        date timestamptz,
        temperature_2m double precision
    );
    """
    utils.sql_query(create_sql)

    log.info(len(pd_dataframe))

    for batch in range(0, len(pd_dataframe), 100):
        # Process current batch of locations (max 100 at a time)
        batch_df = pd_dataframe.iloc[batch:batch+100]

        # Build comma-separated coordinate strings in WGS84 for this batch
        lat_str = ",".join(map(str, batch_df['latitude'].tolist()))
        lon_str = ",".join(map(str, batch_df['longitude'].tolist()))
        coordinates = {"latitude": lat_str, "longitude": lon_str}
        params = {
            "latitude": coordinates["latitude"],
            "longitude": coordinates["longitude"],
            "start_date": config.get_value(["loader", "sources", "openmeteo", "timing", "start_date"]),
            "end_date": config.get_value(["loader", "sources", "openmeteo", "timing", "end_date"]),
            "hourly": "temperature_2m",
        }

        responses = openmeteo.weather_api(url, params=params)
        # Process multiple locations
        for i, response in enumerate(responses):
            # response = responses[0]
            print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
            print(f"Elevation: {response.Elevation()} m asl")
            print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

            # Process hourly data. The order of variables needs to be the same as requested.
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

            hourly_data = {"date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = hourly.Interval()),
                inclusive = "left"
            )}

            grid_id = batch_df.iloc[i]['id']
            longitude = response.Longitude()
            latitude = response.Latitude()

            # Insert metadata record and get the auto-generated id
            insert_metadata_sql = f"""
            INSERT INTO {db_schema}.{db_prefix}_ts_metadata (name, decription, grid_id, type, resolution, unit, changelog, group_id, geom)
            VALUES ('{db_prefix}_hourly_temperature_2m',
                'Temperature from Open-Meteo at 2m height, hourly',
                '{grid_id}',
                'measurement:temperature_2m',
                '1 hour',
                '°C',
                0,
                'openmeteo',
                ST_SetSRID(ST_MakePoint({longitude},{latitude}), 4326)
                )
            ON CONFLICT (id) DO NOTHING
            RETURNING id;
            """
            ts_metadata_id = None
            try:
                with engine.begin() as conn:
                    result = pd.read_sql(insert_metadata_sql, con=conn)
                    if not result.empty:
                        ts_metadata_id = result['id'].iloc[0]
                        log.info("Created metadata record with id=%s", ts_metadata_id)
            except Exception as e:
                log.error("Failed to insert/retrieve metadata record: %s", e)
            
            hourly_data["temperature_2m"] = hourly_temperature_2m
            hourly_data["ts_metadata_id"] = ts_metadata_id
            hourly_dataframe = pd.DataFrame(data = hourly_data)

            try:

                # write CSV to an in-memory buffer without header/index
                # Reorder columns to match COPY target: grid_id, date, temperature_2m, ts_id
                buf = io.StringIO()
                hourly_dataframe[['ts_metadata_id', 'date', 'temperature_2m']].to_csv(buf, index=False, header=False)
                buf.seek(0)

                conn = engine.raw_connection()
                cur = conn.cursor()
                copy_sql = f"COPY {db_schema}.{table_name} (ts_metadata_id, date, temperature_2m) FROM STDIN WITH (FORMAT csv)"
                cur.copy_expert(copy_sql, buf)
                conn.commit()
                cur.close()
                conn.close()
                log.info("COPYed %d hourly rows to %s.%s (response %d/%d)", len(hourly_dataframe), db_schema, table_name, i+1, len(responses))
            except Exception as e:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
                log.error("Failed to COPY hourly data to Postgres: %s", e)


def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("openmeteo"):
        return

    # Create base directory for Open-Meteo data
    base_path = config.get_path(["loader", "sources", "openmeteo", "path", "base"])
    os.makedirs(base_path, exist_ok=True)

    # Make sure BKG grid cells with 10km resolution are available
    bkg.create_geogitter("10km")

    # Read centroid geometry (in WGS84) and numeric lat/lon columns
    engine = utils.get_db_engine("postgres")
    schema = config.get_value(["loader", "sources", "bkg", "schema"])
    table_name = config.get_value(["loader", "sources", "bkg", "geogitter", "table_name"])
    sql = f"""
        SELECT id,
               ST_Y(ST_Transform(ST_Centroid(geom), 4326)) AS latitude,
               ST_X(ST_Transform(ST_Centroid(geom), 4326)) AS longitude
        FROM {schema}.{table_name}
        WHERE name='DE_Grid_ETRS89_LAEA_10km'
    """
    pd_dataframe = pd.read_sql(sql=sql, con=engine)
    print(pd_dataframe.head())
    print(len(pd_dataframe))
    
    temperature_2m(pd_dataframe, engine)

    log.info(f"Openmeteo data loaded successfully")
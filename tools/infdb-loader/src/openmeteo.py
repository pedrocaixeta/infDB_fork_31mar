import os
import shutil
from . import config, utils, logger
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
                'hourly',
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

    base_path = config.get_path(["loader", "sources", "openmeteo", "path", "base"])
    os.makedirs(base_path, exist_ok=True)

    # # Geogitter
    log.info("Creating Geogitter layers")
    resolutions = config.get_value(
        ["loader", "sources", "openmeteo", "resolution", "spatial"]
    )
    schema = config.get_value(["loader", "sources", "openmeteo", "schema"])
    log.info(f"Creating Geogitter for resolution {resolutions}")
    epsg = utils.get_db_parameters("postgres")["epsg"]
    create_geogitter(resolutions, epsg, schema)

    engine = utils.get_db_engine("postgres")
    sql = f"""
        SELECT id,
               ST_Y(ST_Transform(ST_Centroid(geom), 4326)) AS latitude,
               ST_X(ST_Transform(ST_Centroid(geom), 4326)) AS longitude
        FROM {schema}.grid_cells
        WHERE name='DE_Grid_ETRS89_LAEA_10km'
    """

    # Read centroid geometry (in WGS84) and numeric lat/lon columns
    pd_dataframe = pd.read_sql(sql=sql, con=engine)
    print(pd_dataframe.head())
    print(len(pd_dataframe))
    
    temperature_2m(pd_dataframe, engine)

    log.info(f"Openmeteo data loaded successfully")


# def grid_midpoints_numpy(enevelop_geom, cell_size):
#     """Return grid midpoints as WGS84 lat/lon arrays.

#     enevelop_geom is expected to be a GeoDataFrame or GeoSeries with a CRS
#     convertible to EPSG:3035. We generate midpoints in the projected
#     coordinate system (meters), then transform to EPSG:4326 (WGS84) and
#     return a dict with numpy arrays 'latitude' and 'longitude'.
#     """
#     # bounds in EPSG:3035
#     xmin, ymin, xmax, ymax = enevelop_geom.to_crs(3035).total_bounds
#     x_coords = np.arange(xmin + cell_size / 2, xmax, cell_size)
#     y_coords = np.arange(ymin + cell_size / 2, ymax, cell_size)
#     xs, ys = np.meshgrid(x_coords, y_coords)

#     # flatten and transform from EPSG:3035 -> EPSG:4326 (WGS84)
#     x_flat = xs.ravel()
#     y_flat = ys.ravel()

#     transformer = Transformer.from_crs(3035, 4326, always_xy=True)
#     # transformer expects (x, y) -> (lon, lat)
#     lons, lats = transformer.transform(x_flat, y_flat)

#     coordinates = {
#         "latitude": ",".join(map(str, lats)),
#         "longitude": ",".join(map(str, lons))
#     }

#     return coordinates


def create_geogitter(resolutions, epsg, schema):
    """Create a grid_cells table containing cells for all requested resolutions.

    The function drops any existing table, then creates it using the first
    resolution and inserts additional resolutions' cells using INSERT INTO.
    """
    # Drop existing table if present
    sql = f"DROP TABLE IF EXISTS {schema}.grid_cells;"
    utils.sql_query(sql)

    envelop = utils.get_envelop()
    wkt = envelop.to_crs(3035).unary_union.wkt

    created = False
    for resolution in resolutions:
        if resolution.endswith("km"):
            resolution_meters = int(resolution[:-2]) * 1000
        elif resolution.endswith("m"):
            resolution_meters = int(resolution[:-1])
        else:
            # skip unknown formats
            log.warning("Skipping resolution with unknown unit: %s", resolution)
            continue

        # Build the SELECT statement that generates grid cells for one resolution
        select_sql = f"""
            WITH params AS (
                SELECT {resolution_meters}::int AS cell_size
            ),
            boundary AS (
                SELECT ST_GeomFromText('{wkt}', 3035) AS geom
            ),
            envelope AS (
                SELECT
                    FLOOR(ST_XMin(b.geom) / p.cell_size) * p.cell_size AS x_min,
                    FLOOR(ST_YMin(b.geom) / p.cell_size) * p.cell_size AS y_min,
                    CEIL(ST_XMax(b.geom) / p.cell_size) * p.cell_size AS x_max,
                    CEIL(ST_YMax(b.geom) / p.cell_size) * p.cell_size AS y_max,
                    p.cell_size
                FROM boundary b, params p
            ),
            grid_raw AS (
                SELECT (ST_SquareGrid(
                        e.cell_size,
                        ST_MakeEnvelope(e.x_min, e.y_min, e.x_max, e.y_max, 3035)
                        )).* 
                FROM envelope e
            ),
            grid AS (
                SELECT
                    ST_Transform(geom, {epsg}) AS geom,
                    ST_XMin(geom) AS x,
                    ST_YMin(geom) AS y
                FROM grid_raw
            ),
            id_named AS (
                SELECT
                    FORMAT('%sN%sE%s', '{resolution}', g.y::int::text, g.x::int::text) AS id,
                    (g.x + (p.cell_size / 2.0))::int AS x_mp,
                    (g.y + (p.cell_size / 2.0))::int AS y_mp,
                    'DE_Grid_ETRS89_LAEA_{resolution}'::text AS name,
                    p.cell_size::int AS resolution_meters,
                    g.geom
                FROM grid g, params p
            )
            SELECT * FROM id_named;
        """

        if not created:
            # Create table with first resolution
            sql = f"CREATE TABLE {schema}.grid_cells AS {select_sql}"
            created = True
        else:
            # Insert subsequent resolutions' cells
            sql = f"INSERT INTO {schema}.grid_cells {select_sql}"

        utils.sql_query(sql)

    
    # Create spatial index on geom column
    sql = f"CREATE INDEX IF NOT EXISTS building_geom_idx ON {schema}.grid_cells USING GIST (geom);"
    utils.sql_query(sql)
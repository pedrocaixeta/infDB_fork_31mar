import os

import numpy as np
import pandas as pd
from entise.core.generator import TimeSeriesGenerator
from infdb import InfDB

from src import basic_refurbishment, config
from src import rc_calculation
from src import timedata
import io
import asyncio
from functools import partial

# Parameters
rng = np.random.default_rng(seed=42)
end_of_simulation_year = 2025
construction_year_col = "construction_year"
schema = "ro_heat"


def post_time_series(engine, infdblog, output_schema, table_name, hourly_dataframe):
    # Insert time series data using auto generated ts_metadata_id
    try:
        # write CSV to an in-memory buffer without header/index
        # Reorder columns to match COPY target: grid_id, time, value, ts_id
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
        # infdblog.info("COPYed %d hourly rows to %s.%s (response %d/%d)", len(hourly_dataframe), output_schema, table_name, i+1, len(responses))

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

def main():
    # Load InfDB handler
    infdbhandler = InfDB(tool_name="ro-heat")

    # Database connection
    infdbclient_citydb = infdbhandler.connect('postgres')

    # Logger setup
    infdblog = infdbhandler.get_log()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # Setup database engine
    engine = infdbclient_citydb.get_db_engine()

    # Get configuration values
    input_schema = infdbhandler.get_config_value(["ro-heat", "data", "input_schema"])
    output_schema = infdbhandler.get_config_value(["ro-heat", "data", "output_schema"])

    try:

        sql = f"DROP SCHEMA IF EXISTS {output_schema} CASCADE;"
        infdbclient_citydb.execute_query(sql)
        sql = f"CREATE SCHEMA IF NOT EXISTS {output_schema};"
        infdbclient_citydb.execute_query(sql)
        infdblog.info(f"output schema: {output_schema} created successfully")

        # TODO: Refactor with InfdbClient method when available
        full_path = os.path.join("sql", "01_get_building_surface_data.sql")
        with open(full_path, "r", encoding="utf-8") as file:
            sql_content = file.read()
        format_params = {
            'input_schema': input_schema,
        }
        sql_content = sql_content.format(**format_params)
        buildings = pd.read_sql(sql_content, engine)

        infdblog.debug(f"Loaded {len(buildings)} buildings from the database.")
        infdblog.debug(buildings.head())

        buildings[construction_year_col] = basic_refurbishment.sample_construction_year(buildings,
                                                                                        end_of_simulation_year,
                                                                                        construction_year_col, rng)

        refurbishment_parameters = {
            "outer_wall": {
                "distribution": lambda gen, parameters: gen.normal(**parameters),
                "distribution_parameters": {"loc": 40, "scale": 10},
            },
            "rooftop": {
                "distribution": lambda gen, parameters: gen.normal(**parameters),
                "distribution_parameters": {"loc": 50, "scale": 10},
            },
            "window": {
                "distribution": lambda gen, parameters: gen.normal(**parameters),
                "distribution_parameters": {"loc": 30, "scale": 10},
            },
        }

        infdblog.debug("Starting refurbishment simulation")
        refurbed_df = basic_refurbishment.simulate_refurbishment(
            buildings,
            end_of_simulation_year,
            refurbishment_parameters,
            rng,
            age_column=construction_year_col,
            provide_last_refurb_only=True,
        )
        infdblog.debug("Refurbishment simulation completed")
        infdblog.debug(refurbed_df.info())
        infdblog.debug(refurbed_df.head())

        infdbclient_citydb.execute_query("DROP TABLE IF EXISTS ro_heat.buildings_rc CASCADE")
        refurbed_df.to_sql(
            "buildings_rc", engine, if_exists="replace", schema=schema, index=False
        )
        infdblog.debug("Refurbished data writing to database")

        infdblog.debug("Starting construction of building elements")
        # Run SQL: 02_create_layer_view
        infdbclient_citydb.execute_sql_files("sql", ["02_create_layer_view.sql"])

        elements = pd.read_sql("""SELECT *
                                  FROM v_element_layer_data""", engine)

        # TODO: sort by layer_index according to EUReCA specification
        # TODO: Handling of windows
        infdblog.debug("Starting construction of building elements")
        elements = elements[elements["element_name"] != "Window"]

        rc_values = rc_calculation.calculate_rc_values(elements)

        bld2ts = timedata.get_bld2ts(database_connection=engine)

        all_ts_df = timedata.get_all_timeseries_data(database_connection=engine)
        all_ts_df.index.name = 'datetime'
        all_ts_df.rename(columns={"value": "air_temperature[C]"}, inplace=True)
        all_ts_df = all_ts_df.reset_index()
        data = {x: y.sort_index() for x, y in all_ts_df.groupby('ts_metadata_id')}

        # Preparation for EnTiSe
        entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
        entise_input = entise_input.rename(columns={'resistance': 'resistance[K W-1]', 'capacitance': 'capacitance[J K-1]'}, errors=True)
        entise_input["hvac"] = "1R1C"
        entise_input["min_temperature[C]"] = 20.0
        entise_input["max_temperature[C]"] = 24.0
        entise_input["gains_solar"] = 0.0
        

        entise_input = entise_input.merge(
            bld2ts[['bld_objectid', 'ts_metadata_id']].rename(columns={"ts_metadata_id": "weather"}),
            left_on="id",
            right_on="bld_objectid",
            how="left",
        ).drop(columns=["bld_objectid"])
        
        # For testing purposes, limit to one building
        entise_input = entise_input.iloc[:1, :]

        # Initialize the generator
        gen = TimeSeriesGenerator()
        gen.add_objects(entise_input)

        # Generate time series and summary
        summary, dict_df = gen.generate(data, workers=os.cpu_count())

        # Summary
        summary.index.name = "building_objectid"
        summary.to_sql(
            "entise_summary",
            con=engine,
            if_exists="replace",
            schema=output_schema,
            index=True,
            method="multi",
            # chunksize=1000,
        )

        infdblog.info(summary.head())

        # Time Series
        infdblog.debug("Writing EnTiSe output time series to database")

        # Create metadata table if not exists
        metadata_sql = f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.entise_ts_metadata (
            id SERIAL PRIMARY KEY,
            name text,
            decription text,
            grid_id text,
            type text,
            unit text,
            changelog integer,
            objectid text,
            source text
        );
        """
        try:
            infdbclient_citydb.execute_query(metadata_sql)
        except Exception as e:
            infdblog.error("Failed to create metadata table: %s", e)
            
        # Ensure table exists with appropriate types (grid_id, time, temperature, ts_id)

        table_name = 'entise_ts_data'

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} (
            ts_metadata_id integer,
            time timestamptz,
            value double precision
        )
        WITH (
            timescaledb.hypertable,
            timescaledb.partition_column='time',
            timescaledb.segmentby='ts_metadata_id'
        );
        """
        try:
            infdbclient_citydb.execute_query(create_sql)
        except Exception as e:
            infdblog.error("Failed to create timeseries table: %s", e)


        for objectid, row in dict_df.items():
            infdblog.debug(f"Processing building {objectid}")
           
            # Insert indoor temperature
            insert_metadata_sql = f"""
                    INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
                    VALUES ('ro_heat_indoor_temperature',
                        'Indoor temperature for building',
                        'synthetic',
                        '°C',
                        0,
                        '{objectid}',
                        'ro-heat'
                        )
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id;
                    """
            add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, 'indoor_temperature[C]')
           
            # Insert heating load
            insert_metadata_sql = f"""
                    INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
                    VALUES ('ro_heat_heating_load',
                        'Heating load for building',
                        'synthetic',
                        'W',
                        0,
                        '{objectid}',
                        'ro-heat'
                        )
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id;
                    """
            add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, 'heating:load[W]')
           
            # Insert cooling load
            insert_metadata_sql = f"""
                    INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
                    VALUES ('ro_heat_cooling_load',
                        'Cooling load for building',
                        'synthetic',
                        'W',
                        0,
                        '{objectid}',
                        'ro-heat'
                        )
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id;
                    """
            add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, 'cooling:load[W]')
        
        
        infdblog.info("Ro-heat sucessfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

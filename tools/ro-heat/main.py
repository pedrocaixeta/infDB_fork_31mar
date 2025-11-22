import numpy as np
import pandas as pd
from infdb import InfDB
from entise.core.generator import TimeSeriesGenerator

from src import basic_refurbishment
from src import eureca_code
from src import timedata
import os
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


def main():
    # Instantiate new InfDB facade (single entry point)
    infdbhandler = InfDB(tool_name="ro-heat")

    # Logger
    infdblog = infdbhandler.get_log()
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # DB client and engine (both via the facade)
    infdbclient_citydb = infdbhandler.connect()
    engine = infdbhandler.get_db_engine()
    input_schema = infdbhandler.get_config_value([infdbhandler.get_toolname(), "data", "input_schema"])
    output_schema = infdbhandler.get_config_value([infdbhandler.get_toolname(), "data", "output_schema"])

    try:
        sql = f"DROP SCHEMA IF EXISTS {output_schema} CASCADE;"
        infdbclient_citydb.execute_query(sql)
        sql = f"CREATE SCHEMA IF NOT EXISTS {output_schema};"
        infdbclient_citydb.execute_query(sql)
        infdblog.info(f"output schema: {output_schema} created successfully")

        SQL_QUERY = f"""
                    DROP TABLE IF EXISTS {output_schema}.temp_rc_calculation CASCADE;

                    CREATE TABLE {output_schema}.temp_rc_calculation AS
                    WITH wall_data AS (SELECT building_objectid,
                                              SUM(area) AS wall_surface_area
                                       FROM (SELECT regexp_replace(f.objectid, '_[^_]*-.*$', '') AS building_objectid,
                                                    CAST(p.val_string AS double precision)       AS area
                                             FROM feature f
                                                      JOIN geometry_data gd ON f.id = gd.feature_id
                                                      JOIN property p ON gd.feature_id = p.feature_id
                                             WHERE f.objectclass_id = 709 -- WallSurface
                                               AND p.name = 'Flaeche') sub
                                       GROUP BY building_objectid),
                         roof_data AS (SELECT building_objectid,
                                              SUM(area) AS roof_surface_area
                                       FROM (SELECT regexp_replace(f.objectid, '_[^_]*-.*$', '') AS building_objectid,
                                                    CAST(p.val_string AS double precision)       AS area
                                             FROM feature f
                                                      JOIN geometry_data gd ON f.id = gd.feature_id
                                                      JOIN property p ON gd.feature_id = p.feature_id
                                             WHERE f.objectclass_id = 712 -- RoofSurface
                                               AND p.name = 'Flaeche') sub
                                       GROUP BY building_objectid)

                    SELECT b.objectid                                                            AS building_objectid,
                           b.floor_area,
                           b.floor_number,
                           b.building_type,
                           b.construction_year,
                           -- Reduce wall surface by the assumed window area, see below
                           wd.wall_surface_area - b.floor_area * b.floor_number * 0.75 * 0.2 AS wall_area,
                           rd.roof_surface_area                                              AS roof_area,
                           -- Assume heated area = b.floor_area * b.floor_number * 0.75
                           -- Assume window area to be 0.2 m² per heated area ()
                           b.floor_area * b.floor_number * 0.75 * 0.2                        AS window_area
                    FROM {input_schema}.buildings b
                             LEFT JOIN wall_data wd ON b.objectid = wd.building_objectid
                             LEFT JOIN roof_data rd ON b.objectid = rd.building_objectid;

                    SELECT *
                    from {output_schema}.temp_rc_calculation
                    WHERE building_type IS NOT NULL \
                    """

        buildings = pd.read_sql(SQL_QUERY, engine)

        infdblog.debug(f"Loaded {len(buildings)} buildings from the database.")
        infdblog.debug(buildings.head())

        random_years = np.full(len(buildings), np.nan)

        # Define class-to-range mapping
        age_class_ranges = {
            "-1919": (1860, 1918),
            "1919-1948": (1919, 1948),
            "1949-1978": (1949, 1978),
            "1979-1990": (1979, 1990),
            "1991-2000": (1991, 2000),
            "2001-2010": (2001, 2010),
            "2011-2019": (2011, 2019),
            "2020-": (2020, end_of_simulation_year),
        }

        # For each class, find matching rows and assign random years
        for age_class, (start, end) in age_class_ranges.items():
            mask = buildings[construction_year_col] == age_class
            count = sum(mask)
            random_years[mask] = rng.integers(start, end, size=count, endpoint=True)

        buildings[construction_year_col] = random_years.astype(int)

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

        elements = pd.read_sql("""SELECT * FROM v_element_layer_data""", engine)

        # TODO: sort by layer_index according to EUReCA specification
        # TODO: Handling of windows
        infdblog.debug("Starting construction of building elements")
        elements = elements[elements["element_name"] != "Window"]

        elements["materials"] = elements.apply(
            lambda x: eureca_code.Material(
                x["name"], x["thickness"], x["thermal_conduc"], x["heat_capac"], x["density"]
            ),
            axis=1,
        )

        constructions = (
            elements.groupby(["building_objectid", "element_name", "area"])["materials"]
            .apply(list)
            .reset_index()
        )

        # Map tabula to EUReCA names
        tabula_eureca_element_name_mapping = {
            "GroundFloor": "GroundFloor",
            "OuterWall": "ExtWall",
            "Rooftop": "Roof",
        }

        constructions["construction_obj"] = constructions.apply(
            lambda row: eureca_code.Construction(
                name=f"B{row['building_objectid']}_{row['element_name']}",
                materials_list=row["materials"],
                construction_type=tabula_eureca_element_name_mapping[row["element_name"]],
            ),
            axis=1,
        )

        constructions["resistance"] = constructions.apply(
            lambda row: 1 / ((1 / row["construction_obj"].thermal_resistance) * row["area"]),
            axis=1,
        )

        constructions["capacitance"] = constructions.apply(
            lambda row: row["construction_obj"].k_int * row["area"], axis=1
        )

        rc_values = (
            constructions.groupby("building_objectid")[["capacitance", "resistance"]].sum().sort_values("building_objectid")
        )

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
        # entise_input = entise_input.iloc[:1, :]

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
        )
        infdblog.info(summary.head())

        # # Time Series
        # infdblog.debug("Writing EnTiSe output time series to database")

        # # Create metadata table if not exists
        # metadata_sql = f"""
        # CREATE TABLE IF NOT EXISTS {output_schema}.entise_ts_metadata (
        #     id SERIAL PRIMARY KEY,
        #     name text,
        #     decription text,
        #     grid_id text,
        #     type text,
        #     unit text,
        #     changelog integer,
        #     objectid text,
        #     source text
        # );
        # """
        # try:
        #     infdbclient_citydb.execute_query(metadata_sql)
        # except Exception as e:
        #     infdblog.error("Failed to create metadata table: %s", e)

        # # Ensure table exists with appropriate types (grid_id, time, temperature, ts_id)
        # table_name = 'entise_ts_data'
        # create_sql = f"""
        # CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} (
        #     ts_metadata_id integer,
        #     time timestamptz,
        #     value double precision
        # )
        # WITH (
        #     timescaledb.hypertable,
        #     timescaledb.partition_column='time',
        #     timescaledb.segmentby='ts_metadata_id'
        # );
        # """
        # try:
        #     infdbclient_citydb.execute_query(create_sql)
        # except Exception as e:
        #     infdblog.error("Failed to create timeseries table: %s", e)

        # for objectid, row in dict_df.items():
        #     infdblog.debug(f"Processing building {objectid}")

        #     # Insert indoor temperature
        #     insert_metadata_sql = f"""
        #             INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
        #             VALUES ('ro_heat_indoor_temperature',
        #                 'Indoor temperature for building',
        #                 'synthetic',
        #                 '°C',
        #                 0,
        #                 '{objectid}',
        #                 'ro-heat'
        #                 )
        #             ON CONFLICT (id) DO NOTHING
        #             RETURNING id;
        #             """
        #     add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, 'indoor_temperature[C]')

        #     # Insert heating load
        #     insert_metadata_sql = f"""
        #             INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
        #             VALUES ('ro_heat_heating_load',
        #                 'Heating load for building',
        #                 'synthetic',
        #                 'W',
        #                 0,
        #                 '{objectid}',
        #                 'ro-heat'
        #                 )
        #             ON CONFLICT (id) DO NOTHING
        #             RETURNING id;
        #             """
        #     add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, 'heating:load[W]')

        #     # Insert cooling load
        #     insert_metadata_sql = f"""
        #             INSERT INTO {output_schema}.entise_ts_metadata (name, decription, type, unit, changelog, objectid, source)
        #             VALUES ('ro_heat_cooling_load',
        #                 'Cooling load for building',
        #                 'synthetic',
        #                 'W',
        #                 0,
        #                 '{objectid}',
        #                 'ro-heat'
        #                 )
        #             ON CONFLICT (id) DO NOTHING
        #             RETURNING id;
        #             """
        #     add_metadata_and_ts(engine, infdblog, output_schema, table_name, insert_metadata_sql, row, 'cooling:load[W]')

        infdblog.info("Ro-heat sucessfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

import os
import time

import numpy as np
import pandas as pd
# entise package has to type stubs
from entise.core.generator import TimeSeriesGenerator  # type: ignore
from infdb import InfDB

from src import refurbishment, rc_calculation, timedata

# Parameters
rng = np.random.default_rng(seed=42)
simulation_year = 2024
construction_year_col = "construction_year"


def main():
    # Load InfDB handler
    infdbhandler = InfDB(tool_name="ro-heat", config_path="configs")

    # Database connection
    infdbclient_citydb = infdbhandler.connect("postgres")

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
            "input_schema": input_schema,
        }
        sql_content = sql_content.format(**format_params)
        buildings = pd.read_sql(sql_content, engine)

        infdblog.debug(f"Loaded {len(buildings)} buildings from the database.")
        infdblog.debug(buildings.head())

        buildings[construction_year_col] = refurbishment.sample_construction_year(buildings, simulation_year,
                                                                                  construction_year_col, rng)

        refurbishment_simulation_parameters = {
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
        refurbed_df = refurbishment.simulate_refurbishment(
            buildings,
            simulation_year,
            refurbishment_simulation_parameters,
            rng,
            age_column=construction_year_col,
        )
        infdblog.debug("Refurbishment simulation completed")
        infdblog.debug(refurbed_df.info())
        infdblog.debug(refurbed_df.head())

        refurbishment_quotas = {
            "outer_wall": {
                "refurbed_fraction": 0.33,
            },
            "rooftop": {
                "refurbed_fraction": 0.63,
            },
            "window": {
                "refurbed_fraction": 0.9,
            },
        }

        infdblog.debug("Starting harmonization with refurbishment quotas")
        harmonized_df = refurbishment.harmonize_with_quota(refurbed_df, refurbishment_quotas, rng, infdblog,
                                                           age_column=construction_year_col, )
        infdblog.debug(harmonized_df.info())
        infdblog.debug("Harmonization with refurbishment quotas completed")

        infdblog.debug("Writing harmonized refurbishment data to database")
        infdbclient_citydb.execute_query("DROP TABLE IF EXISTS ro_heat.buildings_rc CASCADE")
        harmonized_df.to_sql("buildings_rc", engine, if_exists="replace", schema=output_schema, index=False)

        infdblog.debug("Starting construction of building elements")
        # Run SQL: 02_create_layer_view
        infdbclient_citydb.execute_sql_files("sql", ["02_create_layer_view.sql"])

        elements = pd.read_sql(
            """SELECT *
               FROM v_element_layer_data""",
            engine,
        )

        # TODO: sort by layer_index according to EUReCA specification
        # TODO: Handling of windows
        infdblog.debug("Starting construction of building elements")
        elements = elements[elements["element_name"] != "Window"]

        rc_values = rc_calculation.calculate_rc_values(elements)

        bld2ts = timedata.get_bld2ts(database_connection=engine)

        all_ts_df = timedata.get_all_timeseries_data(
            database_connection=engine,
            start=pd.Timestamp(f"{simulation_year}-01-01"),
            end=pd.Timestamp(f"{simulation_year}-12-31"),
        )
        all_ts_df.index.name = "datetime"
        all_ts_df.rename(columns={"value": "air_temperature[C]"}, inplace=True)
        data = {x: y.sort_index().reset_index() for x, y in all_ts_df.groupby("ts_metadata_id")}

        # Preparation for EnTiSe
        entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
        entise_input = entise_input.rename(
            columns={"resistance": "resistance[K W-1]", "capacitance": "capacitance[J K-1]"}, errors=True
        )
        entise_input["hvac"] = "1R1C"
        entise_input["min_temperature[C]"] = 20.0
        entise_input["max_temperature[C]"] = 24.0
        entise_input["gains_solar"] = 0.0

        entise_input = entise_input.merge(
            bld2ts[["bld_objectid", "ts_metadata_id"]].rename(columns={"ts_metadata_id": "weather"}),
            left_on="id",
            right_on="bld_objectid",
            how="left",
        ).drop(columns=["bld_objectid"])

        # For testing purposes, limit to 500 buildings
        entise_input = entise_input.iloc[:500, :]

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

        # Time Series
        write_timeseries = False
        if not write_timeseries:
            infdblog.info("Skipping EnTiSe output time series writing to database as per configuration")
            return

        infdblog.debug("Writing EnTiSe output time series to database")

        # Create metadata table if not exists
        metadata_sql = f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.entise_ts_metadata (
            id SERIAL PRIMARY KEY,
            name text,
            description text,
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

        # Ensure unique constraint for metadata upserts
        try:
            unique_idx_sql = f"""
            CREATE UNIQUE INDEX IF NOT EXISTS entise_ts_metadata_uniq
            ON {output_schema}.entise_ts_metadata (name, objectid, source);
            """
            infdbclient_citydb.execute_query(unique_idx_sql)
        except Exception as e:
            infdblog.error("Failed to create unique index on metadata: %s", e)

        # Ensure table exists with appropriate types (grid_id, time, temperature, ts_id)

        table_name = "entise_ts_data"

        # Try to ensure TimescaleDB extension is available (best-effort)
        try:
            infdbclient_citydb.execute_query("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        except Exception as e:
            infdblog.warning("Could not ensure timescaledb extension (continuing): %s", e)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} (
            ts_metadata_id integer,
            time timestamptz,
            value double precision
        )
        WITH (
            timescaledb.hypertable,
            timescaledb.partition_column="time",
            timescaledb.segmentby="ts_metadata_id"
        );
        """
        try:
            infdbclient_citydb.execute_query(create_sql)
        except Exception as e:
            infdblog.error("Failed to create timeseries table with Timescale hypertable syntax: %s", e)
            # Fallback: create as a plain Postgres table
            try:
                create_sql_plain = f"""
                CREATE TABLE IF NOT EXISTS {output_schema}.{table_name} (
                    ts_metadata_id integer,
                    time timestamptz,
                    value double precision
                );
                """
                infdbclient_citydb.execute_query(create_sql_plain)
                infdblog.info("Created plain Postgres timeseries table as fallback (no TimescaleDB features).")
            except Exception as e2:
                infdblog.error("Failed to create plain timeseries table: %s", e2)
                raise

        # Upload using baseline
        upload_start = time.perf_counter()
        timedata.upload_timeseries_baseline(
            engine=engine,
            output_schema=output_schema,
            table_name=table_name,
            dict_df=dict_df,
            infdblog=infdblog,
        )
        upload_dt = time.perf_counter() - upload_start
        infdblog.info(f"Baseline batch upload completed in {upload_dt:.2f} seconds")
        print(f"Baseline batch upload completed in {upload_dt:.2f} seconds")

        infdblog.info("Ro-heat successfully completed")
        infdbhandler.stop_logger()

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        infdbhandler.stop_logger()
        raise e


if __name__ == "__main__":
    main()

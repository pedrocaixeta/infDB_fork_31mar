import os
import time

import numpy as np
import pandas as pd
# entise package has to type stubs
from entise.core.generator import TimeSeriesGenerator  # type: ignore
from infdb import InfDB
from timedata import write_ts_data

from src import refurbishment, rc_calculation, timedata

# Parameters
construction_year_col = "construction_year"


def main():
    # Load InfDB handler
    infdbhandler = InfDB(tool_name="ro-heat", config_path="configs")
    ags = infdbhandler.get_env_variable("AGS")

    # Database connection
    infdbclient_citydb = infdbhandler.connect()

    # Logger setup
    infdblog = infdbhandler.get_logger()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")
    infdblog.info("AGS environment variable: %s", ags)

    # Setup database engine
    engine = infdbclient_citydb.get_db_engine()

    # Get configuration values
    input_schema = infdbhandler.get_config_value(["ro-heat", "data", "input", "schema"])
    output_schema = infdbhandler.get_config_value(["ro-heat", "data", "output", "schema"])

    random_seed = infdbhandler.get_config_value(["ro-heat", "data", "input", "random_seed"])
    rng = np.random.default_rng(seed=random_seed)

    simulation_year = infdbhandler.get_config_value(["ro-heat", "data", "input", "simulation_year"])

    refurbishment_config = infdbhandler.get_config_value(["ro-heat", "data", "refurbishment"])

    method = infdbhandler.get_config_value(["ro-heat", "data", "input", "method"])

    try:
        # sql = f"DROP SCHEMA IF EXISTS {output_schema} CASCADE;"
        # infdbclient_citydb.execute_query(sql)
        sql = f"CREATE SCHEMA IF NOT EXISTS {output_schema};"
        infdbclient_citydb.execute_query(sql)
        infdblog.info(f"output schema: {output_schema} created successfully")

        # TODO: Refactor with InfdbClient method when available
        full_path = os.path.join("sql", "01_get_building_surface_data.sql")
        with open(full_path, "r", encoding="utf-8") as file:
            sql_content = file.read()
        format_params = {
            "ags": ags,
            "input_schema": input_schema,
        }
        sql_content = sql_content.format(**format_params)
        buildings = pd.read_sql(sql_content, engine)

        infdblog.debug(f"Loaded {len(buildings)} buildings from the database.")
        infdblog.debug(buildings.head())

        buildings[construction_year_col] = refurbishment.sample_construction_year(buildings, simulation_year,
                                                                                  construction_year_col, rng)

        refurbishment_simulation_parameters = {n: {"distribution": lambda gen, parameters: gen.normal(**parameters),
                                                   "distribution_parameters": {"loc": i['lifespan_mean'],
                                                                               "scale": i['lifespan_spread']}, } for
                                               n, i in refurbishment_config.items()}

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

        refurbishment_quotas = {n: {"refurbed_fraction": i['quota']} for n, i in refurbishment_config.items()}

        infdblog.debug("Starting harmonization with refurbishment quotas")
        harmonized_df = refurbishment.harmonize_with_quota(refurbed_df, refurbishment_quotas, rng, infdblog,
                                                           age_column=construction_year_col, )
        infdblog.debug(harmonized_df.info())
        infdblog.debug("Harmonization with refurbishment quotas completed")

        infdblog.debug("Writing harmonized refurbishment data to database")
        infdbclient_citydb.execute_query("DROP TABLE IF EXISTS ro_heat.buildings_refurbished_status CASCADE")
        harmonized_df.to_sql("buildings_refurbished_status", engine, if_exists="replace", schema=output_schema,
                             index=False)

        infdblog.debug("Starting construction of building elements")
        # Run SQL: 02_create_layer_view
        infdbclient_citydb.execute_sql_files("sql", ["02_create_layer_view.sql"])

        infdblog.debug("Fetching building elements from database")
        elements = pd.read_sql(
            """SELECT *
               FROM v_element_layer_data
               JOIN opendata.buildings_lod2 bld2
                ON v_element_layer_data.building_objectid = bld2.objectid
                WHERE bld2.gemeindeschluessel LIKE %s""",
            engine,
            params=(f"{ags}%",),
        )

        # TODO: sort by layer_index according to EUReCA specification
        infdblog.debug("Starting construction of building elements")

        rc_values = rc_calculation.calculate_rc_values(elements)
        rc_values.to_sql(
            "buildings_rc",
            con=engine,
            if_exists="replace",
            schema=output_schema,
            index=True,
            method="multi",
        )

        bld2ts = timedata.get_bld2ts(database_connection=engine)

        all_ts_df = timedata.get_all_timeseries_data(
            database_connection=engine,
            start=pd.Timestamp(f"{simulation_year}-01-01"),
            end=pd.Timestamp(f"{simulation_year}-12-31"),
        )
        all_ts_df.index.name = "datetime"
        all_ts_df.rename(columns={"value": "air_temperature[C]"}, inplace=True)
        data = {x: y.sort_index().reset_index() for x, y in all_ts_df.groupby("ts_metadata_id")}

        if method == "1R0C":
            # TODO: Implement and set summary
            raise NotImplementedError()
        elif method == "1R1C":
            # Preparation for EnTiSe
            entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
            entise_input = entise_input.rename(
                columns={"resistance": "resistance[K W-1]", "capacitance": "capacitance[J K-1]"}, errors=True
            )
            entise_input["hvac"] = method
            entise_input["min_temperature[C]"] = 20.0
            entise_input["max_temperature[C]"] = 24.0
            entise_input["gains_solar"] = 0.0

            entise_input = entise_input.merge(
                bld2ts[["bld_objectid", "ts_metadata_id"]].rename(columns={"ts_metadata_id": "weather"}),
                left_on="id",
                right_on="bld_objectid",
                how="left",
            ).drop(columns=["bld_objectid"])

            # Initialize the generator
            gen = TimeSeriesGenerator()
            gen.add_objects(entise_input)

            # Generate time series and summary
            summary, dict_df = gen.generate(data, workers=os.cpu_count())
        else:
            raise ValueError("Method must be 1R0C or 1R1C")

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

        write_ts_data(dict_df, engine, infdbclient_citydb, infdbhandler, infdblog, output_schema)

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        infdbhandler.stop_logger()
        raise e


if __name__ == "__main__":
    main()

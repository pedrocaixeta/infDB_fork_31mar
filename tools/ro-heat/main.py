import os

import numpy as np
import pandas as pd
# entise package has to type stubs
from entise.core.generator import TimeSeriesGenerator  # type: ignore
from infdb import InfDB
from timedata import write_ts_data

from src import refurbishment, timedata, tabula_handling

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
        full_path = os.path.join("sql", "02_get_tabula_elements.sql")
        with open(full_path, "r", encoding="utf-8") as file:
            sql_content = file.read()
        tabula_elements = pd.read_sql(sql_content, engine)

        tabula_structure = tabula_handling.create_tabula_structure(tabula_elements)

        # TODO: Remove if AGS handling is in place
        harmonized_df = harmonized_df[harmonized_df['building_objectid'].str.startswith('DEBY')]

        harmonized_df[['resistance', 'capacitance']] = harmonized_df.apply(
            lambda row: tabula_handling.calculate_rc_values(tabula_structure, row), axis=1,
            result_type="expand")
        infdblog.debug("Done with construction of building elements")

        infdblog.debug("Writing R & C values")
        rc_values = harmonized_df[['building_objectid', 'resistance', 'capacitance']]
        rc_values.to_sql(
            "buildings_rc",
            con=engine,
            if_exists="replace",
            schema=output_schema,
            index=False,
            method="multi",
        )
        infdblog.debug("Done writing R & C values")
        infdblog.debug(f"Running heat demand estimation with method {method}")

        start_time = f"{simulation_year}-01-01"
        end_time = f"{simulation_year}-12-31"
        heating_setpoint = 20.0

        if method == "1R0C":
            full_path = os.path.join("sql", "heat-demand-r.sql")
            with open(full_path, "r", encoding="utf-8") as file:
                sql_content = file.read()
            format_params = {
                "ags": ags,
                "start_time": start_time,
                "end_time": end_time,
                "temp_in": heating_setpoint
            }
            sql_content = sql_content.format(**format_params)
            heating_demands = pd.read_sql(sql_content, engine)

            # Summary
            # TODO: Adapt output format to EnTiSe format
            heating_demands.index.name = "building_objectid"
            heating_demands.to_sql(
                "1R0C_summary",
                con=engine,
                if_exists="replace",
                schema=output_schema,
                index=False,
                method="multi",
            )

        elif method == "1R1C":

            bld2ts = timedata.get_bld2ts(database_connection=engine)

            all_ts_df = timedata.get_all_timeseries_data(
                database_connection=engine,
                start=pd.Timestamp(start_time),
                end=pd.Timestamp(end_time),
            )
            all_ts_df.index.name = "datetime"
            all_ts_df.rename(columns={"value": "air_temperature[C]"}, inplace=True)
            data = {x: y.sort_index().reset_index() for x, y in all_ts_df.groupby("ts_metadata_id")}

            # Preparation for EnTiSe
            entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
            entise_input = entise_input.rename(
                columns={"resistance": "resistance[K W-1]", "capacitance": "capacitance[J K-1]"}, errors=True
            )
            entise_input["hvac"] = method
            entise_input["min_temperature[C]"] = heating_setpoint
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
        else:
            raise ValueError("Method must be 1R0C or 1R1C")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        infdbhandler.stop_logger()
        raise e


if __name__ == "__main__":
    main()

import os

import numpy as np
import pandas as pd
from entise.core.generator import TimeSeriesGenerator
from infdb import InfDB

from src import basic_refurbishment
from src import rc_calculation

# Parameters
rng = np.random.default_rng(seed=42)
end_of_simulation_year = 2025
construction_year_col = "construction_year"
schema = "ro_heat"


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

        # Preparation for EnTiSe
        entise_input = rc_values.reset_index().rename(columns={"building_objectid": "id"})
        entise_input["hvac"] = "1R1C"
        entise_input["temp_min"] = 20.0
        entise_input["temp_max"] = 24.0

        # Initialize the generator
        gen = TimeSeriesGenerator()
        gen.add_objects(entise_input)

        # TODO: Adapt datetime range and temp_out
        data = {
            "weather": pd.DataFrame(
                {
                    "temp_out": [0.0] * 24,
                    "datetime": pd.date_range("2025-01-01", periods=24, freq="h"),
                }
            )
        }

        # Generate time series
        # TODO: Handle and save time series
        summary, df = gen.generate(data)

        summary.index.name = "building_objectid"

        summary.to_sql(
            "entise_output",
            con=engine,
            if_exists="replace",
            schema=output_schema,
            index=True,
            method="multi",
            # chunksize=1000,
        )

        infdblog.info(summary.head())

        infdblog.info("Ro-heat sucessfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

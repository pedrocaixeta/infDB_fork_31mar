import numpy as np
import pandas as pd
from infdb import InfDB
from entise.core.generator import TimeSeriesGenerator

from src import basic_refurbishment
from src import eureca_code

# Parameters
rng = np.random.default_rng(seed=42)
end_of_simulation_year = 2025
construction_year_col = "construction_year"
schema = "ro_heat"


def main():

    # Load InfDB handler
    infdbhandler = InfDB(tool_name="ro-heat")

    # Database connection
    infdbclient_citydb = infdbhandler.connect()

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
                    FROM {input_schema}.buildings_pylovo b
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

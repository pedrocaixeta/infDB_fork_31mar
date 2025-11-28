
"""
Main entry point for the linear-heat-density tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
import os
from infdb import InfDB
from src import demo, linear_heat_density


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and runs the demo function. Handles exceptions and logs errors.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="linear-heat-density")

    # Start message
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:
        # ===========================================================
        # Start your added python code in folder "src"
        # ===========================================================
        infdb.log.info("Running python code ...")
        linear_heat_density.example_function(variable="Hello, InfDB!")

        # ===========================================================
        # Start your added sql scripts in folder "sql"
        # ===========================================================
        infdb.log.info("Running SQL scripts ...")
        format_params = {
            'input_streets_schema': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "schema"]),
            'input_streets_table': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "table"]),
            'input_streets_id': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "id_column"]),
            'input_streets_geom': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "geom_column"]),
            'input_buildings_schema': infdb.get_config_value([infdb.get_toolname(), "data", "input", "buildings", "schema"]),
            'input_buildings_table': infdb.get_config_value([infdb.get_toolname(), "data", "input", "buildings", "table"]),
            'input_buildings_id': infdb.get_config_value([infdb.get_toolname(), "data", "input", "buildings", "id_column"]),
            'input_buildings_geom': infdb.get_config_value([infdb.get_toolname(), "data", "input", "buildings", "geom_column"]),
            'output_schema': infdb.get_config_value([infdb.get_toolname(), "data", "output", "schema"]),
            'output_table': infdb.get_config_value([infdb.get_toolname(), "data", "output", "table"]),
        }
        SQL_DIR = os.path.join("sql")   # add subfolders here if needed ("sql/subfolder")
        infdb.connect().execute_sql_files(SQL_DIR, format_params=format_params)

        # ===========================================================
        # Demonstrate database querying - remove or comment out if not needed
        # ===========================================================
        infdb.log.info("Running demo ...")
        demo.sql_demo(infdb)
        demo.database_demo(infdb)
        demo.database_demo_sqlalchemy()

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()
    
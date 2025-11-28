
"""
Main entry point for the linear-heat-density tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
import os
from infdb import InfDB


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
        # Start your added sql scripts in folder "sql"
        # ===========================================================
        infdb.log.info("Running SQL scripts ...")
        format_params = {
            'buildings_to_streets_schema': infdb.get_config_value([infdb.get_toolname(), "data", "input", "buildings-to-streets", "schema"]),
            'buildings_to_streets_table': infdb.get_config_value([infdb.get_toolname(), "data", "input", "buildings-to-streets", "table"]),
            'streets_schema': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "schema"]),
            'streets_table': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "table"]),
            'streets_id': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "id-column"]),
            'streets_geom': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "geom-column"]),
            'heat_demand_schema': infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "schema"]),
            'heat_demand_table': infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "table"]),
            'heat_demand_id': infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "id-column"]),
            'heat_demand_column': infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "heat-demand-column"]),
            'output_schema': infdb.get_config_value([infdb.get_toolname(), "data", "output", "schema"]),
            'output_table': infdb.get_config_value([infdb.get_toolname(), "data", "output", "table"]),
        }
        SQL_DIR = os.path.join("sql")   # add subfolders here if needed ("sql/subfolder")
        infdb.connect().execute_sql_files(SQL_DIR, format_params=format_params)


    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()
    
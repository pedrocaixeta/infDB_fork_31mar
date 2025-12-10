
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

    streets_id = infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "id-column"])
    heat_demand_id = infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "id-column"])

    # Define ID expressions: Use column if exists, else generate stable hash from geometry
    if streets_id == "None" or streets_id is None:
        streets_geom = infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "geom-column"])
        streets_id_expr = f"md5(ST_AsBinary(s.{streets_geom}))"
    else:
        streets_id_expr = f"s.{streets_id}::text"

    if heat_demand_id == "None" or heat_demand_id is None:
        heat_demand_geom = infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "geom-column"])
        heat_demand_id_expr = f"md5(ST_AsBinary(h.{heat_demand_geom}))"
    else:
        heat_demand_id_expr = f"h.{heat_demand_id}::text"

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
            'streets_id_expr': streets_id_expr,
            'streets_geom': infdb.get_config_value([infdb.get_toolname(), "data", "input", "streets", "geom-column"]),
            'heat_demand_schema': infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "schema"]),
            'heat_demand_table': infdb.get_config_value([infdb.get_toolname(), "data", "input", "heat-demand", "table"]),
            'heat_demand_id_expr': heat_demand_id_expr,
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
    
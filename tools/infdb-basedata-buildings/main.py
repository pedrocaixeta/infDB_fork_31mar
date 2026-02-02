"""
Main entry point for the infdb-basedata-buildings tool.
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
    infdb = InfDB(tool_name="infdb-basedata-buildings", config_path="configs")

    # Start message
    log = infdb.get_logger()
    log.info(f"Starting {infdb.get_toolname()} tool")

    try:

        # ===========================================================
        # Start your added sql scripts in folder "sql"
        # ===========================================================
        log.info("Running SQL scripts ...")
        format_params = {
            "input_schema": infdb.get_config_value([infdb.get_toolname(), "data", "input_schema"]),
            "output_schema": infdb.get_config_value([infdb.get_toolname(), "data", "output_schema"]),
            "list_gemeindeschluessel": infdb.get_config_value([infdb.get_toolname(), "data", "list_gemeindeschluessel"]),
            "EPSG": infdb.get_config_value([infdb.get_toolname(), "data", "EPSG"]),
            "use_address_information": str(
                infdb.get_config_value([infdb.get_toolname(), "data", "use_address_information"])
            ).lower(),  # -> "true" / "false"
        }
        SQL_DIR = os.path.join("sql")  # add subfolders here if needed ("sql/subfolder")
        infdb.connect().execute_sql_files(SQL_DIR, format_params=format_params)

        # ===========================================================
        # Demonstrate database querying - remove or comment out if not needed
        # ===========================================================
        
        infdb.stop_logger()

    except Exception as e:
        log.error(f"Something went wrong: {str(e)}")
        infdb.stop_logger()
        raise e


if __name__ == "__main__":
    main()

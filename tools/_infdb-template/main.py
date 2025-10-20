
"""
Main entry point for the choose-a-name tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
import os
from infdb import InfDB
from src import demo, choose_a_name


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and runs the demo function. Handles exceptions and logs errors.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="choose-a-name")

    # Start message
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:
        # Start your added python code in folder "src"
        # Example call function from choose_a_name.py
        choose_a_name.example_function(variable="Hello, InfDB!")

        # Start your added sql scripts in folder "sql"
        format_params = {
            'input_schema': infdb.get_config_value(["data", "input_schema"], insert_toolname=True),
            'output_schema': infdb.get_config_value(["data", "output_schema"], insert_toolname=True),
        }

        # Drop output schema if exists for development purposes
        infdb.connect().execute_query("DROP SCHEMA IF EXISTS {output_schema} CASCADE".format(**format_params))

        # Execute sql scripts
        infdb.get_log().info("Running SQL scripts ...")
        SQL_DIR = os.path.join("sql")   # add subfolders here if needed
        infdb.connect().execute_sql_files(SQL_DIR, format_params=format_params)


        # Demonstrate database querying using InfDB client - remove or comment out if not needed
        demo.sql_demo(infdb)
        demo.database_demo(infdb)
        demo.database_demo_sqlalchemy()

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()
    
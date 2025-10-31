
"""
Main entry point for the pylovo-generation tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
import os
from infdb import InfDB
from src import demo, pylovo_generation


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and runs the demo function. Handles exceptions and logs errors.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="pylovo-generation")

    # Start message
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:
        infdb.log.info("Running python code ...")
        # pylovo_generation.example_function(variable="Hello, InfDB!")

            # ===========================================================
        # Demonstrate database querying - remove or comment out if not needed
        # ===========================================================
        # infdb.log.info("Running demo ...")
        # demo.sql_demo(infdb)
        # demo.database_demo(infdb)
        # demo.database_demo_sqlalchemy()

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()
    
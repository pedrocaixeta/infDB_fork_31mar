"""
Main entry point for the pylovo-generation tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
import os
from infdb import InfDB
from src.create_env import create_pylovo_env_file
from src.pylovo_generation.pylovo.runme import main_constructor, main_generation
import yaml


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and generates the synthetic grids with pylovo.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="pylovo-generation")

    # Start message
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:
        infdb.log.info("Generating synthetic grids with pylovo ...")
        # Create .env file for pylovo
        create_pylovo_env_file(infdb)
        # Setup pylovo database
        main_constructor.main()
        # Get ags from infdb-config
        ags = infdb.get_value(["base", "scope"])[0]
        # Generate grids for the chosen ags
        main_generation.create_grid_single_ags(ags)

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

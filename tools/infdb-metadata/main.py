
"""
Main entry point for the infdb-metadata tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
from infdb import InfDB

import src.infdb_metadata as infdb_metadata


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and runs the demo function. Handles exceptions and logs errors.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="infdb-metadata")
    

    # Start messagero
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:
        # ===========================================================
        # Start your added python code in folder "src"
        # ===========================================================
        infdb.log.info("Running python code ...")
        client = infdb.connect()
        infdb_metadata.run_with_infdb(client, infdb.log)

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

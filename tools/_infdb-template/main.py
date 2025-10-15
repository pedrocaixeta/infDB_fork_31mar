
"""
Main entry point for the your-tool-name tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
from infdb import InfDB
from src import demo


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and runs the demo function. Handles exceptions and logs errors.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="your-tool-name")

    # Start message
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:

        # place here your code and delete demo code below
        
        # Demo functions
        demo.sql_demo(infdb)
        demo.database_demo(infdb)
        demo.database_demo_sqlalchemy()

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

import os
from infdb import InfDB

def main():

    # Load InfDB handler
    infdbhandler = InfDB(tool_name="infdb-basedata")

    # Database connection
    infdbclient_citydb = infdbhandler.connect()

    # Logger setup
    infdblog = infdbhandler.get_log()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # Get configuration values
    input_schema = infdbhandler.get_config_value(["data", "input_schema"], insert_toolname=True)
    output_schema = infdbhandler.get_config_value(["data", "output_schema"], insert_toolname=True)

    # Schema configuration
    format_params = {
        'input_schema': input_schema,
        'output_schema': output_schema,
        'list_gemeindeschluessel': "todo"
    }
    infdblog.info(f"Input schema: {input_schema}")
    infdblog.info(f"Output schema: {output_schema}")

    # Drop output schema if exists for development purposes
    infdbclient_citydb.execute_query("DROP SCHEMA IF EXISTS {output_schema} CASCADE".format(**format_params))

    # Execute WAYS scripts
    infdblog.info("Running WAYS SQL scripts")
    WAYS_SQL_DIR = os.path.join("sql", "ways_sql")
    infdbclient_citydb.execute_sql_files(WAYS_SQL_DIR, format_params=format_params)

    # Execute BUILDINGS scripts
    infdblog.info("Running BUILDINGS SQL scripts")
    BUILDINGS_SQL_DIR = os.path.join("sql", "buildings_sql")
    infdbclient_citydb.execute_sql_files(BUILDINGS_SQL_DIR, format_params=format_params)

    # Execute Connections scripts
    infdblog.info("Execute connections SQL scripts")
    CONNECTIONS_SQL_DIR = os.path.join("sql", "connections")
    infdbclient_citydb.execute_sql_files(CONNECTIONS_SQL_DIR, format_params=format_params)

    # End message
    infdblog.info(f"Successfully finished {infdbhandler.get_toolname()} tool")


if __name__ == "__main__":
    main()

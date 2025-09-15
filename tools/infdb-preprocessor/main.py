import os
from src.infdb.Infdb import InfDB

def main():

    # Load InfDB handler
    infdb = InfDB(tool_name="preprocessor")

    # Database connection
    infdbclient_citydb = infdb.connect(db_name="citydb")

    # Logger setup
    infdblog = infdb.get_log()

    infdblog.info(f"Starting {infdb.get_toolname()} tool")
    
    # Schema configuration
    format_params = {
        'input_schema': infdb.get_config_value(["preprocessor", "data", "input_schema"]),
        'output_schema': infdb.get_config_value(["preprocessor", "data", "output_schema"])
    }
    # # Datatype fix
    # infdblog.info("Fixing SQL data types")
    # infdbclient.execute_sql_file("sql/fixing_need.sql")

    # Execute WAYS scripts first
    infdblog.info("Running WAYS SQL scripts")
    WAYS_SQL_DIR = os.path.join("sql", "ways_sql")
    infdbclient_citydb.execute_sql_files(WAYS_SQL_DIR, format_params=format_params)

    infdbclient_citydb.execute_sql_file("sql/buildings_lod2.sql", format_params=format_params)

    # Then BUILDINGS scripts
    infdblog.info("Running BUILDINGS SQL scripts")
    BUILDINGS_SQL_DIR = os.path.join("sql", "buildings_sql")
    infdbclient_citydb.execute_sql_files(BUILDINGS_SQL_DIR, format_params=format_params)

    # # Connections scripts
    # infdblog.info("Execute connections SQL scripts")
    # CONNECTIONS_SQL_DIR = os.path.join("sql", "connections")
    # infdbclient_citydb.execute_sql_files(CONNECTIONS_SQL_DIR, format_params=format_params)

    infdblog.info(f"Successfully finished {infdb.get_toolname()} tool")


if __name__ == "__main__":
    main()

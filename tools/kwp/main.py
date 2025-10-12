from infdb import InfDB


def main():

    # Load InfDB handler
    infdbhandler = InfDB(tool_name="kwp")

    # Database connection
    infdbclient_citydb = infdbhandler.connect()

    # Logger setup
    infdblog = infdbhandler.get_log()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # Setup database engine
    engine = infdbclient_citydb.get_db_engine()

    # Parameter
    format_params = {
    "input_schema_basedata": infdbhandler.get_config_value(["data", "input_schema_basedata"], insert_toolname=True),
    "input_schema_ro-heat": infdbhandler.get_config_value(["data", "input_schema_ro-heat"], insert_toolname=True),
    "output_schema": infdbhandler.get_config_value(["data", "output_schema"], insert_toolname=True)
}

    try:
        infdblog.info(f"Input schema basedata: {format_params['input_schema_basedata']}")
        infdblog.info(f"Input schema ro-heat: {format_params['input_schema_ro-heat']}")
        infdblog.info(f"Output schema: {format_params['output_schema']}")

        sql = f"DROP SCHEMA IF EXISTS {format_params['output_schema']} CASCADE;"
        infdbclient_citydb.execute_query(sql)
        sql = f"CREATE SCHEMA IF NOT EXISTS {format_params['output_schema']};"
        infdbclient_citydb.execute_query(sql)

        # Run SQL scripts within sql folder
        infdbclient_citydb.execute_sql_files("sql", format_params=format_params)

        infdblog.info("kwp sucessfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

from infdb import InfDB


def main():

    # Load InfDB handler
    infdbhandler = InfDB(tool_name="kwp")

    # Database connection
    infdbclient_citydb = infdbhandler.connect(db_name="citydb")

    # Logger setup
    infdblog = infdbhandler.get_log()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # Setup database engine
    engine = infdbclient_citydb.get_db_engine()

    # Get configuration values
    input_schema = infdbhandler.get_config_value(["kwp", "data", "input_schema"])
    output_schema = infdbhandler.get_config_value(["kwp", "data", "output_schema"])

    try:
        infdblog.info("kwp sucessfully completed")

    except Exception as e:
        infdblog.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

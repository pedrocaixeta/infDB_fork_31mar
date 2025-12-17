from typing import Dict

from infdb import InfDB


def main() -> None:
    """Run the KWP SQL workflow.

    Steps:
      1) Initialize InfDB (config + logging).
      2) Read input/output schemas from config (tool section).
      3) Drop and (re)create output schema.
      4) Execute all SQL files in the local ./sql directory with format parameters.
    """
    # Initialize InfDB (config + logging)
    infdb = InfDB(tool_name="kwp", config_path="configs")
    log = infdb.get_logger()

    log.info("Starting %s tool", infdb.get_toolname())

    # Gather parameters from config
    format_params: Dict[str, str] = {
        "input_schema_basedata": infdb.get_config_value([infdb.get_toolname(), "data", "input_schema_basedata"]),
        "input_schema_ro-heat": infdb.get_config_value([infdb.get_toolname(), "data", "input_schema_ro-heat"]),
        "output_schema": infdb.get_config_value([infdb.get_toolname(), "data", "output_schema"]),
    }

    try:
        log.info("Input schema basedata: %s", format_params["input_schema_basedata"])
        log.info("Input schema ro-heat: %s", format_params["input_schema_ro-heat"])
        log.info("Output schema: %s", format_params["output_schema"])

        # DB work
        with infdb.connect() as db:
            db.execute_query(f"DROP SCHEMA IF EXISTS {format_params['output_schema']} CASCADE;")
            db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {format_params['output_schema']};")

            # Run all SQL scripts in ./sql with format params
            db.execute_sql_files("sql", format_params=format_params)

        log.info("kwp successfully completed")
        infdb.stop_logger()
    except Exception as e:
        log.error("Something went wrong: %s", str(e))
        infdb.stop_logger()
        raise


if __name__ == "__main__":
    main()

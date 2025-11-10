from typing import Dict

from infdb import InfDB


# ============================== Constants ==============================

TOOL_NAME: str = "kwp"
CONFIG_DIR: str = "configs"
DB_NAME: str = "postgres"
SQL_DIR: str = "sql"


def main() -> None:
    """Run the KWP SQL workflow.

    Steps:
      1) Initialize InfDB (config + logging).
      2) Read input/output schemas from config (tool section).
      3) Drop and (re)create output schema.
      4) Execute all SQL files in the local ./sql directory with format parameters.
    """
    # Initialize InfDB (config + logging)
    inf = InfDB(tool_name=TOOL_NAME, config_path=CONFIG_DIR)
    log = inf.get_log()

    log.info("Starting %s tool", inf.get_toolname())

    # Gather parameters from config
    format_params: Dict[str, str] = {
        "input_schema_basedata": inf.get_config_value(["data", "input_schema_basedata"], insert_toolname=True),
        "input_schema_ro-heat": inf.get_config_value(["data", "input_schema_ro-heat"], insert_toolname=True),
        "output_schema": inf.get_config_value(["data", "output_schema"], insert_toolname=True),
    }

    try:
        log.info("Input schema basedata: %s", format_params["input_schema_basedata"])
        log.info("Input schema ro-heat: %s", format_params["input_schema_ro-heat"])
        log.info("Output schema: %s", format_params["output_schema"])

        # DB work
        with inf.connect(db_name=DB_NAME) as db:
            db.execute_query(f"DROP SCHEMA IF EXISTS {format_params['output_schema']} CASCADE;")
            db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {format_params['output_schema']};")

            # Run all SQL scripts in ./sql with format params
            db.execute_sql_files(SQL_DIR, format_params=format_params)

        log.info("kwp successfully completed")

    except Exception as e:
        log.error("Something went wrong: %s", str(e))
        raise


if __name__ == "__main__":
    main()

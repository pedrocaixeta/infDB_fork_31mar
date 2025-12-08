import os
from typing import Any, Dict

from infdb import InfDB


def main() -> None:
    """Run base-data SQL pipelines (WAYS, BUILDINGS, CONNECTIONS) against Postgres.

    Loads configuration and logging via `InfDB`, prepares format parameters, drops
    the output schema (if present), and executes the SQL directories in sequence.
    """
    # Load InfDB facade (config + logging)
    infdb = InfDB(tool_name="infdb-basedata", config_path="configs")

    # Logger
    log = infdb.get_logger()
    log.info("Starting %s tool", infdb.get_toolname())

    # Config
    input_schema = infdb.get_config_value([infdb.get_toolname(), "data", "input_schema"])
    output_schema = infdb.get_config_value([infdb.get_toolname(), "data", "output_schema"])
    epsg = infdb.get_db_parameters_dict().get("epsg")

    format_params: Dict[str, Any] = {
        "input_schema": input_schema,
        "output_schema": output_schema,
        "list_gemeindeschluessel": "todo",
        "EPSG": epsg,
    }

    log.info("Input schema: %s", input_schema)
    log.info("Output schema: %s", output_schema)
    WAYS_SQL_DIR: str = os.path.join("sql", "ways_sql")
    BUILDINGS_SQL_DIR: str = os.path.join("sql", "buildings_sql")
    CONNECTIONS_SQL_DIR: str = os.path.join("sql", "connections")
    # Database work (context-managed)
    with infdb.connect() as db:
        # Drop output schema if exists (dev convenience)
        db.execute_query("DROP SCHEMA IF EXISTS {output_schema} CASCADE".format(**format_params))

        # Execute WAYS scripts
        log.info("Running WAYS SQL scripts")
        db.execute_sql_files(WAYS_SQL_DIR, format_params=format_params)

        # Execute BUILDINGS scripts
        log.info("Running BUILDINGS SQL scripts")
        db.execute_sql_files(BUILDINGS_SQL_DIR, format_params=format_params)

        # Execute CONNECTIONS scripts
        log.info("Executing connections SQL scripts")
        db.execute_sql_files(CONNECTIONS_SQL_DIR, format_params=format_params)

    log.info("Successfully finished %s tool", infdb.get_toolname())
    infdb.stop_logger()


if __name__ == "__main__":
    main()


"""
Main entry point for the process-streets tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages

import json
import sys

from infdb import InfDB
from src import process_streets

# Import your full pipeline
# (The file must be located at: /app/src/process_streets.py)

def main():

    # -----------------------------------------------------
    # INIT INFDB
    # -----------------------------------------------------
    infdb = InfDB(tool_name="process-streets")
    log = infdb.get_log()

    log.info("=== Starting process-streets tool ===")
    log.info("Loading configuration...")

    # -----------------------------------------------------
    # READ CONFIG VALUES
    # -----------------------------------------------------
    table_name = infdb.get_config_value(["process-streets", "data", "table_name"])
    klasse_filter = infdb.get_config_value(["process-streets", "klasse_filter"])
    apply_length_filter = infdb.get_config_value(["process-streets", "apply_length_filter"])
    min_length_deadend_junction = infdb.get_config_value(["process-streets", "min_length_deadend_junction"])
    remove_deadend_deadend = infdb.get_config_value(["process-streets", "remove_deadend_deadend"])

    log.info(f"Table: {table_name}")
    log.info(f"Klasse Filter: {klasse_filter}")

    # -----------------------------------------------------
    # RUN PIPELINE
    # -----------------------------------------------------
    try:
        log.info("Running process-streets pipeline...")

        results = process_streets.main(
            table_name=table_name,
            klasse_filter=klasse_filter,
            apply_length_filter=apply_length_filter,
            min_length_deadend_junction=min_length_deadend_junction,
            remove_deadend_deadend=remove_deadend_deadend,
            infdb=infdb
        )

        log.info("Pipeline finished successfully.")

        # Output result as JSON (InfDB standard)
        print(json.dumps(results, indent=4))
        return 0

    except Exception as e:
        log.error(f"Process-streets failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
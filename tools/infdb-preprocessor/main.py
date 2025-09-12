import os
import logging
from src.infdb import logger
from src.infdb.InfdbClient import InfdbClient
from src.infdb.InfdbConfig import InfdbConfig

log = logging.getLogger(__name__)

# Load InfDB configuration
infdbconfig = InfdbConfig("preprocessor", "configs/config-preprocessor.yml")

# Initialize logging
listener = logger.setup_main_logger(infdbconfig, None)

# SQL files directory and list of files to execute in order
WAYS_SQL_DIR = os.path.join("sql", "ways_sql")
BUILDINGS_SQL_DIR = os.path.join("sql", "buildings_sql")

WAYS_SQL_FILES = [
    '00_cleanup.sql',
    '01_create_functions.sql',
    '02_create_ways_table.sql',
    '03_fill_id_ways_table.sql',
    '04_create_postcode_table.sql',
    '05_assign_postcode_to_ways.sql'
]
BUILDINGS_SQL_FILES = [
    "00_cleanup.sql",
    "01_create_functions.sql",
    "02_create_buildings_table.sql",
    "03_fill_id_object_id_building_use.sql",
    "04_fill_height.sql",
    "05_fill_floor_area_geom.sql",
    "06_create_touching_buildings_temp_tables.sql",
    "07_fill_floor_number.sql",
    "08_prepare_grid.sql",
    "09_fill_occupants.sql",
    "10_fill_households.sql",
    "11_fill_construction_year.sql",
    "12_fill_building_type.sql",
    "13_assign_postcode_to_buildings.sql",
    "14_assign_streets_to_buildings.sql",
    "15_add_constraints.sql",
]


def main():
    try:
        # Database configuration
        infdbclient = InfdbClient(infdbconfig, db_name="citydb")

        # Validate all SQL files exist before starting
        missing_ways = [
            f
            for f in WAYS_SQL_FILES
            if not os.path.exists(os.path.join(WAYS_SQL_DIR, f))
        ]
        missing_buildings = [
            f
            for f in BUILDINGS_SQL_FILES
            if not os.path.exists(os.path.join(BUILDINGS_SQL_DIR, f))
        ]

        if missing_ways or missing_buildings:
            if missing_ways:
                log.error(f"Missing WAYS SQL files in {WAYS_SQL_DIR}/: {missing_ways}")
            if missing_buildings:
                log.error(
                    f"Missing BUILDINGS SQL files in {BUILDINGS_SQL_DIR}/: {missing_buildings}"
                )
            return 1
        
        # Schema configuration
        format_params = {
            'input_schema': infdbconfig.get_value(["preprocessor", "data", "input_schema"]),
            'output_schema': infdbconfig.get_value(["preprocessor", "data", "output_schema"])
        }
        # Datatype fix
        log.info("Fixing SQL data types")
        infdbclient.execute_sql_file("sql/fixing_need.sql")

        # Execute WAYS scripts first
        log.info("Running WAYS SQL scripts")
        infdbclient.execute_sql_files(WAYS_SQL_DIR, WAYS_SQL_FILES, format_params=format_params)

        # Then BUILDINGS scripts
        log.info("Running BUILDINGS SQL scripts")
        infdbclient.execute_sql_files(BUILDINGS_SQL_DIR, BUILDINGS_SQL_FILES, format_params=format_params)

        log.info("Prepared buildings and ways successfully!")

    except Exception as e:
        log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

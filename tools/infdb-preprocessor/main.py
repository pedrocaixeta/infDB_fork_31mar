import os
from src.infdb.Infdb import InfDB

# WAYS_SQL_FILES = [
#     '00_cleanup.sql',
#     '01_create_functions.sql',
#     '02_create_ways_table.sql',
#     '03_fill_id_ways_table.sql',
#     '04_create_postcode_table.sql',
#     '05_assign_postcode_to_ways.sql'
# ]
# BUILDINGS_SQL_FILES = [
#     "00_cleanup.sql",
#     "01_create_functions.sql",
#     "02_create_buildings_table.sql",
#     "03_fill_id_object_id_building_use.sql",
#     "04_fill_height.sql",
#     "05_fill_floor_area_geom.sql",
#     "06_create_touching_buildings_temp_tables.sql",
#     "07_fill_floor_number.sql",
#     "08_prepare_grid.sql",
#     "09_fill_occupants.sql",
#     "10_fill_households.sql",
#     "11_fill_construction_year.sql",
#     "12_fill_building_type.sql",
#     "13_assign_postcode_to_buildings.sql",
#     "14_assign_streets_to_buildings.sql",
#     # "15_add_constraints.sql",
# ]


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

    # Then BUILDINGS scripts
    infdblog.info("Running BUILDINGS SQL scripts")
    BUILDINGS_SQL_DIR = os.path.join("sql", "buildings_sql")
    infdbclient_citydb.execute_sql_files(BUILDINGS_SQL_DIR, format_params=format_params)

    # Connections scripts
    infdblog.info("Execute connections SQL scripts")
    CONNECTIONS_SQL_DIR = os.path.join("sql", "connections")
    infdbclient_citydb.execute_sql_files(CONNECTIONS_SQL_DIR, format_params=format_params)

    
    infdblog.info(f"Successfully finished {infdb.get_toolname()} tool")


if __name__ == "__main__":
    main()

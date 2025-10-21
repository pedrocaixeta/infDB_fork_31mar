import os
from . import config, utils
from .logger import setup_worker_logger
import logging

log = logging.getLogger(__name__)

def create_geogitter(resolutions, clear_existing=False):
    """Create a grid_cells table containing cells for all specified resolutions.

    This function drops any existing table with the same name, creates a new
    table schema, and populates it with grid cells for each requested resolution.
    The grid cells are generated based on the provided EPSG code and the 
    geographical envelope.
    """
    epsg = utils.get_db_parameters("postgres")["epsg"]
    schema = config.get_value(["loader", "sources", "bkg", "schema"])
    table_name = config.get_value(["loader", "sources", "bkg", "geogitter", "table_name"])
    
    # Drop existing table if present
    if clear_existing:
        sql = f"DROP TABLE IF EXISTS {schema}.{table_name};"
        utils.sql_query(sql)

    # Create table schema with proper columns
    log.info(f"Creating {table_name} table schema...")
    sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            id TEXT,
            x_mp INTEGER,
            y_mp INTEGER,
            name TEXT,
            resolution_meters INTEGER,
            geom GEOMETRY
        );
        CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {schema}.{table_name} USING GIST (geom);
    """
    utils.sql_query(sql)

    envelop = utils.get_envelop()
    wkt = envelop.to_crs(3035).union_all().wkt  # ETRS89 is used by BKG for geogitter

    # Ensure resolutions is a list
    if isinstance(resolutions, str):
        resolutions = [resolutions]
    
    for resolution in resolutions:
        if resolution.endswith("km"):
            resolution_meters = int(resolution[:-2]) * 1000
        elif resolution.endswith("m"):
            resolution_meters = int(resolution[:-1])
        else:
            # skip unknown formats
            log.warning("Skipping resolution with unknown unit: %s", resolution)
            continue

        # Generate grid cells for one resolution, only insert if id does not already exist
        generate_grid_cells_sql = f"""
            WITH params AS (
                SELECT {resolution_meters}::int AS cell_size
            ),
            boundary AS (
                SELECT ST_GeomFromText('{wkt}', 3035) AS geom
            ),
            envelope AS (
                SELECT
                    FLOOR(ST_XMin(b.geom) / p.cell_size) * p.cell_size AS x_min,
                    FLOOR(ST_YMin(b.geom) / p.cell_size) * p.cell_size AS y_min,
                    CEIL(ST_XMax(b.geom) / p.cell_size) * p.cell_size AS x_max,
                    CEIL(ST_YMax(b.geom) / p.cell_size) * p.cell_size AS y_max,
                    p.cell_size
                FROM boundary b, params p
            ),
            grid_raw AS (
                SELECT (ST_SquareGrid(
                        e.cell_size,
                        ST_MakeEnvelope(e.x_min, e.y_min, e.x_max, e.y_max, 3035)
                        )).* 
                FROM envelope e
            ),
            grid AS (
                SELECT
                    ST_Transform(geom, {epsg}) AS geom,
                    ST_XMin(geom) AS x,
                    ST_YMin(geom) AS y
                FROM grid_raw
            ),
            id_named AS (
                SELECT
                    FORMAT('%sN%sE%s', '{resolution}', g.y::int::text, g.x::int::text) AS id,
                    (g.x + (p.cell_size / 2.0))::int AS x_mp,
                    (g.y + (p.cell_size / 2.0))::int AS y_mp,
                    'DE_Grid_ETRS89_LAEA_{resolution}'::text AS name,
                    p.cell_size::int AS resolution_meters,
                    g.geom
                FROM grid g, params p
            )
            SELECT * FROM id_named
            WHERE id NOT IN (SELECT id FROM {schema}.{table_name});
        """

        # Insert subsequent resolutions' cells, only if id does not exist
        sql = f"INSERT INTO {schema}.{table_name} {generate_grid_cells_sql};"
        utils.sql_query(sql)


def load(log_queue):
    setup_worker_logger(log_queue)

    # # Todo: Load data for scope selection seperately, think about how to define scope  
    # if not utils.if_active("bkg"):
    #     return

    # Check if the required directories exist, if not create them
    # Base path for zip files
    zip_path = config.get_path(["loader", "sources", "bkg", "path", "zip"])
    os.makedirs(zip_path, exist_ok=True)

    # Base path for unzipped files
    unzip_path = config.get_path(["loader", "sources", "bkg", "path", "unzip"])
    os.makedirs(unzip_path, exist_ok=True)

    ## Create schema in database
    schema = config.get_value(["loader", "sources", "bkg", "schema"])

    # Prefix for table names
    prefix = config.get_value(["loader", "sources", "bkg", "prefix"])

    sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    utils.sql_query(sql)

    # NUTS-Gebiete
    log.info("Downloading and unzipping NUTS")
    url = config.get_value(["loader", "sources", "bkg", "nuts", "url"])
    utils.download_files(url, zip_path)
    files = utils.get_file(zip_path, filename="nuts250", ending=".zip")
    utils.unzip(files, unzip_path)
    
    nuts_layers = config.get_value(["loader", "sources", "bkg", "nuts", "layer"])
    file = utils.get_file(unzip_path, filename="nuts250", ending=".gpkg")
    utils.import_layers(file, nuts_layers, schema, prefix, scope=False)

    # Verwaltungsgebiete
    log.info("Downloading and unzipping VG5000")
    url = config.get_value(["loader", "sources", "bkg", "vg5000", "url"])
    utils.download_files(url, zip_path)
    files = utils.get_file(zip_path, filename="vg5000", ending=".zip")
    utils.unzip(files, unzip_path)

    vg_layers = config.get_value(["loader", "sources", "bkg", "vg5000", "layer"])
    file = utils.get_file(unzip_path, filename="vg5000", ending=".gpkg")
    utils.import_layers(file, vg_layers, schema, prefix, scope=False)

    # Geogitter
    resolutions = config.get_value(
        ["loader", "sources", "bkg", "geogitter", "resolutions"]
    )
    log.info("Creating Geogitter layers resolutions %s", resolutions)
    create_geogitter(resolutions, clear_existing=True)

    log.info("BKG data loaded successfully")

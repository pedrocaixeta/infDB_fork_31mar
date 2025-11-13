# src/bkg.py
import logging
import multiprocessing as mp
import os
from logging.handlers import QueueHandler
from typing import List, Sequence, Union

from infdb import InfDB
from . import utils


# ============================== Constants ==============================

TOOL_NAME: str = "loader"
GPKG_EXT: str = ".gpkg"
ZIP_EXT: str = ".zip"


# Module logger


def create_geogitter(resolutions: Union[Sequence[str], str], infdb:InfDB, clear_existing: bool = False) -> None:
    """Create (or update) a single geogitter table by inserting grid cells per resolution.

    Behavior preserved:
    - Single target table: {schema}.{table_name}
    - Optionally drop the table when `clear_existing=True`
    - Create schema + spatial index if missing
    - Insert grid cells per resolution, skipping rows whose id already exists

    Args:
        resolutions: Either a single resolution string (e.g., "1km", "500m")
            or a sequence of such strings.
        clear_existing: If True, drop the table before (re)creating it.

    Raises:
        KeyError: If EPSG is missing in DB parameters.
    """
    log = infdb.get_worker_logger()
    # DB / config
    epsg = (infdb.get_db_parameters_dict() or {}).get("epsg")
    if epsg is None:
        raise KeyError("Missing 'epsg' in DB parameters for service 'postgres'")

    schema = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "schema"])
    table_name = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "geogitter", "table_name"])

    envelope = utils.get_envelop()
    wkt = envelope.to_crs(3035).unary_union.wkt

    # Ensure list
    if isinstance(resolutions, str):
        resolutions = [resolutions]

    # Build base table
    with infdb.connect() as db:
        if clear_existing:
            db.execute_query(f"DROP TABLE IF EXISTS {schema}.{table_name};")

        log.info("Creating %s table schema if needed...", table_name)
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                id TEXT,
                x_mp INTEGER,
                y_mp INTEGER,
                name TEXT,
                resolution_meters INTEGER,
                geom GEOMETRY
            );
            CREATE INDEX IF NOT EXISTS {table_name}_geom_idx
              ON {schema}.{table_name} USING GIST (geom);
        """
        db.execute_query(ddl)

        # Insert per resolution, skipping existing ids
        for resolution in resolutions or []:
            if resolution.endswith("km"):
                resolution_meters = int(resolution[:-2]) * 1000
            elif resolution.endswith("m"):
                resolution_meters = int(resolution[:-1])
            else:
                log.warning("Skipping resolution with unknown unit: %s", resolution)
                continue

            log.info("Generating grid cells for resolution %s", resolution)

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

            insert_sql = f"INSERT INTO {schema}.{table_name} {generate_grid_cells_sql};"
            db.execute_query(insert_sql)


def load(infdb: InfDB) -> None:
    """Download BKG sources, import layers, and generate geogitter grid.

    Behavior preserved:
    - (Optional) feature guard for BKG: left commented as in original.
    - Download/unzip/import NUTS and VG5000 with scope=False.
    - Create schema if missing; then generate geogitter with configured resolutions.
    """
    log = infdb.get_worker_logger()

    # if not utils.if_active("bkg"):
    #     return

    # Paths
    zip_path = infdb.get_config_path([TOOL_NAME, "sources", "bkg", "path", "zip"], type="loader")
    os.makedirs(zip_path, exist_ok=True)
    unzip_path = infdb.get_config_path([TOOL_NAME, "sources", "bkg", "path", "unzip"], type="loader")
    os.makedirs(unzip_path, exist_ok=True)

    schema = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "schema"])
    prefix = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "prefix"])

    # Ensure schema exists via InfdbClient
    with infdb.connect() as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # --- NUTS (download+unzip+import) ---
    log.info("Downloading and unzipping NUTS")
    nuts_url = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "nuts", "url"])
    utils.download_files(nuts_url, zip_path)
    nuts_zip = utils.get_file(zip_path, filename="nuts250", ending=ZIP_EXT)
    utils.unzip(nuts_zip, unzip_path)

    nuts_layers = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "nuts", "layer"])
    nuts_gpkg = utils.get_file(unzip_path, filename="nuts250", ending=GPKG_EXT)
    utils.import_layers(nuts_gpkg, nuts_layers, schema, prefix, scope=False)

    # --- VG5000 (download+unzip+import) ---
    log.info("Downloading and unzipping VG5000")
    vg_url = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "vg5000", "url"])
    utils.download_files(vg_url, zip_path)
    vg_zip = utils.get_file(zip_path, filename="vg5000", ending=ZIP_EXT)
    utils.unzip(vg_zip, unzip_path)

    vg_layers = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "vg5000", "layer"])
    vg_gpkg = utils.get_file(unzip_path, filename="vg5000", ending=GPKG_EXT)
    utils.import_layers(vg_gpkg, vg_layers, schema, prefix, scope=False)

    # --- Geogitter ---
    resolutions = infdb.get_config_value([TOOL_NAME, "sources", "bkg", "geogitter", "resolutions"])
    log.info("Creating Geogitter layers resolutions %s", resolutions)
    create_geogitter(resolutions, infdb, clear_existing=True)

    log.info("BKG data loaded successfully")

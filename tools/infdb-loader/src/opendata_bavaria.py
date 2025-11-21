import os
import logging
import subprocess
import json
import re
from pathlib import Path
from sqlalchemy import text
from typing import Dict, List, Optional
from osgeo import gdal

from infdb import InfdbConfig, InfDB, InfdbClient
from . import utils

# Module logger
log = logging.getLogger(__name__)

TOOL_NAME = "loader"
CONFIG_DIR: str = "configs"
DB_NAME: str = "postgres"


# ==================== Helpers ====================

def _get_gpkg_layers(gpkg: Path) -> list[str]:
    """List layer names in a GPKG."""
    try:
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", "-json", str(gpkg)], text=True)
        data = json.loads(out)
        return [lyr.get("name") for lyr in data.get("layers", []) if "name" in lyr]
    except Exception:
        # Fallback to text parsing
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", str(gpkg)], text=True, stderr=subprocess.STDOUT)
        return [
            line.split(":", 1)[1].split("(")[0].strip()
            for line in out.splitlines()
            if ":" in line and "(" in line
        ]


def _get_layer_spatial_info(gpkg: Path, layer: str) -> tuple[int | None, tuple[float, ...] | None]:
    """Extract spatial reference system (EPSG) and bounding box from a GPKG layer."""
    
    out = subprocess.check_output(["ogrinfo", "-so", "-ro", "-q", str(gpkg), layer], text=True)
    
    # Parse EPSG
    epsg = None
    if m := re.search(r"EPSG:(\d+)", out):
        epsg = int(m.group(1))
    
    # Parse extent
    bbox = None
    if m := re.search(r"Extent:\s*\(([-\d.]+),\s*([-\d.]+)\)\s*-\s*\(([-\d.]+),\s*([-\d.]+)\)", out):
        bbox = tuple(map(float, m.groups()))
    
    return epsg, bbox


def _distance_2d(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Euclidean distance between two 2D points."""
    return ((a[0] - b[0])**2 + (a[1] - b[1])**2) ** 0.5


# ==================== Main Loader ====================

def load(infdb: InfDB) -> bool:
    """Load OpenData Bavaria datasets: DGM1, LoD2, and TN."""
    try:
        global log
        log = infdb.get_worker_logger()

        if not utils.if_active("opendata_bavaria"):
            return True

        # Enable PostGIS raster extension
        with infdb.connect() as db:
            db.execute_query("CREATE EXTENSION IF NOT EXISTS postgis_raster SCHEMA public CASCADE;")
            log.info("PostGIS raster extension enabled")

        #Get base path from opendata_bavaria config
        base_path = Path(infdb.get_config_path(
            [TOOL_NAME, "sources", "opendata_bavaria", "path", "base"], 
            type="loader"
        ))
        base_path.mkdir(parents=True, exist_ok=True)

        datasets = infdb.get_config_value([TOOL_NAME, "sources", "opendata_bavaria", "datasets"]) or {}
        cfg = InfdbConfig(tool_name=TOOL_NAME, config_path=CONFIG_DIR)

        # Database connection parameters
        db_params = cfg.get_db_parameters("postgres")
        pgurl = f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["exposed_port"]}/{db_params["db"]}'
        target_epsg = db_params["epsg"]

        # DGM1 (Terrain Model)
        dgm1_cfg = datasets.get("gelaendemodell_1m", {})
        if dgm1_cfg.get("status") == "active":
            _load_dgm1(infdb, base_path,target_epsg)

        # LoD2 Buildings
        lod2_cfg = datasets.get("building_lod2", {})
        if lod2_cfg.get("status") == "active":
            if _load_lod2 is None:
                log.warning("LoD2: loader not available; skipping.")
            else:
                log.info("LoD2: delegating to existing loader")
                _load_lod2(infdb)

        # Land Use (Tatsächliche Nutzung)
        tn_cfg = datasets.get("tatsaechliche_nutzung", {})
        if tn_cfg.get("status") == "active":
            _load_tatsaechliche_nutzung(infdb, tn_cfg, base_path, pgurl, target_epsg)

        log.info("OpenData Bavaria: complete.")
        return True

    except Exception as err:
        log.exception(f"An error occurred in OpenData Bavaria loader: {str(err)}")
        return False


def _load_dgm1(infdb: InfDB, base_path: Path, target_epsg: int):
    """Load DGM1 clipped exactly to the configured scope polygon."""

    # ---------------------- Configuration -------------------------
    config_base = [TOOL_NAME, "sources", "opendata_bavaria", "datasets", "gelaendemodell_1m"]

    url_template = infdb.get_config_value(config_base + ["url"])
    schema = infdb.get_config_value(config_base + ["schema"]) or "opendata"
    table = infdb.get_config_value(config_base + ["table_name"]) or "gelaendemodell_1m"
    raw_table = f"{table}_raw"
    source_srid = int(infdb.get_config_value(config_base + ["srid"]) or 25832)
    target_resolution_meters = float(infdb.get_config_value(config_base + ["target_resolution"]) or 10.0)

    # Output directory
    dgm1_base_dir = base_path / "gelaendemodell_1m"
    dgm1_base_dir.mkdir(parents=True, exist_ok=True)

    # Clean tables for new run
    with infdb.connect() as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        db.execute_query(f"DROP TABLE IF EXISTS {schema}.{table};")
        db.execute_query(f"DROP TABLE IF EXISTS {schema}.{raw_table};")

    # ---------------------- SCOPE HANDLING -------------------------
    configured_scope_values = infdb.get_config_value([TOOL_NAME, "scope"]) or []
    if isinstance(configured_scope_values, str):
        configured_scope_values = [configured_scope_values]

    ags_municipality_codes = []
    for raw in configured_scope_values:
        v = str(raw).strip()
        if v:
            ags_municipality_codes.append(v.zfill(8)[:8])

    if not ags_municipality_codes:
        log.warning("DGM1: No valid AGS municipality codes configured; skipping loader.")
        return

    landkreis_download_codes = sorted({ags[:5] for ags in ags_municipality_codes})
    log.info("DGM1: Landkreise: " + ", ".join(landkreis_download_codes))

    # ---------------------- CLIP GEOMETRY (EXACT) -------------------------
    # Get exact scope geometry in source_srid
    clip_wkt, clip_method, _ = utils.get_clip_geometry(source_srid, method="exact")

    clip_extent_args = ""
    cutline_args = ""
    scope_geojson_path = None

    if clip_wkt:
        try:
            from shapely import wkt as shapely_wkt
            from shapely.geometry import mapping
            import json

            geom = shapely_wkt.loads(clip_wkt)
            minx, miny, maxx, maxy = geom.bounds
            clip_extent_args = f"-te {minx} {miny} {maxx} {maxy} -te_srs EPSG:{source_srid}"
            log.info(f"DGM1: Using exact-geometry bbox: {minx},{miny},{maxx},{maxy}")

            # Write scope polygon once as GeoJSON for gdalwarp cutline
            scope_geojson_path = dgm1_base_dir / "dgm1_scope_clip.geojson"
            fc = {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": mapping(geom)
                    }
                ]
            }
            scope_geojson_path.write_text(json.dumps(fc))
            # -crop_to_cutline trims to bbox of polygon, and -dstnodata
            # makes the outside area NoData (= transparent in QGIS)
            cutline_args = (
                f'-cutline "{scope_geojson_path}" '
                f'-cutline_srs EPSG:{source_srid} '
                "-crop_to_cutline "
                "-dstnodata 0 "
            )
        except Exception as e:
            log.error(f"DGM1: Failed to prepare exact clip geometry → {e}")
            clip_extent_args = ""
            cutline_args = ""
            scope_geojson_path = None

    # ---------------------- DB connection strings -------------------------
    pgurl = utils._pg_connstring_for_psql()
    psql_command = f'psql --no-psqlrc -q -v ON_ERROR_STOP=1 -X "{pgurl}"'

    any_data_imported = False

    # ---------------------- Main Loop -------------------------
    for landkreis_code in landkreis_download_codes:

        log.info(f"DGM1: processing Landkreis {landkreis_code}")
        landkreis_dir = dgm1_base_dir / landkreis_code
        landkreis_dir.mkdir(parents=True, exist_ok=True)

        # 1) Download
        download_url = url_template.replace("#scope", landkreis_code)
        utils.download_aria2c(
            download_url,
            landkreis_dir,
            connections=1,
            allow_overwrite=False,
            auto_file_renaming=False,
        )

        # 2) Extract
        zip_files = utils.get_all_files(str(landkreis_dir), ".zip")
        if zip_files:
            utils.unzip(zip_files, str(landkreis_dir))

        # 3) Find source rasters
        raster_source_files = subprocess.check_output(
            ["find", str(landkreis_dir), "-type", "f",
             "(", "-iname", "*.tif", "-o", "-iname", "*.asc", ")", "-print"],
            text=True
        ).strip()

        if not raster_source_files:
            continue

        tile_paths = raster_source_files.splitlines()

        # 4) VRT
        virtual_raster_path = landkreis_dir / f"dgm1_{landkreis_code}.vrt"
        tile_list_file = landkreis_dir / "raster_tiles.txt"
        tile_list_file.write_text("\n".join(tile_paths))

        utils.do_cmd(
            f'gdalbuildvrt -resolution highest -r bilinear '
            f'-input_file_list "{tile_list_file}" "{virtual_raster_path}"'
        )

        # 5) Warp + exact polygon clipping in one step
        output_filename = f"dgm1_{landkreis_code}_{target_resolution_meters}m.tif"
        output_geotiff_path = landkreis_dir / output_filename

        gdalwarp_opts = (
            "-of GTiff "
            "-co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 "
            "-co BIGTIFF=IF_SAFER -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 "
            "-co NUM_THREADS=ALL_CPUS -r bilinear "
            f"-t_srs EPSG:{source_srid} "
            f"-tr {target_resolution_meters} {target_resolution_meters} "
            f"{clip_extent_args} {cutline_args} "
            "-overwrite -multi -wo NUM_THREADS=ALL_CPUS"
        )

        utils.do_cmd(
            f'gdalwarp {gdalwarp_opts} "{virtual_raster_path}" "{output_geotiff_path}"'
        )

        # 6) Validate
        try:
            utils.do_cmd(f'gdalinfo "{output_geotiff_path}" -stats -nomd')
            file_size_mb = output_geotiff_path.stat().st_size / 1_000_000
            log.info(f"DGM1: Created clipped raster ({file_size_mb:.1f} MB)")
        except Exception as e:
            log.error(f"DGM1: Warping/clipping failed for {landkreis_code}: {e}")
            output_geotiff_path.unlink(missing_ok=True)
            continue

        # 7) Import to PostGIS
        target_table = f"{schema}.{table}"
        mode = "" if not any_data_imported else "-a"

        import_pipeline = (
            "raster2pgsql -q "
            f"-s {source_srid} "
            "-I -M "
            "-t 256x256 "
            f'"{output_geotiff_path}" {target_table} {mode} | {psql_command}'
        )

        utils.do_cmd(import_pipeline)
        any_data_imported = True

    # ---------------------- Final indexing -------------------------
    if any_data_imported:
        utils.do_cmd(
            f'{psql_command} -c '
            f'"CREATE INDEX IF NOT EXISTS {table}_rast_gix '
            f'ON {schema}.{table} USING GIST(ST_ConvexHull(rast));"'
        )
        utils.do_cmd(f'{psql_command} -c "ANALYZE {schema}.{table};"')
        log.info(f"DGM1: Completed successfully → {schema}.{table}")
    else:
        log.warning("DGM1: No data imported")



# ==================== Land Use Loader ====================

def _load_tatsaechliche_nutzung(
    infdb: InfDB,
    cfg: dict,
    base_path: Path,
    pgurl: str,
    target_epsg: int
):
    """Load land use (TN) from Nutzung_kreis.gpkg into PostGIS.

    Approach:
      * Treat Nutzung_kreis.gpkg as a local spatial database.
      * For each layer in the GPKG, use ogr2ogr (via utils.import_layers)
        with a spatial filter based on the current scope (scope=True).
      * Only features that intersect the scope are imported.
      * We keep track of how many rows each layer contributes.
    """

    url = cfg["url"]
    schema = cfg.get("schema", "opendata")
    table = cfg.get("table_name", "tatsaechliche_nutzung")

    tn_dir = base_path / "tatsaechliche_nutzung"
    tn_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = tn_dir / "Nutzung_kreis.gpkg"

    # ----------------------- Download GPKG if needed -----------------------
    if gpkg_path.exists() and gpkg_path.stat().st_size > 1_000_000_000:
        log.info(f"TN: using existing {gpkg_path.stat().st_size / 1e9:.1f} GB GPKG")
    else:
        log.info(f"TN: downloading TN dataset from {url}")
        utils.download_aria2c(
            url=url,
            output_dir=tn_dir,
            output_filename="Nutzung_kreis.gpkg",
            connections=4,
            max_connection_per_server=4,
        )

    # ----------------------- Ensure schema exists -----------------------
    conf = InfdbConfig(tool_name=TOOL_NAME, config_path=CONFIG_DIR)
    client = InfdbClient(conf, log, db_name=DB_NAME)
    engine = client.get_db_engine()

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        conn.commit()

    # ----------------------- Check we have a scope geometry -----------------------
    clip_wkt, _, _ = utils.get_clip_geometry(target_crs=target_epsg, method="exact")
    if not clip_wkt:
        log.warning("TN: No scope geometry found; skipping TN import.")
        return
    else:
        log.info("TN: scope geometry available – will use it for spatial filtering via ogr2ogr.")

    # ----------------------- Discover layers in the GPKG -----------------------
    layer_names = _get_gpkg_layers(gpkg_path)
    if not layer_names:
        raise RuntimeError(f"TN: no layers found in {gpkg_path}")

    log.info(f"TN: Nutzung_kreis.gpkg contains {len(layer_names)} layers.")

    # ----------------------- Import each layer, cut to scope -----------------------
    is_first_import = True
    total_rows_before = 0
    total_rows_imported = 0

    # Initial row count (in case table already exists from previous runs)
    with engine.connect() as conn:
        try:
            total_rows_before = conn.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{table}")
            ).scalar() or 0
        except Exception:
            total_rows_before = 0

    total_rows_imported = total_rows_before

    for layer_name in layer_names:
        try:
            log.info(f"TN: importing from layer '{layer_name}' (cut to scope)...")

            # Use ogr2ogr via our helper.
            # 'scope=True' should internally:
            #   * apply a spatial filter using the scope geometry from config
            #   * so only features intersecting the scope are copied
            utils.import_layers(
                input_file=str(gpkg_path),
                layers=[layer_name],
                schema=schema,
                layer_names=[table],
                scope=True,
                overwrite=(is_first_import),
            )
            is_first_import = False

            # Check how many rows we have after this layer
            with engine.connect() as conn:
                current_row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                ).scalar() or 0

            newly_imported = current_row_count - total_rows_imported
            total_rows_imported = current_row_count

            if newly_imported > 0:
                log.info(
                    f"TN: layer '{layer_name}' contributed {newly_imported:,} rows "
                    f"(cumulative {total_rows_imported:,})."
                )
            else:
                log.info(
                    f"TN: layer '{layer_name}' has no features in scope "
                    f"(0 new rows added)."
                )

        except Exception as e:
            log.warning(f"TN: layer '{layer_name}' failed during import: {e}")
            continue

    # ----------------------- Create index if we imported any data -----------------------
    if total_rows_imported > 0:
        with engine.connect() as conn:
            conn.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS {table}_geom_gix "
                    f"ON {schema}.{table} USING GIST(geom);"
                )
            )
            conn.commit()

        log.info(
            f"TN: import finished for {schema}.{table}. "
            f"Total rows after import: {total_rows_imported:,}."
        )
    else:
        log.warning(f"TN: no TN features were imported into {schema}.{table}.")


# ==================== load lod2 ====================
def _load_lod2(infdb: InfDB)  -> bool:
    """Download CityGML (per AGS scope), import via citydb CLI, then run post-import SQL.

    Behavior preserved:
    - Returns True when inactive (matching original early-exit).
    - Uses aria2c for downloads and `citydb import citygml` for loading.
    - Builds URL by replacing `#scope` token with each AGS value.
    - Executes a post-import SQL file with format params.
    """
    log = infdb.get_worker_logger()

    if not utils.if_active("lod2"):
        return True


    base_path = infdb.get_config_path([TOOL_NAME, "sources", "lod2", "path", "lod2"], type="loader")
    os.makedirs(base_path, exist_ok=True)

    gml_path = infdb.get_config_path([TOOL_NAME, "sources", "lod2", "path", "gml"], type="loader")
    os.makedirs(gml_path, exist_ok=True)

    scope = infdb.get_config_value([TOOL_NAME, "scope"])
    if isinstance(scope, str):
        scope = [scope]

    # Download per administrative code (AGS)
    url_cfg = infdb.get_config_value([TOOL_NAME, "sources", "lod2", "url"])
    for ags in scope or []:
        url: str
        if isinstance(url_cfg, list):
            url = " ".join(url_cfg)
        else:
            url = str(url_cfg)

        url = url.replace("#scope", ags)

        log.info("*.gml import target directory: %s", gml_path)
        utils.download_aria2c(
            url=url,
            output_dir=gml_path,
            allow_overwrite=False,
            auto_file_renaming=False
        )

    # Import CityGML into PostGIS via citydb CLI
    params: Dict[str, str] = infdb.get_db_parameters_dict()
    import_mode: Optional[str] = infdb.get_config_value([TOOL_NAME, "sources", "lod2", "import-mode"])
    cmd_parts: List[str] = [
        "citydb import citygml",
        "-H", params["host"],
        "-d", params["db"],
        "-u", params["user"],
        "-p", params["password"],
        "-P", str(params["exposed_port"]),
        f"--import-mode={import_mode}",
        str(gml_path),
    ]
    utils.do_cmd(" ".join(str(a) for a in cmd_parts))

    # Post-import SQL (e.g., create LOD2 building table/view)
    format_params = {"output_schema": "opendata"}
    with infdb.connect() as db:
        db.execute_sql_file("sql/buildings_lod2.sql", format_params)

    log.info("LOD2 data loaded successfully")
    return True

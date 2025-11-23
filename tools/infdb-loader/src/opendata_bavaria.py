import os
import logging
import subprocess
import json
from pathlib import Path
from sqlalchemy import text
from typing import Dict, List, Optional
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping

from infdb import InfDB 
from . import utils

# Module logger
log = logging.getLogger(__name__)


# ====================================================================================
# HELPER FUNCTIONS - Utilities for GDAL/OGR operations and geometry calculations
# ====================================================================================

def _get_gpkg_layers(gpkg: Path) -> list[str]:
    """List layer names in a GPKG."""
    try:
        # Attempt JSON parsing (preferred method for structured output)
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", "-json", str(gpkg)], text=True)
        data = json.loads(out)
        return [lyr.get("name") for lyr in data.get("layers", []) if "name" in lyr]
    except Exception:
        # Fallback: parse text output line by line
        # Expected format: "1: layer_name (Geometry Type)"
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", str(gpkg)], text=True, stderr=subprocess.STDOUT)
        return [
            line.split(":", 1)[1].split("(")[0].strip()
            for line in out.splitlines()
            if ":" in line and "(" in line
        ]




# ====================================================================================
# MAIN ORCHESTRATION - Entry point that coordinates all dataset loaders
# ====================================================================================

def load(infdb: InfDB) -> bool:
    """Main entry point for loading OpenData Bavaria datasets."""
    try:
        log = infdb.get_worker_logger()

        # Early exit if this module is disabled
        if not utils.if_active("opendata_bavaria", infdb):
            return True

        # -------------------- Enable PostGIS Extensions --------------------
        # PostGIS raster needed for DGM1 elevation data
        with infdb.connect() as db:
            db.execute_query("CREATE EXTENSION IF NOT EXISTS postgis_raster SCHEMA public CASCADE;")
            log.info("PostGIS raster extension enabled")

        # -------------------- Configuration Setup --------------------
        # Get base directory for downloaded/processed files
        base_path = Path(infdb.get_config_path(
            [infdb.get_toolname(), "sources", "opendata_bavaria", "path", "base"], 
            type="loader"
        ))
        base_path.mkdir(parents=True, exist_ok=True)

        # Read dataset configurations
        datasets = infdb.get_config_value([infdb.get_toolname(), "sources", "opendata_bavaria", "datasets"]) or {}

        # -------------------- Database Connection Parameters --------------------
        db_params = infdb.get_db_parameters_dict()
        pgurl = f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["exposed_port"]}/{db_params["db"]}'
        target_epsg = db_params["epsg"]

        # -------------------- Load DGM1 (Digital Terrain Model) --------------------
        dgm1_cfg = datasets.get("gelaendemodell_1m", {})
        if dgm1_cfg.get("status") == "active":
            _load_dgm1(infdb, base_path, target_epsg)

        # -------------------- Load LoD2 (3D Buildings) --------------------
        lod2_cfg = datasets.get("building_lod2", {})
        if lod2_cfg.get("status") == "active":
            if _load_lod2 is None:
                log.warning("LoD2: loader not available; skipping.")
            else:
                log.info("LoD2: delegating to existing loader")
                _load_lod2(infdb)

        # -------------------- Load TN (Land Use) --------------------
        tn_cfg = datasets.get("tatsaechliche_nutzung", {})
        if tn_cfg.get("status") == "active":
            _load_tatsaechliche_nutzung(infdb, tn_cfg, base_path, pgurl, target_epsg)

        log.info("OpenData Bavaria: complete.")
        return True

    except Exception as err:
        log.exception(f"An error occurred in OpenData Bavaria loader: {str(err)}")
        return False


# ====================================================================================
# DGM1 LOADER - Digital Terrain Model (Elevation Raster Data)
# ====================================================================================

def _load_dgm1(infdb: InfDB, base_path: Path, target_epsg: int):
    """Load DGM1 clipped exactly to the configured scope polygon."""

    # ---------------------- Configuration -------------------------
    config_base = [infdb.get_toolname(), "sources", "opendata_bavaria", "datasets", "gelaendemodell_1m"]

    # Read loader configuration
    url_template = infdb.get_config_value(config_base + ["url"])  # URL with #scope placeholder
    schema = infdb.get_config_value(config_base + ["schema"]) or "opendata"
    table = infdb.get_config_value(config_base + ["table_name"]) or "gelaendemodell_1m"
    raw_table = f"{table}_raw"
    source_srid = int(infdb.get_config_value(config_base + ["srid"]) or 25832)  # UTM32N
    target_resolution_meters = float(infdb.get_config_value(config_base + ["target_resolution"]) or 10.0)

    # Create output directory structure
    dgm1_base_dir = base_path / "gelaendemodell_1m"
    dgm1_base_dir.mkdir(parents=True, exist_ok=True)

    # Prepare fresh database tables
    with infdb.connect() as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        db.execute_query(f"DROP TABLE IF EXISTS {schema}.{table};")
        db.execute_query(f"DROP TABLE IF EXISTS {schema}.{raw_table};")

    # ==================== 2. SCOPE DETERMINATION ====================
    # Get municipality codes (AGS - Amtlicher Gemeindeschlüssel)
    configured_scope_values = infdb.get_config_value([infdb.get_toolname(), "scope"]) or []
    if isinstance(configured_scope_values, str):
        configured_scope_values = [configured_scope_values]

    # Normalize to 8-digit AGS codes
    ags_municipality_codes = []
    for raw in configured_scope_values:
        v = str(raw).strip()
        if v:
            ags_municipality_codes.append(v.zfill(8)[:8])

    if not ags_municipality_codes:
        log.warning("DGM1: No valid AGS municipality codes configured; skipping loader.")
        return

    # Extract Landkreis codes (first 5 digits) - data is distributed by district
    landkreis_download_codes = sorted({ags[:5] for ags in ags_municipality_codes})
    log.info("DGM1: Landkreise: " + ", ".join(landkreis_download_codes))

    # ==================== 3. EXACT CLIPPING PREPARATION ====================
    # Get precise scope geometry (union of all municipalities)
    clip_wkt, clip_method, _ = utils.get_clip_geometry(source_srid, infdb=infdb, method="exact")

    clip_extent_args = ""  # For gdalwarp bounding box
    cutline_args = ""       # For gdalwarp polygon clipping
    scope_geojson_path = None

    if clip_wkt:
        try:
            # Parse WKT geometry and extract bounding box
            geom = shapely_wkt.loads(clip_wkt)
            minx, miny, maxx, maxy = geom.bounds
            clip_extent_args = f"-te {minx} {miny} {maxx} {maxy} -te_srs EPSG:{source_srid}"
            log.info(f"DGM1: Using exact-geometry bbox: {minx},{miny},{maxx},{maxy}")

            # Export scope polygon as GeoJSON for gdalwarp cutline
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
            
            # Configure gdalwarp to clip to polygon and set outside areas to NoData
            cutline_args = (
                f'-cutline "{scope_geojson_path}" '
                f'-cutline_srs EPSG:{source_srid} '
                "-crop_to_cutline "  # Trim to polygon extent
                "-dstnodata 0 "      # Set pixels outside polygon to 0 (transparent)
            )
        except Exception as e:
            log.error(f"DGM1: Failed to prepare exact clip geometry → {e}")
            clip_extent_args = ""
            cutline_args = ""
            scope_geojson_path = None

    # ==================== 4. DATABASE CONNECTION SETUP ====================
    pgurl = utils._pg_connstring_for_psql(infdb)
    psql_command = f'psql --no-psqlrc -q -v ON_ERROR_STOP=1 -X "{pgurl}"'

    any_data_imported = False

    # ==================== 5. PER-LANDKREIS PROCESSING LOOP ====================
    for landkreis_code in landkreis_download_codes:

        log.info(f"DGM1: processing Landkreis {landkreis_code}")
        landkreis_dir = dgm1_base_dir / landkreis_code
        landkreis_dir.mkdir(parents=True, exist_ok=True)

        # ---------- 5a. DOWNLOAD ----------
        # Fetch raster tiles for this district
        download_url = url_template.replace("#scope", landkreis_code)
        utils.download_aria2c(
            download_url,
            landkreis_dir,
            connections=1,
            allow_overwrite=False,
            auto_file_renaming=False,
        )

        # ---------- 5b. EXTRACT ----------
        # Unzip downloaded archives
        zip_files = utils.get_all_files(str(landkreis_dir), ".zip")
        if zip_files:
            utils.unzip(zip_files, str(landkreis_dir), infdb=infdb)

        # ---------- 5c. FIND SOURCE RASTERS ----------
        # Locate all .tif and .asc files recursively
        raster_source_files = subprocess.check_output(
            ["find", str(landkreis_dir), "-type", "f",
             "(", "-iname", "*.tif", "-o", "-iname", "*.asc", ")", "-print"],
            text=True
        ).strip()

        if not raster_source_files:
            continue  # No raster data for this district

        tile_paths = raster_source_files.splitlines()

        # ---------- 5d. BUILD VIRTUAL RASTER (VRT) ----------
        # Combine all tiles into a single virtual dataset for efficient processing
        virtual_raster_path = landkreis_dir / f"dgm1_{landkreis_code}.vrt"
        tile_list_file = landkreis_dir / "raster_tiles.txt"
        tile_list_file.write_text("\n".join(tile_paths))

        utils.do_cmd(
            f'gdalbuildvrt -resolution highest -r bilinear '
            f'-input_file_list "{tile_list_file}" "{virtual_raster_path}"'
        )

        # ---------- 5e. WARP, RESAMPLE & CLIP ----------
        # Transform to target resolution and clip to exact scope polygon
        output_filename = f"dgm1_{landkreis_code}_{target_resolution_meters}m.tif"
        output_geotiff_path = landkreis_dir / output_filename

        gdalwarp_opts = (
            "-of GTiff "  # Output format
            # Compression and tiling options for efficient storage
            "-co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 "
            "-co BIGTIFF=IF_SAFER -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 "
            "-co NUM_THREADS=ALL_CPUS -r bilinear "  # Resampling method
            f"-t_srs EPSG:{source_srid} "  # Target coordinate system
            f"-tr {target_resolution_meters} {target_resolution_meters} "  # Target resolution
            f"{clip_extent_args} {cutline_args} "  # Spatial clipping
            "-overwrite -multi -wo NUM_THREADS=ALL_CPUS"  # Performance options
        )

        utils.do_cmd(
            f'gdalwarp {gdalwarp_opts} "{virtual_raster_path}" "{output_geotiff_path}"'
        )

        # ---------- 5f. VALIDATE ----------
        # Check output file integrity and log statistics
        try:
            utils.do_cmd(f'gdalinfo "{output_geotiff_path}" -stats -nomd')
            file_size_mb = output_geotiff_path.stat().st_size / 1_000_000
            log.info(f"DGM1: Created clipped raster ({file_size_mb:.1f} MB)")
        except Exception as e:
            log.error(f"DGM1: Warping/clipping failed for {landkreis_code}: {e}")
            output_geotiff_path.unlink(missing_ok=True)
            continue

        # ---------- 5g. IMPORT TO POSTGIS ----------
        # Convert raster to PostGIS format and load into database
        target_table = f"{schema}.{table}"
        mode = "" if not any_data_imported else "-a"  # Append mode after first import

        import_pipeline = (
            "raster2pgsql -q "  # Quiet mode
            f"-s {source_srid} "  # Set SRID
            "-I -M "  # Create index, vacuum analyze
            "-t 256x256 "  # Tile size for efficient queries
            f'"{output_geotiff_path}" {target_table} {mode} | {psql_command}'
        )

        utils.do_cmd(import_pipeline)
        any_data_imported = True

    # ==================== 6. FINALIZATION ====================
    # Create spatial index and update statistics
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


# ====================================================================================
# LAND USE (TN) LOADER - Vector Polygon Data for Actual Land Usage
# ====================================================================================

def _load_tatsaechliche_nutzung(
    infdb: InfDB,
    cfg: dict,
    base_path: Path,
    pgurl: str,
    target_epsg: int
):
    """Load land use (TN) from Nutzung_kreis.gpkg into PostGIS."""

    url = cfg["url"]
    schema = cfg.get("schema", "opendata")
    table = cfg.get("table_name", "tatsaechliche_nutzung")

    tn_dir = base_path / "tatsaechliche_nutzung"
    tn_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = tn_dir / "Nutzung_kreis.gpkg"

    # ==================== 2. DOWNLOAD GPKG ====================
    # Check if we have a valid cached copy (> 1GB indicates complete download)
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

    # ==================== 3. SCHEMA SETUP ====================
    # Ensure target schema exists in database
    engine = infdb.get_db_engine()

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        conn.commit()

    # ==================== 4. SCOPE GEOMETRY CHECK ====================
    # Verify we have a scope polygon for spatial filtering
    clip_wkt, _, _ = utils.get_clip_geometry(target_crs=target_epsg, infdb=infdb, method="exact")
    if not clip_wkt:
        log.warning("TN: No scope geometry found; skipping TN import.")
        return
    else:
        log.info("TN: scope geometry available – will use it for spatial filtering via ogr2ogr.")

    # ==================== 5. LAYER DISCOVERY ====================
    # Enumerate all thematic layers in the GeoPackage
    layer_names = _get_gpkg_layers(gpkg_path)
    if not layer_names:
        raise RuntimeError(f"TN: no layers found in {gpkg_path}")

    log.info(f"TN: Nutzung_kreis.gpkg contains {len(layer_names)} layers.")

    # ==================== 6. PER-LAYER IMPORT ====================
    # Import each layer, applying spatial filter to keep only features in scope
    is_first_import = True  # First import creates table, rest append
    total_rows_before = 0
    total_rows_imported = 0

    # Get initial row count (in case table exists from previous run)
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

            # Import via ogr2ogr with spatial filter
            # scope=True triggers utils.import_layers to:
            # - Apply spatial filter using configured scope geometry
            # - Only copy features that intersect the scope polygon
            utils.import_layers(
                input_file=str(gpkg_path),
                layers=[layer_name],
                schema=schema,
                infdb=infdb,
                layer_names=[table],
                scope=True,  # Enable spatial filtering
                overwrite=(is_first_import),  # First layer creates, rest append
            )
            is_first_import = False

            # Track import progress
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

    # ==================== 7. FINALIZATION ====================
    # Create spatial index for efficient spatial queries
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

    if not utils.if_active("lod2", infdb):
        return True


    base_path = infdb.get_config_path([infdb.get_toolname(), "sources", "lod2", "path", "lod2"], type="loader")
    os.makedirs(base_path, exist_ok=True)

    # Directory for extracted GML files
    gml_path = infdb.get_config_path([infdb.get_toolname(), "sources", "lod2", "path", "gml"], type="loader")
    os.makedirs(gml_path, exist_ok=True)

    # ==================== 3. SCOPE PROCESSING ====================
    # Get list of administrative codes (AGS) defining spatial scope
    scope = infdb.get_config_value([infdb.get_toolname(), "scope"])
    if isinstance(scope, str):
        scope = [scope]

    # Download CityGML files for each administrative region
    url_cfg = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2", "url"])
    for ags in scope or []:
        # Construct download URL by replacing placeholder with AGS code
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
            allow_overwrite=False,  # Skip if already downloaded
            auto_file_renaming=False
        )

    # ==================== 4. CITYDB IMPORT ====================
    # Import CityGML into PostGIS using 3D City Database (3DCityDB) CLI tool
    params: Dict[str, str] = infdb.get_db_parameters_dict()
    import_mode: Optional[str] = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2", "import-mode"])

    # Build citydb import command with database connection parameters
    cmd_parts: List[str] = [
        "citydb import citygml",
        "-H", params["host"],           # Database host
        "-d", params["db"],             # Database name
        "-u", params["user"],           # Username
        "-p", params["password"],       # Password
        "-P", str(params["exposed_port"]),  # Port
        f"--import-mode={import_mode}", # Import mode (e.g., import, delete-import)
        str(gml_path),                  # Directory containing GML files
    ]
    utils.do_cmd(" ".join(str(a) for a in cmd_parts))

    # ==================== 5. POST-PROCESSING ====================
    # Execute SQL to create simplified building table/view from 3DCityDB schema
    format_params = {"output_schema": "opendata"}
    with infdb.connect() as db:
        db.execute_sql_file("sql/buildings_lod2.sql", format_params)

    log.info("LOD2 data loaded successfully")
    return True

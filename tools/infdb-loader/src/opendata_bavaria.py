import os
import logging
import subprocess
import json
import re
from pathlib import Path
from sqlalchemy import text
from typing import Dict, List, Optional

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


# ==================== DGM1 Loader ====================

def _load_dgm1(infdb: InfDB, base_path: Path, target_epsg: int):
    """Load DGM1 (Digital Elevation Model 1m resolution) terrain rasters into PostGIS."""

    # Configuration via infdb.get_config_value()
    config_base = [TOOL_NAME, "sources", "opendata_bavaria", "datasets", "gelaendemodell_1m"]
    
    url_template = infdb.get_config_value(config_base + ["url"])
    schema = infdb.get_config_value(config_base + ["schema"]) or "opendata"
    table = infdb.get_config_value(config_base + ["table_name"]) or "gelaendemodell_1m"
    import_mode = infdb.get_config_value(config_base + ["import-mode"]) or "delete"
    source_srid = int(infdb.get_config_value(config_base + ["srid"]) or 25832)
    target_resolution_meters = float(infdb.get_config_value(config_base + ["target_resolution"]) or 10.0)

    # Setup directories
    dgm1_base_dir = base_path / "gelaendemodell_1m"
    dgm1_base_dir.mkdir(parents=True, exist_ok=True)

    # Ensure schema exists
    with infdb.connect() as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Get clipping bounding box (rectangular extent for GDAL)
    clipping_bbox = None
    clip_wkt, clip_method, _ = utils.get_clip_geometry(target_crs=source_srid, method='bbox')
    if clip_wkt:
        # Extract bbox coordinates from WKT POLYGON
        coordinates = re.findall(r"([-\d.]+)\s+([-\d.]+)", clip_wkt)
        if len(coordinates) >= 4:
            x_coords = [float(c[0]) for c in coordinates]
            y_coords = [float(c[1]) for c in coordinates]
            clipping_bbox = (min(x_coords), min(y_coords), max(x_coords), max(y_coords))
            log.info(f"DGM1: clipping to bounding box {clipping_bbox}")

    # Get administrative district codes (5-digit Landkreis codes)
    scope_list = infdb.get_config_value([TOOL_NAME, "scope"]) or []
    if isinstance(scope_list, str):
        scope_list = [scope_list]
    landkreis_codes = sorted({str(code)[:5] for code in scope_list if str(code)})
    
    if not landkreis_codes:
        log.warning("DGM1: no scope codes configured; skipping.")
        return

    # PostgreSQL connection string via utils
    pgurl = utils._pg_connstring_for_psql()
    psql_command = f'psql --no-psqlrc -q -v ON_ERROR_STOP=1 -X "{pgurl}"'
    
    any_data_imported = False

    # Process each Landkreis (administrative district)
    for landkreis_code in landkreis_codes:
        log.info(f"DGM1: processing Landkreis {landkreis_code}")
        
        # Create directory for this district
        landkreis_dir = dgm1_base_dir / landkreis_code
        landkreis_dir.mkdir(parents=True, exist_ok=True)

        # ========== Step 1: Download ZIP archives ==========
        download_url = url_template.replace("#scope", landkreis_code)
        log.info(f"DGM1: downloading data for {landkreis_code}...")
        utils.download_aria2c(
            url=download_url,
            output_dir=landkreis_dir,
            connections=1,  # Metalink files are small
            allow_overwrite=False,
            auto_file_renaming=False
        )

        # ========== Step 2: Extract ZIP archives ==========
        log.info(f"DGM1: extracting archives for {landkreis_code}...")
        zip_files = utils.get_all_files(str(landkreis_dir), ".zip")
        if zip_files:
            log.info(f"DGM1: found {len(zip_files)} ZIP files → extracting...")
            utils.unzip(zip_files, str(landkreis_dir))
        else:
            log.info(f"DGM1: no ZIP files found in {landkreis_dir} ")

        # ========== Step 3: Find raster source files ==========
        raster_source_files = subprocess.check_output(
            ["find", str(landkreis_dir), "-type", "f", 
             "(", "-iname", "*.tif", "-o", "-iname", "*.asc", ")", "-print"],
            text=True
        ).strip()

        if not raster_source_files:
            log.warning(f"DGM1: no raster tiles found in {landkreis_dir}; skipping.")
            continue

        tile_count = len(raster_source_files.splitlines())
        log.info(f"DGM1: found {tile_count} raster tiles")

        # ========== Step 4: Build VRT (Virtual Raster Dataset) ==========
        # VRT = Virtual format that references multiple raster files as one dataset
        virtual_raster_path = landkreis_dir / f"dgm1_{landkreis_code}.vrt"
        tile_list_file = landkreis_dir / "raster_tiles.txt"
        tile_list_file.write_text(raster_source_files.replace(" ", "\n"), encoding="utf-8")
        
        log.info(f"DGM1: building virtual raster mosaic from {tile_count} tiles...")
        utils.do_cmd(
            f'gdalbuildvrt -resolution highest -r bilinear '
            f'-input_file_list "{tile_list_file}" "{virtual_raster_path}"'
        )

        # ========== Step 5: Create COG (Cloud Optimized GeoTIFF) ==========
        # COG = Optimized GeoTIFF format for efficient cloud storage and streaming
        if clipping_bbox:
            output_filename = f"dgm1_{landkreis_code}_crop_{target_resolution_meters}m.tif"
        else:
            output_filename = f"dgm1_{landkreis_code}_{target_resolution_meters}m.tif"
        
        output_geotiff_path = landkreis_dir / output_filename
        temp_geotiff_path = landkreis_dir / f"{output_filename}.tmp"

        # GDAL warp options for optimized GeoTIFF creation
        gdalwarp_options = (
            f'-of GTiff '                          # Output format: GeoTIFF
            f'-co TILED=YES '                      # Use tiled structure (faster access)
            f'-co COMPRESS=DEFLATE '               # Lossless compression
            f'-co PREDICTOR=2 '                    # Predictor for better compression
            f'-co BIGTIFF=IF_SAFER '               # Use BigTIFF if needed (>4GB)
            f'-co BLOCKXSIZE=512 '                 # Tile width
            f'-co BLOCKYSIZE=512 '                 # Tile height
            f'-co NUM_THREADS=ALL_CPUS '           # Use all CPU cores for compression
            f'-r bilinear '                        # Bilinear resampling (smooth)
            f'-t_srs EPSG:{source_srid} '          # Target spatial reference
            f'-tr {target_resolution_meters} {target_resolution_meters} '  # Output resolution
            f'-overwrite '                         # Overwrite if exists
            f'-multi '                             # Multi-threaded warping
            f'-wo NUM_THREADS=ALL_CPUS'            # Warp operation threads
        )

        if clipping_bbox:
            bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax = clipping_bbox
            
            # Calculate expected output dimensions
            output_width_pixels = int((bbox_xmax - bbox_xmin) / target_resolution_meters)
            output_height_pixels = int((bbox_ymax - bbox_ymin) / target_resolution_meters)
            output_megapixels = (output_width_pixels * output_height_pixels) / 1_000_000
            
            log.info(
                f"DGM1: creating {target_resolution_meters}m resolution raster "
                f"({output_width_pixels}×{output_height_pixels} = {output_megapixels:.1f}M pixels)..."
            )
            
            utils.do_cmd(
                f'gdalwarp {gdalwarp_options} '
                f'-te {bbox_xmin} {bbox_ymin} {bbox_xmax} {bbox_ymax} '  # Target extent
                f'"{virtual_raster_path}" "{temp_geotiff_path}"'
            )
        else:
            log.info(f"DGM1: creating {target_resolution_meters}m resolution raster (full extent)...")
            utils.do_cmd(
                f'gdalwarp {gdalwarp_options} '
                f'"{virtual_raster_path}" "{temp_geotiff_path}"'
            )

        # ========== Step 6: Validate and publish GeoTIFF ==========
        try:
            # Validate GeoTIFF (generates statistics)
            utils.do_cmd(f'gdalinfo "{temp_geotiff_path}" -stats -nomd')
            
            # Move temp file to final location
            temp_geotiff_path.replace(output_geotiff_path)
            
            file_size_mb = output_geotiff_path.stat().st_size / 1_000_000
            log.info(f"DGM1: created {output_geotiff_path.name} ({file_size_mb:.1f} MB)")
            
        except Exception as e:
            log.error(f"DGM1: invalid GeoTIFF for {landkreis_code}: {e}")
            continue

        # ========== Step 7: Import to PostGIS ==========
        log.info(f"DGM1: importing {landkreis_code} to PostGIS...")
        
        # Determine if we should append or create new table
        raster2pgsql_mode = "-a" if any_data_imported or import_mode == "append" else ""
        
        # Build pipeline: raster2pgsql → psql
        import_pipeline = (
            f'raster2pgsql '
            f'-q '                    # Quiet mode
            f'-s {source_srid} '      # Spatial reference ID
            f'-I '                    # Create spatial index
            f'-C '                    # Add raster constraints
            f'-M '                    # Vacuum analyze after import
            f'-t 256x256 '            # Tile size (256x256 pixels)
            f'"{output_geotiff_path}" '
            f'{schema}.{table} '
            f'{raster2pgsql_mode} | '
            f'{psql_command}'
        )
        
        utils.do_cmd(import_pipeline)
        any_data_imported = True
        log.info(f"DGM1: successfully imported {landkreis_code}")

    # ========== Step 8: Final indexing and optimization ==========
    if any_data_imported:
        log.info(f"DGM1: creating spatial index on {schema}.{table}...")
        utils.do_cmd(
            f'{psql_command} -c '
            f'"CREATE INDEX IF NOT EXISTS {table}_rast_gix '
            f'ON {schema}.{table} USING GIST(ST_ConvexHull(rast));"'
        )
        
        log.info(f"DGM1: analyzing table statistics...")
        utils.do_cmd(f'{psql_command} -c "ANALYZE {schema}.{table};"')
        
        log.info(f"DGM1: import complete → {schema}.{table}")
    else:
        log.warning("DGM1: no data was imported")


# ==================== Land Use Loader ====================

def _load_tatsaechliche_nutzung(infdb: InfDB, cfg: dict, base_path: Path, pgurl: str, target_epsg: int):
    """Load land use (TN) GPKG into PostGIS."""
    url = cfg["url"]
    schema = cfg.get("schema", "opendata")
    table = cfg.get("table_name", "tatsaechliche_nutzung")
    import_mode = cfg.get("import-mode", "delete")

    tn_dir = base_path / "tatsaechliche_nutzung"
    tn_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = tn_dir / "Nutzung_kreis.gpkg"

    # Download if needed (skip if already 1GB+)
    if gpkg_path.exists() and gpkg_path.stat().st_size > 1_000_000_000:
        log.info(f"TN: using existing {gpkg_path.stat().st_size / 1e9:.1f} GB file")
    else:
        log.info(f"TN: downloading from {url}")
        utils.download_aria2c(
            url=url,
            output_dir=tn_dir,
            output_filename="Nutzung_kreis.gpkg",
            connections=4,  # Large file, use multiple connections
            max_connection_per_server=4
        )

    # Ensure schema exists
    conf = InfdbConfig(tool_name=TOOL_NAME, config_path=CONFIG_DIR)
    client = InfdbClient(conf, log, db_name=DB_NAME)
    engine = client.get_db_engine()
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        conn.commit()

    # Get all layers
    layers = _get_gpkg_layers(gpkg_path)
    if not layers:
        raise RuntimeError(f"TN: no layers in {gpkg_path}")

    log.info(f"TN: found {len(layers)} layers")

    # Order layers by proximity to scope centroid
    scope_centroid = None
    clip_wkt, _, _ = utils.get_clip_geometry(target_crs=target_epsg, method='exact')
    if clip_wkt:
        from shapely import wkt as shapely_wkt
        geom = shapely_wkt.loads(clip_wkt)
        scope_centroid = (geom.centroid.x, geom.centroid.y)

    # Sort layers by distance to centroid
    layer_distances = []
    for lyr in layers:
        lyr_epsg, bbox = _get_layer_spatial_info(gpkg_path, lyr)
        if scope_centroid and bbox and (lyr_epsg == target_epsg or lyr_epsg in (None, 0)):
            cx = (bbox[0] + bbox[2]) / 2.0
            cy = (bbox[1] + bbox[3]) / 2.0
            dist = _distance_2d(scope_centroid, (cx, cy))
            layer_distances.append((lyr, dist))

    layer_distances.sort(key=lambda x: x[1])
    ordered_layers = [lyr for lyr, _ in layer_distances]
    ordered_layers.extend([lyr for lyr in layers if lyr not in ordered_layers])

    log.info(f"TN: trying {len(ordered_layers)} layers (nearest first)")

    # Import layers
    first = True
    for lyr in ordered_layers:
        try:
            utils.import_layers(
                input_file=str(gpkg_path),
                layers=[lyr],
                schema=schema,
                layer_names=[table],
                scope=True,
                overwrite=(import_mode != "append" and first)
            )
            first = False

            # Check if we got data
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT EXISTS(SELECT 1 FROM {schema}.{table} LIMIT 1)"))
                has_rows = result.scalar()
                
                if has_rows:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
                    row_count = result.scalar()
                    log.info(f"TN: imported {row_count:,} rows from '{lyr}'; done.")
                    break

        except Exception as e:
            log.warning(f"TN: layer '{lyr}' failed: {e}")
            continue

    # Create index
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {table}_geom_gix ON {schema}.{table} USING GIST(geom);"))
        conn.commit()
    
    log.info(f"TN: indexed {schema}.{table}")

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

"""
OpenData Bavaria loader: DGM1 (terrain model), LoD2 buildings, and land use (TN).

Optimizations:
  - Uses InfDB package for config and logging
  - Reuses shared clipping logic from utils
  - Cleaner structure with consistent patterns
  - Removed redundant code
"""
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

def _gpkg_layers(gpkg: Path) -> list[str]:
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


def _gpkg_layer_extent_epsg(gpkg: Path, layer: str) -> tuple[int | None, tuple[float, ...] | None]:
    """Return (epsg, bbox) tuple parsed from ogrinfo."""
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
    """Load OpenData Bavaria datasets: DGM1, LoD2, and TN.
    
    Args:
        infdb: InfDB instance for config and logging
        
    Returns:
        True on success, False on failure
    """
    try:
        global log
        log = infdb.get_worker_logger()

        if not utils.if_active("opendata_bavaria"):
            return True

        # Enable PostGIS raster extension in public schema (required!)
        try:
            with infdb.connect() as db:
                db.execute_query("CREATE EXTENSION IF NOT EXISTS postgis_raster SCHEMA public CASCADE;")
                log.info("PostGIS raster extension enabled")
        except Exception as ext_err:
            log.warning(f"Could not enable PostGIS raster extension: {ext_err}")
            log.info("Continuing without raster support (will only create COG files)")

        # Get base path
        base_path = Path(infdb.get_config_path([TOOL_NAME, "path", "opendata"], type="loader"))
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
            _load_dgm1(infdb, dgm1_cfg, base_path, pgurl, target_epsg)

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

def _load_dgm1(infdb: InfDB, cfg: dict, base_path: Path, pgurl: str, target_epsg: int):
    """Load DGM1 terrain rasters into PostGIS."""

    url_template = cfg["url"]
    schema = cfg.get("schema", "opendata")
    table = cfg.get("table_name", "gelaendemodell_1m")
    import_mode = cfg.get("import-mode", "delete")
    srid = int(cfg.get("srid", 25832))

    # Get target resolution from config (default 10m for speed)
    target_resolution = float(cfg.get("target_resolution", 10.0))

    dgm1_dir = base_path / "gelaendemodell_1m"
    dgm1_dir.mkdir(parents=True, exist_ok=True)

    # Ensure schema exists
    with infdb.connect() as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Get clipping bbox using shared utils
    bbox = None
    clip_wkt, clip_method, _ = utils.get_clip_geometry(target_crs=srid, method='bbox')
    if clip_wkt:
        # Extract bbox from WKT POLYGON
        coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", clip_wkt)
        if len(coords) >= 4:
            xs = [float(c[0]) for c in coords]
            ys = [float(c[1]) for c in coords]
            bbox = (min(xs), min(ys), max(xs), max(ys))
            log.info(f"DGM1: clipping to bbox {bbox}")

    # Get scope codes (5-digit Landkreis)
    scope_list = infdb.get_config_value([TOOL_NAME, "scope"]) or []
    if isinstance(scope_list, str):
        scope_list = [scope_list]
    kreis5_list = sorted({str(a)[:5] for a in scope_list if str(a)})
    
    if not kreis5_list:
        log.warning("DGM1: no scope codes; skipping.")
        return

    _psql_quiet = f'psql --no-psqlrc -q -v ON_ERROR_STOP=1 -X "{pgurl}"'

    created_any = False

    for kreis5 in kreis5_list:
        log.info(f"DGM1: processing Landkreis {kreis5}")
        
        k_dir = dgm1_dir / kreis5
        k_dir.mkdir(parents=True, exist_ok=True)

        # Download metalink
        meta_url = url_template.replace("#scope", kreis5)
        utils.do_cmd(
            f'aria2c -c --allow-overwrite=false --auto-file-renaming=false '
            f'--summary-interval=60 --console-log-level=warn '
            f'"{meta_url}" -d "{k_dir}"'
        )

        # Unzip archives
        utils.do_cmd(f'find "{k_dir}" -iname "*.zip" -exec unzip -n {{}} -d "{k_dir}" \\;')

        # Find raster tiles
        srcs = subprocess.check_output(
            ["find", str(k_dir), "-type", "f", "(", "-iname", "*.tif", "-o", "-iname", "*.asc", ")", "-print"],
            text=True
        ).strip()

        if not srcs:
            log.warning(f"DGM1: no rasters found in {k_dir}; skipping.")
            continue

        # Build VRT
        vrt = k_dir / f"dgm1_{kreis5}.vrt"
        listfile = k_dir / "inputs.txt"
        listfile.write_text(srcs.replace(" ", "\n"), encoding="utf-8")
        utils.do_cmd(f'gdalbuildvrt -resolution highest -r bilinear -input_file_list "{listfile}" "{vrt}"')

        # Create COG with target resolution
        cog_name = f"dgm1_{kreis5}_crop_{target_resolution}m.tif" if bbox else f"dgm1_{kreis5}_{target_resolution}m.tif"
        final_cog = k_dir / cog_name
        tmp_cog = k_dir / f"{cog_name}.tmp"

        #Add -tr flag for resolution control
        gdalwarp_opts = (
            f'-of GTiff -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 '
            f'-co BIGTIFF=IF_SAFER -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 '
            f'-co NUM_THREADS=ALL_CPUS '  # Use all CPU cores
            f'-r bilinear -t_srs EPSG:{srid} -overwrite '
            f'-tr {target_resolution} {target_resolution} '  # Resolution control
            f'-multi -wo NUM_THREADS=ALL_CPUS'  # Multi-threaded warping
        )

        if bbox:
            xmin, ymin, xmax, ymax = bbox
            # Calculate expected output size
            expected_width = int((xmax - xmin) / target_resolution)
            expected_height = int((ymax - ymin) / target_resolution)
            expected_pixels = expected_width * expected_height
            log.info(
                f"DGM1: creating {target_resolution}m raster "
                f"({expected_width}×{expected_height} = {expected_pixels/1e6:.1f}M pixels)..."
            )
            utils.do_cmd(f'gdalwarp {gdalwarp_opts} -te {xmin} {ymin} {xmax} {ymax} "{vrt}" "{tmp_cog}"')
        else:
            utils.do_cmd(f'gdalwarp {gdalwarp_opts} "{vrt}" "{tmp_cog}"')

        # Validate and publish
        try:
            utils.do_cmd(f'gdalinfo "{tmp_cog}" -stats -nomd')
            tmp_cog.replace(final_cog)
            file_size_mb = final_cog.stat().st_size / 1_000_000
            log.info(f"DGM1: created {final_cog.name} ({file_size_mb:.1f} MB)")
        except Exception as e:
            log.error(f"DGM1: invalid COG for {kreis5}: {e}")
            continue

        # Import to PostGIS
        append_flag = "-a" if created_any or import_mode == "append" else ""
        pipe = (
            f'raster2pgsql -q -s {srid} -I -C -M -t 256x256 "{final_cog}" {schema}.{table} {append_flag} | '
            f'{_psql_quiet}'
        )
        utils.do_cmd(pipe)
        created_any = True

    # Final indexing
    if  created_any:
        utils.do_cmd(f'{_psql_quiet} -c "CREATE INDEX IF NOT EXISTS {table}_rast_gix ON {schema}.{table} USING GIST(ST_ConvexHull(rast));"')
        utils.do_cmd(f'{_psql_quiet} -c "ANALYZE {schema}.{table};"')
        log.info(f"DGM1: imported into {schema}.{table}")


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
        utils.do_cmd(
            f'aria2c -c -x4 -s4 --summary-interval=60 --console-log-level=warn '
            f'-d "{tn_dir}" -o "Nutzung_kreis.gpkg" "{url}"'
        )

    # Ensure schema exists
    conf = InfdbConfig(tool_name=TOOL_NAME, config_path=CONFIG_DIR)
    client = InfdbClient(conf, log, db_name=DB_NAME)
    engine = client.get_db_engine()
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        conn.commit()

    # Get all layers
    layers = _gpkg_layers(gpkg_path)
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
        lyr_epsg, bbox = _gpkg_layer_extent_epsg(gpkg_path, lyr)
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
        cmd = f"{"aria2c --continue=true --allow-overwrite=false --auto-file-renaming=false"} {url} -d {gml_path}"
        utils.do_cmd(cmd)

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

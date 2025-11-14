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
import shutil
from pathlib import Path
from sqlalchemy import text

from infdb import InfdbConfig, InfDB, InfdbClient
from . import utils

# Optional: reuse existing LoD2 loader if enabled
try:
    from . import lod2 as lod2_loader
except ImportError:
    lod2_loader = None

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
            if lod2_loader is None:
                log.warning("LoD2: loader not available; skipping.")
            else:
                log.info("LoD2: delegating to existing loader")
                lod2_loader.load(infdb)

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

    # Check if raster2pgsql is available
    can_import = bool(shutil.which("raster2pgsql"))
    if not can_import:
        log.warning("DGM1: 'raster2pgsql' not found; will only create COG files.")

    # Drop table if delete mode
    _psql_quiet = f'psql --no-psqlrc -q -v ON_ERROR_STOP=1 -X "{pgurl}"'
    if import_mode == "delete" and can_import:
        os.system(f'{_psql_quiet} -c "DROP TABLE IF EXISTS {schema}.{table} CASCADE;" > /dev/null 2>&1')

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

        # Create COG (Cloud-Optimized GeoTIFF)
        cog_name = f"dgm1_{kreis5}_crop.tif" if bbox else f"dgm1_{kreis5}.tif"
        final_cog = k_dir / cog_name
        tmp_cog = k_dir / f"{cog_name}.tmp"

        gdalwarp_opts = (
            f'-of GTiff -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 '
            f'-co BIGTIFF=IF_SAFER -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 '
            f'-r bilinear -t_srs EPSG:{srid} -overwrite'
        )

        if bbox:
            xmin, ymin, xmax, ymax = bbox
            utils.do_cmd(f'gdalwarp {gdalwarp_opts} -te {xmin} {ymin} {xmax} {ymax} "{vrt}" "{tmp_cog}"')
        else:
            utils.do_cmd(f'gdalwarp {gdalwarp_opts} "{vrt}" "{tmp_cog}"')

        # Validate and publish
        try:
            utils.do_cmd(f'gdalinfo "{tmp_cog}" -stats -nomd')
            tmp_cog.replace(final_cog)
            log.info(f"DGM1: created {final_cog}")
        except Exception:
            log.error(f"DGM1: invalid COG for {kreis5}; skipping.")
            continue

        # Import to PostGIS
        if can_import:
            append_flag = "-a" if created_any or import_mode == "append" else ""
            pipe = (
                f'raster2pgsql -q -s {srid} -I -C -M -t 256x256 "{final_cog}" {schema}.{table} {append_flag} | '
                f'{_psql_quiet}'
            )
            try:
                utils.do_cmd(pipe)
                created_any = True
                log.info(f"DGM1: imported {kreis5}")
            except Exception as e:
                log.error(f"DGM1: import failed for {kreis5}: {e}")

    # Final indexing
    if can_import and created_any:
        try:
            utils.do_cmd(f'{_psql_quiet} -c "CREATE INDEX IF NOT EXISTS {table}_rast_gix ON {schema}.{table} USING GIST(ST_ConvexHull(rast));"')
            utils.do_cmd(f'{_psql_quiet} -c "ANALYZE {schema}.{table};"')
            log.info(f"DGM1: indexed {schema}.{table}")
        except Exception as e:
            log.warning(f"DGM1: indexing failed: {e}")


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

# opendata_bavaria.py
import os
import logging
import subprocess
import json
import re
from pathlib import Path

from . import config, utils, logger
from infdb import InfDB

# optional: reuse existing LoD2 loader if enabled
try:
    from . import lod2 as lod2_loader
except Exception:
    lod2_loader = None

log = logging.getLogger(__name__)

# ---------- helpers ----------

def _gpkg_has_layer(gpkg: Path, layer_name: str) -> bool:
    """Return True if gpkg contains layer_name (case-sensitive check)."""
    try:
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", "-json", str(gpkg)], text=True)
        data = json.loads(out)
        layers = [lyr.get("name") for lyr in data.get("layers", []) if "name" in lyr]
    except Exception:
        # fallback to text parsing
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", str(gpkg)], text=True, stderr=subprocess.STDOUT)
        layers = []
        for line in out.splitlines():
            line = line.strip()
            if ":" in line and "(" in line:
                name_part = line.split(":", 1)[1].strip()
                name = name_part.split("(")[0].strip()
                if name:
                    layers.append(name)
    return layer_name in layers

def _gpkg_first_layer(gpkg: Path) -> str:
    """Return the first layer name found in the GPKG, raise if none."""
    try:
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", "-json", str(gpkg)], text=True)
        data = json.loads(out)
        layers = [lyr.get("name") for lyr in data.get("layers", []) if "name" in lyr]
    except Exception:
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", str(gpkg)], text=True, stderr=subprocess.STDOUT)
        layers = []
        for line in out.splitlines():
            line = line.strip()
            if ":" in line and "(" in line:
                name_part = line.split(":", 1)[1].strip()
                name = name_part.split("(")[0].strip()
                if name:
                    layers.append(name)
    if not layers:
        raise RuntimeError(f"No layers found in {gpkg}")
    return layers[0]

def _gpkg_layers(gpkg: Path):
    """List layer names in a GPKG."""
    try:
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", "-json", str(gpkg)], text=True)
        data = json.loads(out)
        return [lyr.get("name") for lyr in data.get("layers", []) if "name" in lyr]
    except Exception:
        out = subprocess.check_output(["ogrinfo", "-ro", "-q", str(gpkg)], text=True, stderr=subprocess.STDOUT)
        names = []
        for line in out.splitlines():
            line = line.strip()
            if ":" in line and "(" in line:
                name_part = line.split(":", 1)[1].strip()
                name = name_part.split("(")[0].strip()
                if name:
                    names.append(name)
        return names

def _gpkg_fields(gpkg: Path, layer: str):
    """List field names for a layer in a GPKG (simple parse of ogrinfo -so)."""
    out = subprocess.check_output(["ogrinfo", "-so", "-ro", "-q", str(gpkg), layer], text=True)
    fields = []
    for ln in out.splitlines():
        ln = ln.strip()
        # lines look like: "AGS: String (0.0)"
        if ":" in ln and "(" in ln and ")" in ln:
            fname = ln.split(":", 1)[0].strip()
            fields.append(fname)
    return fields

def _gpkg_layer_extent_epsg(gpkg: Path, layer: str):
    """
    Return (epsg:int|None, (minx,miny,maxx,maxy)) parsed from 'ogrinfo -so'.
    """
    out = subprocess.check_output(["ogrinfo", "-so", "-ro", "-q", str(gpkg), layer], text=True)
    epsg = None
    m = re.search(r"EPSG:(\d+)", out)
    if m:
        epsg = int(m.group(1))
    # Extent: (654000.000, 5395000.000) - (669000.000, 5407000.000)
    bb = None
    m2 = re.search(r"Extent:\s*\(([-\d\.]+),\s*([-\d\.]+)\)\s*-\s*\(([-\d\.]+),\s*([-\d\.]+)\)", out)
    if m2:
        bb = (float(m2.group(1)), float(m2.group(2)), float(m2.group(3)), float(m2.group(4)))
    return epsg, bb

def _bbox_intersects(a, b):
    if not a or not b: return False
    ax1, ay1, ax2, ay2 = a; bx1, by1, bx2, by2 = b
    return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)

# ---------- main ----------

def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("opendata_bavaria"):
        return

    # keep everything under the same tree as other sources
    base_path = config.get_path(["loader", "path", "opendata"])
    os.makedirs(base_path, exist_ok=True)

    ags_list = config.get_list(["loader", "scope"]) or []
    datasets = (config.get_value(["loader", "sources", "opendata_bavaria", "datasets"]) or {})

    # ==========================================================
    # 1) Geländemodell Bayern 1m (DGM1) -> VRT -> gdalwarp crop -> COG -> PostGIS
    # ==========================================================
    dgm1_cfg = datasets.get("gelaendemodell_1m", {})
    if dgm1_cfg.get("status") == "active":
        import shutil, subprocess

        dgm1_url_tpl = dgm1_cfg["url"]  # .../dgm1/meta/metalink/#scope.meta4
        dgm1_root = Path(base_path) / "gelaendemodell_1m"
        dgm1_root.mkdir(parents=True, exist_ok=True)

        schema = dgm1_cfg.get("schema", "opendata")
        table = dgm1_cfg.get("table_name", "gelaendemodell_1m")
        fqtn = f"{schema}.{table}"
        import_mode = dgm1_cfg.get("import-mode", "delete")
        srid = int(dgm1_cfg.get("srid", 25832))

        params = utils.get_db_parameters("postgres")
        pgurl = f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:{params["exposed_port"]}/{params["db"]}'

        # ---------- quiet psql wrapper ----------
        _PSQL = f'psql --no-psqlrc -q -v ON_ERROR_STOP=1 -X "{pgurl}"'
        def _pg_ok(sql: str) -> bool:
            # return True if command succeeded (and keep logs quiet)
            rc = os.system(f'{_PSQL} -c "{sql}" > /dev/null')
            return rc == 0

        # Ensure schema exists (idempotent)
        _pg_ok(f'CREATE SCHEMA IF NOT EXISTS {schema};')


        # ---------- scope bbox ----------
        bbox = None
        try:
            gdf_scope = utils.get_envelop()
            if gdf_scope is not None and not gdf_scope.empty:
                bb = gdf_scope.to_crs(epsg=srid).total_bounds
                bbox = (bb[0], bb[1], bb[2], bb[3])
                log.info(f"DGM1: crop bbox EPSG:{srid} = {bb[0]:.2f},{bb[1]:.2f} — {bb[2]:.2f},{bb[3]:.2f}")
            else:
                log.warning("DGM1: scope envelope is empty; importing full Landkreis tiles.")
        except Exception as e:
            log.warning(f"DGM1: could not compute scope bbox; importing full Landkreis tiles. Reason: {e}")

        # ---------- derive Landkreis (5-digit) list ----------
        ags_list = config.get_list(["loader", "scope"]) or []
        kreis5_list = sorted({str(a)[:5] for a in ags_list if str(a)})
        if not kreis5_list:
            log.warning("DGM1: no scope codes given; using raw AGS list as fallback.")
            kreis5_list = [str(a) for a in ags_list]

        # ---------- DROP TABLE in delete-mode ----------
        if import_mode == "delete" and can_import_to_db:
            utils.do_cmd(f'{_PSQL} -c "DROP TABLE IF EXISTS {fqtn} CASCADE;"')

        # Make sure raster2pgsql exists if we plan to import
        raster2pgsql_path = shutil.which("raster2pgsql")
        if can_import_to_db and not raster2pgsql_path:
            log.warning("DGM1: 'raster2pgsql' not found in PATH; will skip DB import and only build COG(s).")
            can_import_to_db = False

        created_any = False

        for kreis5 in kreis5_list:
            k_dir = dgm1_root / kreis5
            k_dir.mkdir(parents=True, exist_ok=True)

            meta_url = dgm1_url_tpl.replace("#scope", kreis5)
            log.info("DGM1: downloading Landkreis %s from %s", kreis5, meta_url)
            utils.do_cmd(
                f'aria2c -c --allow-overwrite=false --auto-file-renaming=false '
                f'--summary-interval=60 --console-log-level=warn '
                f'"{meta_url}" -d "{k_dir}"'
            )

            utils.do_cmd(f'find "{k_dir}" -iname "*.zip" -print0 | xargs -0 -I{{}} unzip -n "{{}}" -d "{k_dir}"')

            # Gather source rasters
            src_list_cmd = f'find "{k_dir}" -type f \\( -iname "*.tif" -o -iname "*.asc" \\) -printf "%p "'
            try:
                srcs = subprocess.check_output(["bash", "-lc", src_list_cmd], text=True).strip()
            except Exception:
                srcs = ""

            if not srcs:
                log.warning("DGM1: no raster tiles found under %s (Landkreis %s). Skipping.", k_dir, kreis5)
                continue

            # --- Build VRT from a list file (safer than long argv) ---
            vrt = k_dir / f"dgm1_{kreis5}.vrt"
            listfile = k_dir / f"dgm1_{kreis5}_inputs.txt"
            try:
                with open(listfile, "w", encoding="utf-8") as f:
                    for p in srcs.split():
                        f.write(p + "\n")
            except Exception as e:
                log.error("DGM1: failed to write listfile %s: %s", listfile, e)
                continue

            utils.do_cmd(f'gdalbuildvrt -resolution highest -r bilinear -input_file_list "{listfile}" "{vrt}"')

            # --- Crop/reproject to COG using a TEMP file, validate, then publish ---
            final_cog = k_dir / (f"dgm1_{kreis5}_crop.tif" if bbox else f"dgm1_{kreis5}.tif")
            tmp_cog   = k_dir / (final_cog.name + ".tmp")
            if tmp_cog.exists():
                try: tmp_cog.unlink()
                except: pass

            gdalwarp_common = (
                f'-of GTiff -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 '
                f'-co BIGTIFF=IF_SAFER -co COPY_SRC_OVERVIEWS=YES '
                f'-co BLOCKXSIZE=512 -co BLOCKYSIZE=512 '
                f'-r bilinear -t_srs EPSG:{srid} -overwrite '
            )

            if bbox:
                xmin, ymin, xmax, ymax = bbox
                utils.do_cmd(f'gdalwarp {gdalwarp_common} -te {xmin} {ymin} {xmax} {ymax} "{vrt}" "{tmp_cog}"')
            else:
                utils.do_cmd(f'gdalwarp {gdalwarp_common} "{vrt}" "{tmp_cog}"')

            # Validate the temp output before publishing
            try:
                utils.do_cmd(f'gdalinfo "{tmp_cog}" -stats -nomd')
            except Exception:
                if tmp_cog.exists():
                    try: tmp_cog.unlink()
                    except: pass
                log.error("DGM1: gdalwarp produced an invalid file for Landkreis %s; skipping this tile.", kreis5)
                continue

            # publish atomically
            if final_cog.exists():
                try: final_cog.unlink()
                except: pass
            tmp_cog.rename(final_cog)
            log.info("DGM1: created COG %s", final_cog)

            # Import into PostGIS only if extensions & tool are available
            if can_import_to_db:
                append_flag = "-a" if created_any or import_mode == "append" else ""
                pipe = (
                    f'{raster2pgsql_path} -q -s {srid} -I -C -M -t 256x256 "{final_cog}" {fqtn} {append_flag} | '
                    f'{_PSQL}'
                )
                utils.do_cmd(pipe)
                created_any = True

        # Final report
        if can_import_to_db:
            exists = os.popen(f'{_PSQL} -tAc "SELECT to_regclass(\'{fqtn}\')"').read().strip()
            if exists:
                utils.do_cmd(f'{_PSQL} -c "SELECT COUNT(*) AS tiles FROM {fqtn};"')
                # Optional raster table helper index + analyze
                utils.do_cmd(f'{_PSQL} -c "CREATE INDEX IF NOT EXISTS {table}_rast_gix ON {fqtn} USING GIST (ST_ConvexHull(rast));"')
                utils.do_cmd(f'{_PSQL} -c "ANALYZE {fqtn};"')
                log.info("DGM1: imported rasters into %s", fqtn)
            else:
                log.warning("DGM1: no rasters imported; table %s does not exist.", fqtn)
        else:
            log.warning("DGM1: skipped PostGIS import (missing privileges/extensions or raster2pgsql). COG files are available on disk.")




    # ==========================================================
    # 2) Building LoD2 (optional) -> delegate to existing loader
    # ==========================================================
    lod2_cfg = datasets.get("building_lod2", {})
    if lod2_cfg.get("status") == "active":
        if lod2_loader is None:
            log.warning("building_lod2 is active but lod2 loader not importable; skipping.")
        else:
            log.info("LoD2: delegating to existing lod2 loader")
            lod2_loader.load(log_queue)  # uses your current sources.lod2 config

    # ==========================================================
    # 3) Tatsächliche Nutzung (TN) -> GPKG to PostGIS (scoped, robust)
    # ==========================================================
    tn_cfg = datasets.get("tatsaechliche_nutzung", {})
    if tn_cfg.get("status") == "active":

        def _gpkg_layers(gpkg: Path):
            """List layer names in a GPKG (OGR JSON first, then text)."""
            try:
                out = subprocess.check_output(["ogrinfo", "-ro", "-q", "-json", str(gpkg)], text=True)
                data = json.loads(out)
                return [lyr.get("name") for lyr in data.get("layers", []) if "name" in lyr]
            except Exception:
                out = subprocess.check_output(["ogrinfo", "-ro", "-q", str(gpkg)], text=True, stderr=subprocess.STDOUT)
                names = []
                for line in out.splitlines():
                    line = line.strip()
                    if ":" in line and "(" in line:
                        name_part = line.split(":", 1)[1].strip()
                        name = name_part.split("(")[0].strip()
                        if name:
                            names.append(name)
                return names

        def _gpkg_layer_extent_epsg(gpkg: Path, layer: str):
            """Return (epsg:int|None, (minx,miny,maxx,maxy)) parsed from 'ogrinfo -so'."""
            out = subprocess.check_output(["ogrinfo", "-so", "-ro", "-q", str(gpkg), layer], text=True)
            epsg = None
            m = re.search(r"EPSG:(\d+)", out)
            if m:
                epsg = int(m.group(1))
            bb = None
            m2 = re.search(r"Extent:\s*\(([-\d\.]+),\s*([-\d\.]+)\)\s*-\s*\(([-\d\.]+),\s*([-\d\.]+)\)", out)
            if m2:
                bb = (float(m2.group(1)), float(m2.group(2)), float(m2.group(3)), float(m2.group(4)))
            return epsg, bb

        def _dist(a, b):
            return ((a[0]-b[0])**2 + (a[1]-b[1])**2) ** 0.5

        url = tn_cfg["url"]
        table = tn_cfg.get("table_name", "tatsaechliche_nutzung")
        schema = tn_cfg.get("schema", "opendata")
        import_mode = tn_cfg.get("import-mode", "delete")
        overwrite = (import_mode != "append")

        base_path = config.get_path(["loader", "path", "opendata"])
        tn_dir = Path(base_path) / "tatsaechliche_nutzung"
        tn_dir.mkdir(parents=True, exist_ok=True)
        gpkg_path = tn_dir / "Nutzung_kreis.gpkg"

        # Skip download if already present
        if gpkg_path.exists() and gpkg_path.stat().st_size > 1_000_000_000:
            log.info("TN: local GPKG present (%.1f GB). Skipping download.", gpkg_path.stat().st_size/1e9)
        else:
            utils.do_cmd(
                f'aria2c -c -x4 -s4 --summary-interval=60 --console-log-level=warn '
                f'-d "{tn_dir}" -o "Nutzung_kreis.gpkg" "{url}"'
            )

        # Ensure schema exists + get EPSG/PG URL
        params = utils.get_db_parameters("postgres")
        pgurl = f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:{params["exposed_port"]}/{params["db"]}'
        epsg = params["epsg"]
        utils.do_cmd(f'psql "{pgurl}" -c "CREATE SCHEMA IF NOT EXISTS {schema};"')
        fqtn = f"{schema}.{table}"

        # Figure out all layers
        layers = _gpkg_layers(gpkg_path)
        if not layers:
            raise RuntimeError(f"TN: no layers found in {gpkg_path}")
        log.info("TN: found %d layers.", len(layers))

        # ----- order layers by proximity to scope centroid (EPSG from DB) -----
        scope_centroid = None
        try:
            gdf_scope = utils.get_envelop()
            if gdf_scope is not None and not gdf_scope.empty:
                g_scope = gdf_scope.to_crs(epsg=epsg).unary_union
                scope_centroid = (g_scope.centroid.x, g_scope.centroid.y)
        except Exception as e:
            log.warning("TN: could not compute scope centroid: %s", e)

        near, fallback = [], []
        for lyr in layers:
            lyr_epsg, bb = _gpkg_layer_extent_epsg(gpkg_path, lyr)
            if scope_centroid and bb and (lyr_epsg == epsg or lyr_epsg in (None, 0)):
                cx = (bb[0] + bb[2]) / 2.0
                cy = (bb[1] + bb[3]) / 2.0
                near.append((lyr, _dist(scope_centroid, (cx, cy))))
            else:
                fallback.append(lyr)

        near.sort(key=lambda x: x[1])
        ordered_layers = [lyr for (lyr, _) in near] + [l for l in fallback if l not in [lyr for (lyr, _) in near]]
        log.info("TN: trying %d layers (nearest first). First 10: %s%s",
                 len(ordered_layers),
                 ", ".join(ordered_layers[:10]),
                 " ..." if len(ordered_layers) > 10 else "")

        # ----- import: overwrite once, then append; stop at first rows>0 -----
        first = True
        for lyr in ordered_layers:
            try:
                utils.import_layers(
                    input_file=str(gpkg_path),
                    layers=[lyr],
                    schema=schema,
                    prefix="",
                    layer_names=[table],
                    scope=True,                      # clip to scope bbox
                    overwrite=(overwrite and first)  # first layer overwrite; then append
                )
                first = False

                # FAST existence check
                has_rows = os.popen(
                    f'psql "{pgurl}" -tAc "SELECT EXISTS (SELECT 1 FROM {fqtn} LIMIT 1)"'
                ).read().strip().lower()
                if has_rows in ("t", "true"):
                    rows = os.popen(f'psql "{pgurl}" -tAc "SELECT COUNT(*) FROM {fqtn}"').read().strip()
                    log.info("TN: imported %s rows from layer '%s'; stopping further layer attempts.", rows or "some", lyr)
                    break

            except Exception as e:
                log.warning("TN: layer '%s' import error: %s (continuing).", lyr, e)

        # Final check and index
        has_rows_final = os.popen(
            f'psql "{pgurl}" -tAc "SELECT EXISTS (SELECT 1 FROM {fqtn} LIMIT 1)"'
        ).read().strip().lower()
        if has_rows_final not in ("t", "true"):
            preview = ", ".join(ordered_layers[:10]) + (" ..." if len(ordered_layers) > 10 else "")
            log.error("TN: zero rows imported into %s after trying layers (nearest first). Tried: %s", fqtn, preview)
            raise RuntimeError(f"TN import produced empty table {fqtn}")

        utils.do_cmd(f'psql "{pgurl}" -c "CREATE INDEX IF NOT EXISTS {table}_geom_gix ON {fqtn} USING GIST(geom);"')
        log.info("TN: index ensured on %s.geom", fqtn)


    log.info("Opendata Bavaria: done.")

import os
import sys
from typing import Dict, List, Optional

from infdb import InfDB

from . import utils
from shapely import wkt as shapely_wkt
from shapely.geometry import box
import requests

def _iter_index_names(data):
    """
    Yield filenames from different possible index.json shapes:
      - ["LoD2_32_311_5670_1_NW.gml", ...]
      - [{"name": "..."} , ...]
      - {"files": [...]} or {"entries": [...]} or {"children": [...]}
    """
    if data is None:
        return

    # unwrap common container dicts
    if isinstance(data, dict):
        for key in ("files", "entries", "children", "items"):
            if key in data:
                data = data[key]
                break
        else:
            # fallback: if dict values look like list entries
            data = list(data.values())

    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                yield item.split("/")[-1]
            elif isinstance(item, dict):
                name = item.get("name") or item.get("path") or item.get("file") or item.get("href")
                if name:
                    yield str(name).split("/")[-1]
            # else ignore
        return

    # single string
    if isinstance(data, str):
        yield data.split("/")[-1]

def _iter_tiles_for_geom(geom, tile_size_m: int):
    minx, miny, maxx, maxy = geom.bounds
    start_x = int(minx // tile_size_m) * tile_size_m
    start_y = int(miny // tile_size_m) * tile_size_m
    end_x = int(maxx // tile_size_m) * tile_size_m
    end_y = int(maxy // tile_size_m) * tile_size_m

    for x in range(start_x, end_x + tile_size_m, tile_size_m):
        for y in range(start_y, end_y + tile_size_m, tile_size_m):
            cell = box(x, y, x + tile_size_m, y + tile_size_m)
            if geom.intersects(cell):
                yield (x // tile_size_m, y // tile_size_m)  # (e_km, n_km)


def load(infdb: InfDB) -> bool:
    """Download CityGML (per AGS scope), import via citydb CLI, then run post-import SQL.

    Behavior preserved:
    - Returns True when inactive (matching original early-exit).
    - Uses aria2c for downloads and `citydb import citygml` for loading.
    - Builds URL by replacing `#scope` token with each AGS value.
    - Executes a post-import SQL file with format params.
    """
    log = infdb.get_worker_logger()
    try:
        if not utils.if_active("lod2-nrw", infdb):
            return True

        base_path = infdb.get_config_path([infdb.get_toolname(), "sources", "lod2-nrw", "path", "lod2"], type="loader")
        os.makedirs(base_path, exist_ok=True)

        gml_path = infdb.get_config_path([infdb.get_toolname(), "sources", "lod2-nrw", "path", "gml"], type="loader")
        os.makedirs(gml_path, exist_ok=True)

        base_url = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "base_url"]).rstrip("/") + "/"
        index_url = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "index_url"])
        tile_size_m = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "tile_size_m"]) or 1000
        template = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "filename_template"])

        # 1) Get scope geometry in UTM32 using your existing utils
        clip_wkt, _, _ = utils.get_clip_geometry(target_crs=25832, infdb=infdb)
        if not clip_wkt:
            log.warning("No scope geometry resolved; skipping NRW LoD2.")
            return True
        scope_geom = shapely_wkt.loads(clip_wkt)

        # 2) Optional: load available filenames from index.json to avoid 404s
        available = None
        if index_url:
            resp = requests.get(index_url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            available = set(_iter_index_names(data))
            log.info("Loaded %d filenames from index.json", len(available))

        # 3) Compute tile filenames for intersecting cells
        wanted = []
        for e_km, n_km in _iter_tiles_for_geom(scope_geom, tile_size_m=tile_size_m):
            fname = template.format(e_km=e_km, n_km=n_km)
            if (available is None) or (fname in available):
                wanted.append(base_url + fname)

        wanted = sorted(set(wanted))
        log.info("NRW LoD2: %d tiles to download", len(wanted))

        # 4) Download tiles into gml_path
        if wanted:
            # Minimal option: loop single downloads (no utils change)
            # for u in wanted:
            #     utils.download_aria2c(u, output_dir=gml_path, quiet=True)

            # Better option: one aria2 run (needs download_aria2c_many helper)
            utils.download_aria2c_many(wanted, output_dir=gml_path)

        # 5) Import (unchanged)
        params = infdb.get_db_parameters_dict()
        import_mode = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "import-mode"])
        cmd_parts = [
            "citydb", "import", "citygml",
            "-H", params["host"],
            "-d", params["db"],
            "-u", params["user"],
            "-p", params["password"],
            "-P", str(params["exposed_port"]),
            f"--import-mode={import_mode}",
            str(gml_path),
        ]
        utils.do_cmd(cmd_parts)

        # 6) Post SQL scope list: use the resolved AGS list (your new function)
        ags_list = utils.fetch_scope_ags_from_db(infdb)
        formatted_scope = ",".join(f"'{s}'" for s in ags_list)
        with infdb.connect() as db:
            db.execute_sql_file("sql/buildings_lod2.sql", {"output_schema": "opendata", "gemeindeschluessel": formatted_scope})

        log.info("LOD2-NRW data loaded successfully")
        sys.exit(0)
    except Exception:
        log.exception("An error occurred while processing LOD2_NRW data")
        sys.exit(1)

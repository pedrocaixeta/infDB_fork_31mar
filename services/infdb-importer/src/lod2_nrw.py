import os
import sys

from infdb import InfDB
from shapely import wkt as shapely_wkt
from shapely.geometry import box

from . import utils


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

        gml_path = infdb.get_config_path(
            [infdb.get_toolname(), "sources", "lod2-nrw", "path", "gml"], type="loader"
        )
        os.makedirs(gml_path, exist_ok=True)

        base_url = (
            infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "base_url"]).rstrip("/") + "/"
        )
        tile_size_m = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "tile_size_m"]) or 1000
        template = infdb.get_config_value([infdb.get_toolname(), "sources", "lod2-nrw", "filename_template"])

        # 1) Get scope geometry in UTM32
        clip_wkt, _, _ = utils.get_clip_geometry(target_crs=25832, infdb=infdb, state_prefix="05")
        if not clip_wkt:
            log.warning("No scope geometry resolved; skipping NRW LoD2.")
            return True
        scope_geom = shapely_wkt.loads(clip_wkt)

        # 2) Build URLs for intersecting tiles
        urls = []
        for e_km, n_km in _iter_tiles_for_geom(scope_geom, tile_size_m=tile_size_m):
            fname = template.format(e_km=e_km, n_km=n_km)
            urls.append(base_url + fname)

        urls = sorted(set(urls))
        log.info("NRW LoD2: %d tiles to download", len(urls))

        # 3) Download
        if urls:
            utils.download_aria2c_many(urls, output_dir=gml_path)

        # 4) Import
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

       
        log.info("LOD2-NRW data loaded successfully")
        sys.exit(0)
    except Exception:
        log.exception("An error occurred while processing LOD2_NRW data")
        sys.exit(1)

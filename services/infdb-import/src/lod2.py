import os
import sys

from infdb import InfDB
from shapely import wkt as shapely_wkt
from shapely.geometry import box

from . import utils


def _iter_tile_origins_for_geom(geom, tile_size_m: int):
    """Yield lower-left tile origin coordinates (in meters) for all tiles
    intersecting the given geometry.
    """
    minx, miny, maxx, maxy = geom.bounds
    start_x = int(minx // tile_size_m) * tile_size_m
    start_y = int(miny // tile_size_m) * tile_size_m
    end_x = int(maxx // tile_size_m) * tile_size_m
    end_y = int(maxy // tile_size_m) * tile_size_m

    for x in range(start_x, end_x + tile_size_m, tile_size_m):
        for y in range(start_y, end_y + tile_size_m, tile_size_m):
            cell = box(x, y, x + tile_size_m, y + tile_size_m)
            if geom.intersects(cell):
                yield (x, y)


def _build_urls_for_region(region_name: str, region_cfg: dict, infdb: InfDB, log) -> list[str]:
    """Build all LoD2 tile URLs for one region based on the scoped geometry."""
    if region_cfg.get("status") != "active":
        log.info("LoD2 %s: inactive, skipping.", region_name)
        return []

    state_prefix = region_cfg.get("state_prefix")
    base_url = str(region_cfg.get("base_url", "")).rstrip("/") + "/"
    tile_size_m = int(region_cfg.get("tile_size_m") or 0)
    template = region_cfg.get("filename_template")

    if not state_prefix or not base_url or not tile_size_m or not template:
        log.warning("LoD2 %s: incomplete configuration, skipping.", region_name)
        return []

    clip_wkt, _, _ = utils.get_clip_geometry(target_crs=25832, infdb=infdb, state_prefix=state_prefix)
    if not clip_wkt:
        log.info("LoD2 %s: no scope geometry resolved for state prefix %s, skipping.", region_name, state_prefix)
        return []

    scope_geom = shapely_wkt.loads(clip_wkt)

    urls = []
    for x, y in _iter_tile_origins_for_geom(scope_geom, tile_size_m=tile_size_m):
        # x/y are tile origin coordinates in meters
        fname = template.format(
            e_km=x // 1000,
            n_km=y // 1000,
        )
        urls.append(base_url + fname)

    urls = sorted(set(urls))
    log.info("LoD2 %s: %d intersecting tiles resolved.", region_name, len(urls))
    return urls


def load(infdb: InfDB) -> bool:
    """Download LoD2 CityGML tiles for all active configured regions, import them via citydb,
    then create the flat LoD2 building table.

    Behavior:
    - Uses one shared CityGML folder for all configured regions.
    - Resolves scope geometry per region/state in EPSG:25832.
    - Computes intersecting tiles using regular grid logic.
    - Deduplicates URLs globally, so the same file is not downloaded twice.
    """
    log = infdb.get_worker_logger()

    try:
        if not utils.if_active("lod2", infdb):
            return True

        source_cfg = [infdb.get_toolname(), "sources", "lod2"]

        gml_path = infdb.get_config_path(source_cfg + ["path", "gml"], type="loader")
        os.makedirs(gml_path, exist_ok=True)

        nrw_cfg = infdb.get_config_value(source_cfg + ["nrw"]) or {}
        bavaria_cfg = infdb.get_config_value(source_cfg + ["bavaria"]) or {}

        urls = []
        urls.extend(_build_urls_for_region("NRW", nrw_cfg, infdb, log))
        urls.extend(_build_urls_for_region("Bavaria", bavaria_cfg, infdb, log))

        urls = sorted(set(urls))
        log.info("LoD2: %d unique tiles to download across all active regions.", len(urls))

        if not urls:
            log.warning("LoD2: no tiles resolved for any active region; skipping import.")
            return True

        # Download all unique tiles into one shared folder
        utils.download_aria2c_many(infdb, urls, output_dir=gml_path)

        # Import all downloaded CityGML files from the shared folder
        params = infdb.get_db_parameters_dict()
        import_mode = infdb.get_config_value(source_cfg + ["import-mode"]) or "skip"

        cmd_parts = [
            "citydb",
            "import",
            "citygml",
            "-H",
            params["host"],
            "-d",
            params["db"],
            "-u",
            params["user"],
            "-p",
            params["password"],
            "-P",
            str(params["exposed_port"]),
            f"--import-mode={import_mode}",
            # "--log-level=warn",
            str(gml_path),
        ]
        utils.do_cmd(infdb, cmd_parts)

        # Create flat building table
        object_id_prefix = infdb.get_config_value(source_cfg + ["object_id_prefix"]) or "DE"
        utils.create_building_lod2_table(object_id_prefix=object_id_prefix, infdb=infdb)

        log.info("LoD2 data loaded successfully")
        sys.exit(0)

    except Exception:
        log.exception("An error occurred while processing LoD2 data")
        sys.exit(1)
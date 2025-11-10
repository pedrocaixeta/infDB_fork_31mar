import os
from typing import Dict, List, Optional

from infdb import InfDB
from . import utils


# ============================== Constants ==============================

TOOL_NAME: str = "loader"
ARIA2C_BASE_CMD: str = (
    "aria2c --continue=true --allow-overwrite=false --auto-file-renaming=false"
)


def load(infdb: InfDB)  -> bool:
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
        cmd = f"{ARIA2C_BASE_CMD} {url} -d {gml_path}"
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

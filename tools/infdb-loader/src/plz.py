import os
from logging.handlers import QueueHandler
from typing import List, Sequence

from infdb import InfDB
from . import utils


# ============================== Constants ==============================

TOOL_NAME: str = "loader"



def load(infdb: InfDB) -> bool:
    """Download PLZ dataset (if active), ensure schema, and import configured layers.

    Behavior preserved:
    - Early exit (True) when feature flag `plz` is inactive.
    - Skips download when the target file already exists.
    - Ensures schema via InfdbClient and imports layers with configured prefix.
    """
    file_path: str | None = None  # for safe logging if errors occur before assignment
    try:
        log = infdb.get_worker_logger()
        if not utils.if_active("plz"):
            return True

        base_path = infdb.get_config_path([TOOL_NAME, "sources", "plz", "path", "base"], type="loader")
        log.debug("base_path=%s", base_path)
        os.makedirs(base_path, exist_ok=True)

        url: str = infdb.get_config_value([TOOL_NAME, "sources", "plz", "url"])
        log.debug("url=%s", url)
        filename, *_ = utils.get_file_from_url(url)

        file_path = os.path.join(base_path, filename)
        log.debug("Downloading PLZ data from %s to %s", url, file_path)

        if os.path.exists(file_path):
            log.info("File %s already exists.", file_path)
        else:
            log.info("File %s will be downloaded from %s", file_path, url)
            utils.download_files(url, base_path)

        schema: str = infdb.get_config_value([TOOL_NAME, "sources", "plz", "schema"])

        # Ensure schema exists using InfdbClient
        with infdb.connect() as db:
            db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        prefix: str = infdb.get_config_value([TOOL_NAME, "sources", "plz", "prefix"])
        layers: Sequence[str] = infdb.get_config_value([TOOL_NAME, "sources", "plz", "layer"])

        log.info("Loading PLZ data from %s to %s", url, file_path)
        utils.import_layers(file_path, layers, schema, prefix=prefix)

        log.info("PLZ data loaded successfully")
        return True

    except Exception as err:
        log.exception(
            "An error occurred while processing plz file: %s %s",
            file_path if file_path else "<unknown>",
            str(err),
        )
        return False

import logging
from typing import Final

from infdb import InfDB
from . import utils


# ============================== Constants ==============================

LOGGER_NAME: Final[str] = __name__
TOOL_NAME: Final[str] = "loader"
CONFIG_DIR: Final[str] = "configs"
PACKAGE_NAME: Final[str] = "opendata"
PACKAGE_EXT: Final[str] = ".zip"

def load(infdb: InfDB) -> None:
    """Download and unpack the opendata package using the loader configuration.

    Behavior preserved:
    - Reads paths and URL from the 'loader' tool config.
    - Downloads the archive to the base path, then unzips into the processed path.
    """
    log = infdb.get_worker_logger()
    # Use loader config via infdb_package

    # Download opendata package
    archive_path = infdb.get_config_path([TOOL_NAME, "sources", "package", "path", "base"], type="loader")
    url = infdb.get_config_value([TOOL_NAME, "sources", "package", "url"])
    log.info("Download opendata package from %s to %s", url, archive_path)
    utils.download_files(url, archive_path)

    # Unzip opendata package
    file_path = utils.get_file(archive_path, filename=PACKAGE_NAME, ending=PACKAGE_EXT)
    opendata_path = infdb.get_config_path([TOOL_NAME, "sources", "package", "path", "processed"], type="loader")
    log.info("Unzip opendata package from %s to %s", file_path, opendata_path)
    utils.unzip(file_path, opendata_path)

    log.info("package done!")

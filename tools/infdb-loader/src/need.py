import os
import logging
from . import utils, config, logger

log = logging.getLogger(__name__)


def load(log_queue):
    try:
        logger.setup_worker_logger(log_queue)

        if not utils.if_active("need"):
            return
        
        # Dump input schema from source database
        source_host = config.get_value(["loader", "sources", "need", "host"])
        source_port = config.get_value(["loader", "sources", "need", "port"])
        source_db = config.get_value(["loader", "sources", "need", "database"])
        source_user = config.get_value(["loader", "sources", "need", "user"])
        source_password = config.get_value(["loader", "sources", "need", "password"])
        schema_input = config.get_value(["loader", "sources", "need", "schema_input"])

        # Create dump file path
        path_dump = config.get_path(["loader", "sources", "need", "path_dump"])
        file_dump = os.path.join(path_dump, "need.dump")
        os.makedirs(os.path.dirname(file_dump), exist_ok=True)

        # Dump schema from source database
        if os.path.exists(file_dump):
            log.info(f"Dump file {file_dump} already exists and will be skipped")
        else:
            log.info(f"Dumping need data from source database {source_db} schema {schema_input} to {file_dump}...")
            command = f"PGPASSWORD={source_password} pg_dump -h {source_host} -p {source_port} -U {source_user} -d {source_db} -n {schema_input} -F c -f {file_dump}"
            utils.do_cmd(command)

        # Restore dump into target database
        log.info(f"Restoring need data from dump file {file_dump} into target database...")
        params = utils.get_db_parameters("postgres")
        command = f"PGPASSWORD={params['password']} pg_restore -h {params['host']} -p {params['exposed_port']} -U {params['user']} -d {params['db']} -j 4 --clean --if-exists --no-owner --role={params['user']} {file_dump}"
        utils.do_cmd(command)
        
        log.info(f"Need data loaded successfully")
    
    except Exception as err:
        log.exception(f"An error occurred while processing need data: {str(err)}")
        return False

    return True
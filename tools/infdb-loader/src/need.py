import os
import logging
from . import utils, config, logger

log = logging.getLogger(__name__)


def load(log_queue):
    try:
        logger.setup_worker_logger(log_queue)

        if not utils.if_active("need"):
            return
        
        # Create schema if it doesn't exist
        schema_output = config.get_value(["loader", "sources", "need", "schema_output"])
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema_output};"
        utils.sql_query(sql)

        source_host = config.get_value(["loader", "sources", "need", "host"])
        source_port = config.get_value(["loader", "sources", "need", "port"])
        source_db = config.get_value(["loader", "sources", "need", "database"])
        source_user = config.get_value(["loader", "sources", "need", "user"])
        source_password = config.get_value(["loader", "sources", "need", "password"])

        schema_output = config.get_value(["loader", "sources", "need", "schema_output"])
        params = utils.get_db_parameters("citydb")

        utils.do_cmd(f"export PGPASSWORD={source_password}")
                     
        for table_name in ["buildings_lod2", "basemap_verkehrslinien"]:  # Replace with actual table names
            # command = f"pg_dump -h {source_host} -U {source_user} -d {source_db} -n {schema} -p {source_password} -P {source_port} | psql -h {params['host']} -U {params['user']} -d {params['database']} -p {params['exposed_port']} -v schema={schema}"
            command = f"pg_dump -h {source_host} -U {source_user} -d {source_db} -t {table_name} -P {source_port} | psql -h {params['host']} -U {params['user']} -d {params['db']} -p {params['exposed_port']} -v schema={schema_output}"
            log.debug(f"Executing command: {command}")
            utils.do_cmd(command)

        log.info(f"Need data loaded successfully")
    
    except Exception as err:
        log.exception(f"An error occurred while processing need data: {str(err)}")
        return False

    return True


load(None)
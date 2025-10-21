import os
from infdb import InfDB
import subprocess

def main():

    # Load InfDB handler
    infdbhandler = InfDB(tool_name="infdb-init")

    # Database connection
    infdbclient = infdbhandler.connect()

    # Logger setup
    infdblog = infdbhandler.get_log()

    # Start message
    infdblog.info(f"Starting {infdbhandler.get_toolname()} tool")

    # Check if schema 'citydb' exists
    cursor = infdbclient.get_db_cursor()
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'citydb')"
    )
    schema_exists = cursor.fetchone()[0]
    cursor.close()
    
    if schema_exists:
        infdblog.info("Schema 'citydb' exists, skipping initialization")
    
    else:
        infdblog.info("Schema 'citydb' does not exist, proceeding with initialization")

        # Set up environment variables for 3DCityDB
        param = {k: str(v) for k, v in infdbhandler.infdbconfig.get_db_parameters("postgres").items()}  # Ensure all values are strings
        env = {
            'PGBIN': '/var/lib/postgresql/17/bin',
            'PGHOST': param['host'],
            'PGPORT': param['exposed_port'],
            'CITYDB': param['db'],
            'PGUSER': param['user'],
            'PGPASSWORD': param['password'],
            'SRID': param['epsg'],
            'HEIGHT_EPSG': "0",
            'CHANGELOG': "no"
        }
        infdblog.debug(f"Environment variables set: {env}")
        
        # Install 3DcityDB extension
        infdblog.info("Installing 3DcityDB extension")
        subprocess.run([
            "bash",
            "/tmp/3dcitydb/postgresql/shell-scripts/unix/create-db.sh"
        ], env=env, check=True)
    
    # End message
    infdblog.info(f"Successfully finished {infdbhandler.get_toolname()} tool")


if __name__ == "__main__":
    main()

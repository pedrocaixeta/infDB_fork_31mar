import psycopg2
from . import InfdbConfig
import os
import time
import sqlalchemy


class InfdbClient:
    """Responsible for connecting to InfDB database."""

    def __init__(self, infdbconfig: InfdbConfig, infdblog, db_name="citydb"):
        self.log = infdblog

        infdb_params = infdbconfig.get_db_parameters(db_name)
        try:
            self.conn = psycopg2.connect(
                host=infdb_params["host"],
                port=infdb_params["exposed_port"],
                database=infdb_params["db"],
                user=infdb_params["user"],
                password=infdb_params["password"],
                # options=f"-c search_path={INFDB_SOURCE_SCHEMA},public",
            )
            self.cur = self.conn.cursor()
            self.db_params = self.conn.get_dsn_parameters()
            # self.db_path = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}"  #  self.conn.get_dsn_parameters() Why not?
        except psycopg2.OperationalError as err:
            self.log.warning(f"Connecting to {self.db_params} was not successful."
                                f"Make sure, that you have established the SSH connection with correct port mapping.")
            raise err

        self.log.debug(f"InfDB DatabaseClient is constructed and connected to {self.db_params}.")

    
    def __del__(self):
        self.cur.close()
        self.conn.close()

    
    def __str__(self):
        return f"InfdbClient connected to {self.db_params}"


    def execute_query(self, query: str, params: tuple = None) -> list[tuple]:
        """
        Execute a SQL query and return the results.
        """
        self.cur.execute(query, params)
        if self.cur.description is None:
            return []
        rows = self.cur.fetchall()
        return rows
    

    def execute_sql_files(self, sql_dir, file_list=None, format_params=None):
        """ Execute multiple SQL files from a directory.
        If file_list is provided, only those files will be executed.
        """
        # Execute all sql scripts if no filelist is defined
        if file_list is None:
            file_list = [f for f in os.listdir(sql_dir) if f.endswith(".sql")]
            file_list.sort()  # Ensure consistent order

        total_files = len(file_list)
        self.log.info(f"Starting execution of {total_files} SQL scripts")

        for file_path in file_list:
            self.log.info(f"Executing SQL file: {file_path}")
            start_time = time.time()
            try:
                full_path = os.path.join(sql_dir, file_path)
                with open(full_path, "r", encoding="utf-8") as file:
                    sql_content = file.read()

                # todo: Check for empty query and not only empty lines
                if sql_content.strip() == "":
                    self.log.warning(f"SQL file {file_path} is empty. Skipping.")
                    continue
                
                # # Apply schema parameter substitution
                if format_params:
                    sql_content = sql_content.format(**format_params)

                self.cur.execute(sql_content)
                self.conn.commit()
                elapsed_time = time.time() - start_time
                self.log.info(f"Successfully executed {file_path} in {elapsed_time:.2f} seconds")
            except Exception as e:
                self.conn.rollback()
                self.log.error(f"Failed to execute {file_path}: {str(e)}")
                raise
   
    def execute_sql_file(self, file_path, format_params=None):
        """ Execute a single SQL file. """
        self.execute_sql_files(os.path.dirname(file_path), [os.path.basename(file_path)], format_params=format_params)

    def get_db_engine(self):
        """ Create and return a SQLAlchemy engine for the current database connection. """
        host = self.db_params["host"]
        user = self.db_params["user"]
        password = self.db_params["password"]
        db = self.db_params["db"]
        port = self.db_params["exposed_port"]

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        engine = sqlalchemy.create_engine(db_url)

        return engine

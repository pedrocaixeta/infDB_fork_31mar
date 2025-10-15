"""
Demo module for database operations with InfDB.

This module provides example functions for connecting to and querying
the InfDB database using both the InfDB client and direct SQLAlchemy connections.
"""

import os
import geopandas as gpd
from sqlalchemy import create_engine


def sql_demo(infdb):
    """
    Demonstrate SQL script execution using InfDB.
    
    Drops and recreates the output schema, then executes all SQL scripts
    from the sql/ directory in alphabetical order. Format parameters are
    passed to allow dynamic schema names in SQL templates.
    
    Args:
        infdb: InfDB client instance with database connection.
        
    Note:
        - SQL files can use placeholders like {output_schema}, {input_schema}
        - Scripts are executed in alphabetical order (use prefixes: 01_, 02_)
    """
    # Schema configuration
    format_params = {
        'input_schema': infdb.get_config_value(["data", "input_schema"], insert_toolname=True),
        'output_schema': infdb.get_config_value(["data", "output_schema"], insert_toolname=True),
    }

    # Drop output schema if exists for development purposes
    infdb.con.execute_query("DROP SCHEMA IF EXISTS {output_schema} CASCADE".format(**format_params))

    # Execute sql scripts
    infdb.get_log().info("Running SQL scripts ...")
    SQL_DIR = os.path.join("sql")   # add subfolders here if needed
    infdb.connect().execute_sql_files(SQL_DIR, format_params=format_params)



def database_demo(infdb):
    """
    Demonstrate database querying using InfDB client.

    Retrieves building heat demand data from the kwp schema using
    the InfDB database engine and loads it into a GeoDataFrame.
    
    Args:
        infdb: InfDB client instance with database connection.
        
    Returns:
        GeoDataFrame: Buildings with heat demand data and geometry.
    """
    engine = infdb.get_db_engine()
    sql = "SELECT * FROM kwp.buildings_heat_demand"
    gdf_buildings = gpd.read_postgis(sql, engine,  geom_col='geom')
    gdf_buildings.head()
    
    return gdf_buildings


def database_demo_sqlalchemy():
    """
    Demonstrate direct SQLAlchemy database connection.
    
    Creates a direct database connection using SQLAlchemy engine
    and queries building heat demand data from the kwp schema.
    Uses hardcoded connection parameters suitable for Docker environments.
    
    Returns:
        GeoDataFrame: Buildings with heat demand data and geometry.
    """
    # Database connection parameters
    user = "infdb_user"
    password = "infdb"
    host = "host.docker.internal"
    port = "54328"
    db = "infdb"
    db_connection_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    engine = create_engine(db_connection_url)
    sql = "SELECT * FROM kwp.buildings_heat_demand"
    gdf_buildings = gpd.read_postgis(sql, engine, geom_col='geom')
    gdf_buildings.head()
    
    return gdf_buildings

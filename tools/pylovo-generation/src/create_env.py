def create_pylovo_env_file(infdb):
    """
    Creates a .env file with db settings for pylovo from the config-infdb.
    """
    db_name = infdb.get_value(["services", "postgres", "db"])
    db_user = infdb.get_value(["services", "postgres", "user"])
    db_password = infdb.get_value(["services", "postgres", "password"])
    db_port = infdb.get_value(["services", "postgres", "exposed_port"])
    db_host = "postgres"  # internal docker host name

    env_content = f"""# PYLOVO Database
    USE_INFDB=True

    DBNAME="{db_name}"
    DBUSER="{db_user}"
    HOST="{db_host}"
    PORT="{db_port}"
    PASSWORD="{db_password}"
    TARGET_SCHEMA="pylovo"

    INFDB_DBNAME="{db_name}"
    INFDB_USER="{db_user}"
    INFDB_HOST="{db_host}"
    INFDB_PORT="{db_port}"
    INFDB_PASSWORD="{db_password}"
    INFDB_SOURCE_SCHEMA="basedata"
    """

    tool_dir = os.path.dirname(__file__)
    env_path = os.path.join(tool_dir / src / pylovo, ".env")
    with open(env_path, "w") as f:
        f.write(env_content)
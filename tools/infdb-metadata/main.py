
"""
Main entry point for the infdb-metadata tool.
Handles InfDB initialization, database connection, logging, and demo execution.
"""

# Import packages
import os
import sys
from infdb import InfDB
from pathlib import Path
import tempfile
from src.infdb_metadata import (
    parse_args,
    load_env,
    get_conn,
    fetch_metadata,
    print_available_schemas,
    prompt_schema_selection,
    filter_schemas,
    wrap_database,
    write_metadata_file,
    write_metadata_yaml,
    generate_rdf,
    SCHEMA_PATH,
    HERE,
)


def main():
    """
    Initializes InfDB handler, sets up logging, connects to the database,
    and runs the demo function. Handles exceptions and logs errors.
    """

    # Initialize InfDB handler
    infdb = InfDB(tool_name="infdb-metadata")

    # Start messagero
    infdb.log.info(f"Starting {infdb.get_toolname()} tool")

    try:
        # ===========================================================
        # Start your added python code in folder "src"
        # ===========================================================
        infdb.log.info("Running python code ...")
        # infdb_metadata.run()

        args = parse_args()
        load_env(log=infdb.log)
        try:
            conn = get_conn(infdb.log)
        except Exception as exc:
            infdb.log.error(
                f"Failed to connect to database: {exc}", file=sys.stderr)
            return 1

        try:
            metadata = fetch_metadata(infdb.log, conn)
        finally:
            conn.close()
        infdb.log.info("✅ Finished fetching metadata.")

        available_schemas = print_available_schemas(infdb.log, metadata)
        if not available_schemas:
            return 0

        chosen = args.schemas
        if not chosen:
            chosen = prompt_schema_selection(infdb.log, available_schemas)

        if chosen:
            infdb.log.info("Filtering to selected schemas...")
        metadata = filter_schemas(metadata, chosen)

        selected_schemas = [s.get("schema_name")
                            for s in metadata.get("schemas", [])]
        infdb.log.info(selected_schemas)
        if chosen and not selected_schemas:
            infdb.log.error(
                f"No matching schemas found for: {', '.join(chosen)}", file=sys.stderr)
            return 1
        if chosen:
            infdb.log.info(f"Including schemas: {', '.join(selected_schemas)}")
        db_label = metadata.get("name") or "database"

        # NOTE: to make sure the output is writable in containerized environments,
        # write to OUTPUT_DIR or /app/mnt/data in a docker by default
        out_base = Path(os.getenv("OUTPUT_DIR", "/app/mnt/data"))
        out_base.mkdir(parents=True, exist_ok=True)

        # NOTE: revert to naming without schema names in suffix
        # suffix = ""
        # if selected_schemas:
        #     suffix = "-" + "-".join(selected_schemas)
        # data_path = HERE / f"{db_label}{suffix}_schema.json"
        # yaml_path = HERE / f"{db_label}{suffix}_schema.yaml"
        data_path = out_base / f"{db_label}_schema.json"
        yaml_path = out_base / f"{db_label}_schema.yaml"
        wrapped = wrap_database(metadata)
        infdb.log.info("Writing JSON and YAML outputs...")
        write_metadata_file(infdb.log, wrapped, data_path)
        write_metadata_yaml(infdb.log, wrapped, yaml_path)

        rdf_input_path = Path(tempfile.mkstemp(
            prefix="rdf_input_", suffix=".json", dir=str(HERE))[1])
        try:
            write_metadata_file(infdb.log, metadata,
                                rdf_input_path, quiet=True)
            # with open (rdf_input_path, "r", encoding="utf-8") as fh:
            #     print(fh.read())

            rdf_output = data_path.with_suffix(".ttl")
            try:
                generate_rdf(infdb.log, SCHEMA_PATH,
                             rdf_input_path, rdf_output)
            except Exception as exc:
                infdb.log.info(
                    f"Skipping RDF generation because LinkML tooling is unavailable: {exc}",
                    file=sys.stderr,
                )
                infdb.log.info(
                    "To generate RDF manually, run "
                    f"`linkml-convert {rdf_input_path} --schema {SCHEMA_PATH} "
                    f"--target-class Database --output {rdf_output} --output-format ttl`.",
                    file=sys.stderr,
                )
        finally:
            try:
                rdf_input_path.unlink()
            except OSError:
                pass
        return 0

    except Exception as e:
        infdb.log.error(f"Something went wrong: {str(e)}")
        raise e


if __name__ == "__main__":
    main()

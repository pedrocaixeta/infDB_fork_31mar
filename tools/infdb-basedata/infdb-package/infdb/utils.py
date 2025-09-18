import os, yaml, subprocess
import logging

log = logging.getLogger(__name__)


def do_cmd(cmd: str):
    """ Execute a shell command and log its output in real-time. """
    log.info(f"Executing command: {cmd}")

    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Zeilenweise Puffern
    )

    # Zeilenweise lesen und direkt loggen
    if process.stdout:
        for line in process.stdout:
            log.info(line.rstrip())

    # Warten bis der Prozess beendet ist
    return_code = process.wait()
    if return_code == 0:
        log.info("Command completed successfully.")
    else:
        log.error(f"Command failed with return code {return_code}")


def write_yaml(self, output_yaml, output_path):
    """ Write a YAML dictionary to a file. """
    output_path = os.path.join(self.get_root_path(), output_path)
    with open(output_path, "w") as f:
        yaml.dump(output_yaml, f, default_flow_style=False, sort_keys=False)


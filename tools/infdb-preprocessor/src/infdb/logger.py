import logging
from logging.handlers import QueueHandler, QueueListener
import sys
from . import InfdbConfig


def setup_main_logger(infdbconfig, log_queue=None):
    """ Set up the main logger with console and file handlers.
    If log_queue is provided, use it for multiprocessing logging.
    """
    formatter = logging.Formatter(
        "%(asctime)s | %(processName)s | %(levelname)s: %(message)s"
    )

    # Logging to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Logging to file
    file_path = infdbconfig.get_value([infdbconfig.tool_name, "logging", "path"])
    # if os.path.exists(file_path):
    #     os.remove(file_path)    # for debugging
    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)

    # Get the root logger and set its level and handlers
    root_logger = logging.getLogger()
    level_string = infdbconfig.get_value([infdbconfig.tool_name, "logging", "level"])
    level = logging._nameToLevel.get(level_string.upper(), logging.INFO)
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    if log_queue:
        listener = QueueListener(log_queue, console_handler, file_handler)
        listener.start()
    else:
        listener = None

    return listener


def setup_worker_logger(infdbconfig,log_queue):
    """ Set up logger for worker processes to use the provided log queue. """
    root_logger = logging.getLogger()
    level_string = infdbconfig.get_value([infdbconfig.tool_name, "logging", "level"])
    level = logging._nameToLevel.get(level_string.upper(), logging.INFO)
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(QueueHandler(log_queue))
    root_logger.propagate = False

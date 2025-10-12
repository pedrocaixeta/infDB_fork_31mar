import logging, multiprocessing
from logging.handlers import QueueHandler, QueueListener
import sys
from . import Infdb
import os

class InfdbLogger:
    """Responsible for setting up logging for tools connected to infDB."""

    def __init__(self, infdb: type[Infdb], cleanup=False):
   
        self.formatter = logging.Formatter(
            "%(asctime)s | %(processName)s | %(levelname)s: %(message)s"
        )

        # Logging to console
        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setFormatter(self.formatter)

        # Logging to file
        self.file_path = infdb.get_config_value(["logging", "path"], insert_toolname=True)
        if cleanup:
            if os.path.exists(self.file_path):
                os.remove(self.file_path)    # for debugging
        self.file_handler = logging.FileHandler(self.file_path)
        self.file_handler.setFormatter(self.formatter)

        # Get the root logger and set its level and handlers
        self.root_logger = logging.getLogger(__name__)
        self.level_string = infdb.get_config_value(["logging", "level"], insert_toolname=True)
        self.level = logging._nameToLevel.get(self.level_string.upper(), logging.INFO)
        self.root_logger.setLevel(self.level)
        self.root_logger.handlers.clear()
        self.root_logger.addHandler(self.console_handler)
        self.root_logger.addHandler(self.file_handler)

        # Set up queue listener for multiprocessing
        self.log_queue = multiprocessing.Queue()
        self.listener = QueueListener(self.log_queue, self.console_handler, self.file_handler)
        self.listener.start()

    
    def __del__(self):
        if self.listener:
            self.listener.stop()
            self.listener = None

    
    def __str__(self):
        return f"InfdbLogger with listener {self.listener}"       


    def setup_worker_logger(self):
        """ Set up logger for worker processes to use the provided log queue. """
        logger = logging.getLogger(__name__)
        logger.setLevel(self.level)
        logger.handlers.clear()
        logger.addHandler(QueueHandler(self.log_queue))
        logger.propagate = False
        return logger

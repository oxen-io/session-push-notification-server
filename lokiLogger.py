import logging
from logging.handlers import TimedRotatingFileHandler


class LokiLogger:
    def __init__(self):
        self.logger = logging.getLogger()
        self.log_config()

    def log_config(self):
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        file_handler = TimedRotatingFileHandler('apns.log', 'midnight', 1, 0)
        file_handler.suffix = '%Y%m%d'
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

'''
Module-level logging component.
'''
import logging

LOGGING_FORMAT = '[%(asctime)s][%(name)s][%(levelname)s] %(message)s'
LOGGING_DATEFMT = '%m/%d|%H:%M:%S'
SHORT_LOGGING_FORMAT = '[\x1b[33;20m%(name)s\x1b[0m][%(levelname)s][%(URI)s] %(message)s'


logger = logging.getLogger('dht')
logger.setLevel(logging.DEBUG)

# Formatters:
formatter = logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT)
short_formatter = logging.Formatter(SHORT_LOGGING_FORMAT)

# Handlers:
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(short_formatter)
logger.addHandler(stdout_handler)

file_handler = logging.FileHandler('logs/dht.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
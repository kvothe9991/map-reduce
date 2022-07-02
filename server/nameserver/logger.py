'''
Module-level logging component.
'''
import logging

LOGGING_FORMAT = '[%(asctime)s][%(name)s][%(levelname)s][%(URI)s] %(message)s'
LOGGING_DATEFMT = '%m/%d|%H:%M:%S'


logger = logging.getLogger(__name__)

# Formatters:
formatter = logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT)

# Handlers:
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

file_handler = logging.FileHandler('nameserver.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
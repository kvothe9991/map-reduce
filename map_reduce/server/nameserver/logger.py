'''
Module-level logging component.
'''
import logging

LOGGING_FORMAT = '[\033[1;32m%(name)s\x1b[0m ][%(levelname)s][%(URI)s] %(message)s'
LOGGING_DATEFMT = '%m/%d|%H:%M:%S'


logger = logging.getLogger('ns')
logger.setLevel(logging.DEBUG)

# Formatters:
formatter = logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT)

# Handlers:
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

file_handler = logging.FileHandler('logs/nameserver.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
'''
Module-level logging component.
'''
import logging

LOGGING_FORMAT = '[%(asctime)s][%(name)s][%(levelname)s] %(message)s'
LOGGING_DATEFMT = '%m/%d|%H:%M:%S'
BLUE_SHORT_LOGGING_FORMAT = '[\x1b[1;36m%(name)s\x1b[0m][%(levelname)s][%(URI)s] %(message)s'
RED_SHORT_LOGGING_FORMAT = '[\x1b[1;31m%(name)s\x1b[0m][%(levelname)s][%(URI)s] %(message)s'

# Loggers:
follower_logger = logging.getLogger('follow')
follower_logger.setLevel(logging.DEBUG)
master_logger = logging.getLogger('master')
master_logger.setLevel(logging.DEBUG)

# Formatters:
formatter = logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT)
blue_short_formatter = logging.Formatter(BLUE_SHORT_LOGGING_FORMAT)
red_short_formatter = logging.Formatter(RED_SHORT_LOGGING_FORMAT)

# Handlers:
follower_stdout_handler = logging.StreamHandler()
follower_stdout_handler.setLevel(logging.INFO)
follower_stdout_handler.setFormatter(blue_short_formatter)
follower_logger.addHandler(follower_stdout_handler)

master_stdout_handler = logging.StreamHandler()
master_stdout_handler.setLevel(logging.INFO)
master_stdout_handler.setFormatter(red_short_formatter)
master_logger.addHandler(master_stdout_handler)

file_handler = logging.FileHandler('dht.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
follower_logger.addHandler(file_handler)
'''
Multi-module logger for all server components.
'''
import logging
from map_reduce.server.configs import LOGGING, ConfigError

FORMAT = '[%(IP)s][{color}%(name)s{reset}][%(levelname)s]: %(message)s'
RESET = '\x1b[0m'


def get_logger(name: str, adapter: dict = {}, extras: bool = False) -> logging.Logger:
    '''
    Create a new STDOUT logger for the module `name` with minimum `level` defined.
    Accepts color codes for the logger name.
    '''
    configs = LOGGING.get(name, {})
    level = configs.get('level', logging.INFO)
    color = configs.get('color', RESET)

    logger = logging.getLogger(name.ljust(5))
    logger.setLevel(level)
    logger.propagate = False

    format = FORMAT.format(color=color, reset=RESET)
    if extras:
        format += ' [%(extra)s]'
    formatter = logging.Formatter(format)

    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if adapter:
        logger = logging.LoggerAdapter(logger, adapter)

    return logger
'''
Multi-module logger for all server components.
'''
import logging

FORMAT = '[%(IP)s][{color}%(name)s{reset}][%(levelname)s]: %(message)s'
RESET = '\x1b[0m'


def get_logger(name: str,
               level: int = logging.INFO,
               color: str = '\033[1;32m',
               extras: bool = False) -> logging.Logger:
    '''
    Create a new STDOUT logger for the module `name` with minimum `level` defined.
    Accepts color codes for the logger name.
    '''
    logger = logging.getLogger(name)
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

    return logger
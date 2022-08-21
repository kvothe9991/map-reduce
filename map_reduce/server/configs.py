import logging
from map_reduce.server.utils import SHA1_BIT_COUNT

# Exceptions.
class ConfigError(Exception):
    pass

# General.
DAEMON_PORT = 8008
BROADCAST_PORT = 8009
REQUESTS_WAIT_TIME = 1  # Seconds.

# DHT.
DHT_NAME = 'chord.dht'
DHT_FINGER_TABLE_SIZE = SHA1_BIT_COUNT // 2
DHT_STABILIZATION_INTERVAL = 0.5
DHT_RECHECK_INTERVAL = 1

# NS.
NS_CONTEST_INTERVAL = 0.01

# Logging.
LOGGING = {
    'main': {
        'color': '\033[1;34m',  # Blue.
        'level': logging.INFO,
    },
    'dht': {
        'color': '\x1b[33;20m', # Yellow.
        'level': logging.DEBUG,
    },
    'dht:s': {
        'color': '\x1b[33;20m', # Yellow.
        'level': logging.DEBUG,
    },
    'ns': {
        'color': '\033[1;32m',  # Green.
        'level': logging.DEBUG,
    },
    'flwr': {
        'color': '\x1b[1;31m',  # Red.
        'level': logging.INFO,
    },
    'mstr': {
        'color': '\x1b[1;31m',  # Red.
        'level': logging.INFO,
    },
    'test': {
        'color': '\x1b[1;31m',  # Red.
        'level': logging.DEBUG,
    },
    'lock': {
        'color': '\x1b[1;31m',  # Red.
        'level': logging.DEBUG,
    }
}
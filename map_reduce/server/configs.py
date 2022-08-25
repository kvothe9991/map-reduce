import Pyro4.socketutil
import socket
import logging

# Exceptions.
class ConfigError(Exception):
    pass

# General.
IP = Pyro4.socketutil.getIpAddress(None, workaround127=None)
DAEMON_PORT = 8008
BROADCAST_PORT = 8009
REQUESTS_WAIT_TIME = 1  # Seconds.

# DHT.
DHT_NAME = 'chord.dht'
DHT_FINGER_TABLE_SIZE = 160 // 2
DHT_STABILIZATION_INTERVAL = 1
DHT_RECHECK_INTERVAL = 1
DHT_REPLICATION_SIZE = 5

# NS.
NS_CONTEST_INTERVAL = 0.01
NS_BACKUP_INTERVAL = 1
NS_BACKUP_KEY = 'ns/backup'

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
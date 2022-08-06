import logging
from map_reduce.server.utils import SHA1_BIT_COUNT

# General.
DAEMON_PORT = 8008
BROADCAST_PORT = 8009
MAIN_LOGGING_COLOR = '\033[1;34m'   # blue.
MAIN_LOGGING_LEVEL = logging.INFO

# DHT.
DHT_NAME = 'chord.dht'
DHT_FINGER_TABLE_SIZE = SHA1_BIT_COUNT // 2
DHT_LOGGING_COLOR = '\x1b[33;20m'   # yellow.
DHT_LOGGING_LEVEL = logging.INFO

# NS.
NS_CONTEST_INTERVAL = 0.1
NS_LOGGING_COLOR = '\033[1;32m'     # green.
NS_LOGGING_LEVEL = logging.INFO
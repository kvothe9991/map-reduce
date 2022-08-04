import server.utils


# General.
DAEMON_PORT = 8008
BROADCAST_PORT = 8009

# DHT.
DHT_NAME = 'chord.dht'
DHT_FINGER_TABLE_SIZE = server.utils.SHA1_BIT_COUNT // 2

# NS.
NS_CONTEST_INTERVAL = 0.1
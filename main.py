import logging
import socket, threading
import Pyro4
import Pyro4.errors
import Pyro4.naming
import Pyro4.socketutil
from Pyro4 import Proxy, URI
Pyro4.config.SERVERTYPE = 'multiplex'
Pyro4.config.POLLTIMEOUT = 3
Pyro4.config.COMMTIMEOUT = 3

from server.nameserver import NameServer
from server.dht import ChordNode
from server.configs import BROADCAST_PORT, DAEMON_PORT, DHT_NAME

# IP retrieval.
HOSTNAME = socket.gethostname()
IP = Pyro4.socketutil.getIpAddress(None, workaround127=True)

# Daemon objects.
DAEMON = Pyro4.Daemon(host=IP, port=DAEMON_PORT)
DHT = ChordNode(URI(f'PYRO:{DHT_NAME}@{IP}:{DAEMON_PORT}'))

# URIs and Daemon registration.
DHT_URI = DAEMON.register(DHT, DHT_NAME)

# Nameserver.
NS = BoundNameServer(IP, BROADCAST_PORT)
NS_THREAD = threading.Thread(target=NS.check_NS_loop)
NS_THREAD.setDaemon(True)
NS_THREAD.start()




if __name__ == '__main__':
    
    # Preface logging.
    logger = logging.getLogger('[MAIN]')
    logger.setLevel(logging.DEBUG)
    logger.propagate = 0
    logger.info(f'Started with:\n\t{HOSTNAME=}\n\t{IP=}.')

    # DHT integration using NS syntax.
    with NS.bind() as ns:
        try:
            ring_uri = ns.lookup(DHT_NAME)
            DHT.join(ring_uri)
        except Pyro4.errors.NamingError:
            ns.register(DHT_NAME, DHT_URI)
            logger.info(f'No DHT found. Registered {DHT_NAME} at nameserver {DHT_URI}.')
    
    # Run combined daemons until SIGINT / SIGKILL.
    try:
        DAEMON.requestLoop()
    except KeyboardInterrupt:
        logger.info('Server stopped. Exiting.')
    finally:
        logger.info('Killing nameserver thread.')
        NS.kill_check_NS_loop()
        if NS_THREAD.join(timeout=0.2) and NS_THREAD.is_alive():
            logger.error('Nameserver thread did not stop after daemon shutdown order.')
        
        logger.info('Killing daemon.')
        DAEMON.shutdown()

        logger.info('Deleting garbage objects.')
        del NS
        del NS_THREAD
        del DAEMON
        del DHT
        
        logger.info('Exiting.')
        exit(0)
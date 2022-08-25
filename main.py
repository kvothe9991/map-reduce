import select
import socket
import logging
import threading
from time import sleep

import Pyro4
import Pyro4.errors
import Pyro4.naming
import Pyro4.socketutil
from Pyro4 import Proxy, URI, Daemon

Pyro4.config.SERVERTYPE = 'thread'
# Pyro4.config.SERVERTYPE = 'multiplex'
# Pyro4.config.COMMTIMEOUT = 3

from map_reduce.server.configs import *
from map_reduce.server.dht import ChordNode, ChordService, service_address
from map_reduce.server.logger import get_logger
from map_reduce.server.nameserver import NameServer

DHT_ADDRESS = URI(f'PYRO:{DHT_NAME}@{IP}:{DAEMON_PORT}')
DHT_SERVICE_ADDRESS = service_address(DHT_ADDRESS)


logger = get_logger('main')
logger = logging.LoggerAdapter(logger, {'IP': IP})

def setup_daemon(ip: str, port: int, objects: dict) -> Daemon:
    ''' Setup main daemon. '''
    daemon = Pyro4.Daemon(host=ip, port=port)
    for name, obj in objects.items():
        daemon.register(obj, name)
    return daemon

def setup_nameserver(ip: str, port: int) -> NameServer:
    ''' Setup the nameserver wrapper. '''
    return NameServer(ip, port)


if __name__ == "__main__":

    # Main daemon.
    objs_for_daemon = {}

    dht = ChordNode(DHT_ADDRESS)
    objs_for_daemon[DHT_ADDRESS.object] = dht

    dht_service = ChordService(DHT_SERVICE_ADDRESS, DHT_ADDRESS)
    objs_for_daemon[DHT_SERVICE_ADDRESS.object] = dht_service

    main_daemon = setup_daemon(IP, DAEMON_PORT, objs_for_daemon)

    # Nameserver.
    nameserver = setup_nameserver(IP, BROADCAST_PORT)
    nameserver.start()
    
    # Hang until nameservers stop contesting.
    sleep(5)
    
    # Start request loop.
    try:
        main_daemon.requestLoop()
    except KeyboardInterrupt:
        logger.info('Server stopped by user.')
    finally:
        logger.info('Killing nameserver.')
        nameserver.stop()
        
        logger.info('Killing main daemon.')
        main_daemon.shutdown()
        
        del nameserver
        del main_daemon
        del dht
        del dht_service

        logger.info('Exiting.')
        exit(0)
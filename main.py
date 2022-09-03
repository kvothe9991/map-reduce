import argparse
import logging
from time import sleep

import Pyro4
import Pyro4.util
import Pyro4.errors
import Pyro4.naming
import Pyro4.socketutil
from Pyro4 import Daemon, URI
Pyro4.config.SERVERTYPE = 'thread'
Pyro4.config.SERIALIZER = 'dill'
Pyro4.config.SERIALIZERS_ACCEPTED.add('dill')

from map_reduce.client.client import run_client
from map_reduce.server.configs import *
from map_reduce.server.dht import ChordNode, ChordService, service_address
from map_reduce.server.logger import get_logger
from map_reduce.server.nameserver import NameServer
from map_reduce.server.nodes import Master, Follower, RequestHandler

DHT_ADDRESS = URI(f'PYRO:{DHT_NAME}@{IP}:{DAEMON_PORT}')
DHT_SERVICE_ADDRESS = service_address(DHT_ADDRESS)
MASTER_ADDRESS = URI(f'PYRO:{MASTER_NAME}@{IP}:{DAEMON_PORT}')
FOLLOWER_ADDRESS = URI(f'PYRO:{FOLLOWER_NAME}@{IP}:{DAEMON_PORT}')
RQH_ADDRESS = URI(f'PYRO:{RQ_HANDLER_NAME}@{IP}:{DAEMON_PORT}')


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


def run_servers():
    # Main daemon.
    objs_for_daemon = {}

    dht = ChordNode(DHT_ADDRESS)
    objs_for_daemon[DHT_ADDRESS.object] = dht

    dht_service = ChordService(DHT_SERVICE_ADDRESS, DHT_ADDRESS)
    objs_for_daemon[DHT_SERVICE_ADDRESS.object] = dht_service

    master = Master(MASTER_ADDRESS)
    objs_for_daemon[MASTER_NAME] = master

    follower = Follower(FOLLOWER_ADDRESS)
    objs_for_daemon[FOLLOWER_NAME] = follower

    rq_handler = RequestHandler(RQH_ADDRESS)
    objs_for_daemon[RQ_HANDLER_NAME] = rq_handler

    main_daemon = setup_daemon(IP, DAEMON_PORT, objs_for_daemon)

    # Nameserver.
    nameserver = setup_nameserver(IP, BROADCAST_PORT)
    nameserver.delegate(RQH_ADDRESS, rq_handler.start, rq_handler.stop)
    nameserver.delegate(MASTER_ADDRESS, master.start, master.stop)
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser('Start a map-reduce module.')
    parser.add_argument('module', action='store', choices=['server', 'client'])
    args = parser.parse_args()
    if args.module == 'server':
        run_servers()
    else:
        run_client()
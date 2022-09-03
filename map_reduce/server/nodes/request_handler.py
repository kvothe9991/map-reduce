from logging import LoggerAdapter
import time

import Pyro4
import Pyro4.errors
from Pyro4 import Proxy, URI

from map_reduce.server.utils import chunks_from, service_address
from map_reduce.server.configs import ( DHT_NAME, DHT_SERVICE_NAME, MASTER_MAP_CODE, MASTER_REDUCE_CODE,
                                        MASTER_DATA, REQUEST_RETRIES, REQUEST_TIMEOUT,
                                        IP, RESULTS_KEY )
from map_reduce.server.logger import get_logger
logger = get_logger('rq')


@Pyro4.expose
@Pyro4.behavior('single')
class RequestHandler:
    def __init__(self, address: URI):
        self.address = address
        self.user_address = None
        global logger
        logger = LoggerAdapter(logger, {'IP': IP})
    
    def start(self):
        '''
        Start the request handler in the nameserver.
        '''
        with Pyro4.locateNS() as ns:
            ns.register(self.address.object, self.address)
    
    def stop(self):
        '''
        Remove the request handler from the nameserver.
        '''
        with Pyro4.locateNS() as ns:
            if ns.lookup(self.address.object) == self.address:
                ns.remove(self.address.object)

    def startup(self, user_addr, input_data, map_function, reduce_function) -> bool:
        '''
        Start up the map-reduce process on the provided data. Request data is
        staged to DHT, input is split in chunks, functions stay serialized.

        Returns True if the process was started successfully, False otherwise.
        '''
        logger.info(f'Received request from {user_addr!s}.')
        self.user_address = user_addr
        input_data_chunks = { f'map/{i}': data for i,data in chunks_from(input_data).items() }
        logger.info(f'Chunks: {list(input_data_chunks.keys())}')
        for _ in range(REQUEST_RETRIES):
            try:
                with Pyro4.locateNS() as ns:
                    dht_addr = ns.lookup(DHT_NAME)
                with Proxy(service_address(dht_addr)) as dht:
                    dht.insert(MASTER_MAP_CODE, map_function)
                    dht.insert(MASTER_REDUCE_CODE, reduce_function)
                    dht.insert(MASTER_DATA, input_data_chunks)
                    k = len(input_data_chunks)
                    logger.info(f'Pushed input data: {k} chunks: {input_data_chunks.keys()}.')
                return True
            except Pyro4.errors.CommunicationError as e:
                logger.error(f'{e.__class__.__name__}: {e}')
                time.sleep(REQUEST_TIMEOUT)
                continue
        return False

    def notify_results(self):
        '''
        Notify the user who requested the process with the results.
        '''
        with Pyro4.locateNS() as ns:
            with Proxy(service_address(ns.lookup(DHT_NAME))) as dht:
                results = dht.lookup(RESULTS_KEY)
        with Proxy(self.user_address) as user:
            user.notify_results(results)
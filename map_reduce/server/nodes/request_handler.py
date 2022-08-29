import time

import Pyro4
import Pyro4.errors
from Pyro4 import Proxy, URI

from map_reduce.server.utils import chunks_from
from map_reduce.server.configs import ( DHT_NAME, MASTER_MAP_CODE, MASTER_REDUCE_CODE,
                                        MASTER_DATA, REQUEST_RETRIES, REQUEST_TIMEOUT,
                                        IP )
from map_reduce.server.logger import get_logger
logger = get_logger('rq', adapter={'IP': IP})


@Pyro4.expose
class RequestHandler:
    def __init__(self):
        self.user_address = None

    def startup(self, user_addr, input_data, map_function, reduce_function) -> bool:
        '''
        Start up the map-reduce process on the provided data. Request data is
        staged to DHT, input is split in chunks, functions stay serialized.

        Returns True if the process was started successfully, False otherwise.
        '''
        self.user_address = user_addr
        input_data_chunks = { f'map/{i}': data for i,data in chunks_from(input_data) }
        for _ in range(REQUEST_RETRIES):
            try:
                with Pyro4.locateNS() as ns:
                    dht_addr = ns.lookup(DHT_NAME)
                with Proxy(dht_addr) as dht:
                    dht.insert(MASTER_MAP_CODE, map_function)
                    dht.insert(MASTER_REDUCE_CODE, reduce_function)
                    dht.insert(MASTER_DATA, input_data_chunks)
                return True
            except Pyro4.errors.CommunicationError:
                time.sleep(REQUEST_TIMEOUT)
                continue
        return False

    def notify_results(self, results):
        '''
        Notify the user who requested the process with the results.
        '''
        with Proxy(self.user_address) as user:
            user.notify_results(results)
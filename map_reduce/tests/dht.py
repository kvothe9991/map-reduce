import unittest
from time import sleep

import Pyro4
import Pyro4.errors
import logging
from Pyro4 import Proxy
from Pyro4.naming import NameServer

from map_reduce.server.configs import DHT_NAME, DHT_SERVICE_NAME
from map_reduce.server.logger import get_logger
from map_reduce.server.utils import daemon_address, service_address

logger = get_logger('test', adapter={'IP': ''})


class DHT_DataLayerTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Test data.
        self._test_data = { 'charge': 'uSjWpfjfvIawHQUvFFWf',
                            'Democrat': -329.131172394825,
                            'cell': 'NevfLCdfQnLOlaCCUmIa',
                            'third': 2509,
                            'lead': 'ohsIouukKketTEHqkVpf',
                            'type': 4044,
                            'between': 'jOzQLmJqkOlsAERaRDNL',
                            'involve': 'dramirez@example.com',
                            'to': 'https://www.barnett.com/author/',
                            'human': 'RXgOjDjJVkFQznSjVswI' }
        
        # Search for the DHT in the nameserver.
        with Pyro4.locateNS() as ns:
            self._dht_addr = service_address(ns.lookup(DHT_NAME))
        
    
    def test_1_data_insertion(self):
        
        # Data insertion.
        with Proxy(self._dht_addr) as dht:
            for k,v in self._test_data.items():
                dht.insert(k,v)
        logger.info('Inserted data into DHT.')
        
        # Await some time for data to replicate.
        sleep(0.25)
    
    def test_2_data_in_dht(self):
        with Proxy(self._dht_addr) as dht:
            items_in_dht: dict = dht.items
            for repl in dht.replicated_items:
                items_in_dht.update(repl)
        
        for k,v in items_in_dht.items():
            logger.info(f'Found {k!r}:{v!r} in DHT.')
    
    def test_3_data_replication(self):
        
        # Replication checks.
        with Proxy(self._dht_addr) as dht:
            items_in_dht: dict = dht.items
            for repl in dht.replicated_items:
                items_in_dht.update(repl)
        
        pending = self._test_data.copy()
        for k in items_in_dht.keys():
            pending.pop(k, None)
        
        self.assertTrue(expr=(len(pending) == 0),
                        msg=f'Not all inserted data was found in the DHT. '
                            f'The following items were not found in the DHT: {pending}')


# # Test 1.
# # Test that the DHT is working.
# with Pyro4.locateNS() as ns:
#     ns: NameServer
#     dht_uri = ns.lookup(DHT_NAME)

# # Insert some data.
# data = {
#     'foo': 'bar',
#     'asd': [1,2,3,4,5],
#     'ip' :'172.18.0.5'
# }

# logger.info(f'Found DHT at {dht_uri}.')
# with Proxy(service_address(dht_uri)) as dht:
#     try:
#         for k,v in data.items():
#             logger.info(f'Inserting {k!r}:{v!r}.')
#             dht.insert(k,v)

#             # sleep(1)
            
#             logger.info(f'Looking up {k!r}.')
#             rv = dht.lookup(k)
#             if v == rv:
#                 logger.info('Result was identical.')
#             else:
#                 logger.error(f'Result {rv!r} was different from original value {v!r}!')
            
#             # logger.info(f'Removing {k!r}.')
#             # dht.remove(k)
#     except Pyro4.errors.TimeoutError as e:
#         logger.error(str(e))
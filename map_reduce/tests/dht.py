from time import sleep
import Pyro4
import Pyro4.errors
import logging
from Pyro4 import Proxy
from Pyro4.naming import NameServer

from map_reduce.server.configs import DHT_NAME
from map_reduce.server.logger import get_logger
from map_reduce.server.dht.data_layer import service_address

logger = get_logger('test', adapter={'IP': ''})


# Test 1.
# Test that the DHT is working.
with Pyro4.locateNS() as ns:
    ns: NameServer
    dht_uri = ns.lookup(DHT_NAME)

# Insert some data.
data = {
    'foo': 'bar',
    'asd': [1,2,3,4,5],
    'ip' :'172.18.0.5'
}

logger.info(f'Found DHT at {dht_uri}.')
with Proxy(service_address(dht_uri)) as dht:
    try:
        for k,v in data.items():
            logger.info(f'Inserting {k!r}:{v!r}.')
            dht.insert(k,v)

            sleep(1)
            
            logger.info(f'Looking up {k!r}.')
            rv = dht.lookup(k)
            if v == rv:
                logger.info('Result was identical.')
            else:
                logger.error(f'Result {rv!r} was different from original value {v!r}!')
            
            # logger.info(f'Removing {k!r}.')
            # dht.remove(k)
    except Pyro4.errors.TimeoutError as e:
        logger.error(str(e))
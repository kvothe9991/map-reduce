import Pyro4
import unittest
from map_reduce.server.logger import get_logger
from map_reduce.server.configs import DHT_NAME
from map_reduce.server.utils import service_address, unpack

logger = get_logger('test', adapter={'IP': ''})


class NameServerPropagationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_dht_is_propagated(self):
        with Pyro4.locateNS() as ns:
            ns_listings = ns.list()
        self.assertTrue(DHT_NAME in ns_listings)
    
    def test_ns_ip(self):
        print()
        with Pyro4.locateNS() as ns:
            logger.info(f'Found NS at {ns._pyroUri}.')
        return
    
    def test_dht_has_info(self):
        with Pyro4.locateNS() as ns:
            dht_addr = ns.lookup(DHT_NAME)
        dht_addr = service_address(dht_addr)
        with Pyro4.Proxy(dht_addr) as dht:
            print( dht.items )

if __name__ == "__main__":
    unittest.main()
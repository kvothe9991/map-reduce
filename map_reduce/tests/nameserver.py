import Pyro4
import unittest
from map_reduce.server.logger import get_logger
from map_reduce.server.configs import DHT_NAME

logger = get_logger('test', adapter={'IP': ''})


class NameServerPropagationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        with Pyro4.locateNS() as ns:
            self.ns_listings = ns.list()
    
    def test_dht_is_propagated(self):
        self.assertTrue(DHT_NAME in self.ns_listings)


if __name__ == "__main__":
    unittest.main()
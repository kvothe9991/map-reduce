import Pyro4
from Pyro4 import Proxy
from Pyro4.naming import NameServer

from map_reduce.server.dht import ChordNode
from map_reduce.server.configs import DHT_NAME


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
with Proxy(dht_uri) as dht:
    for k,v in data.items():
        dht.insert(k,v)
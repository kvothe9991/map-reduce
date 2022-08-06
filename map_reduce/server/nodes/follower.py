import Pyro4
import logging
from typing import List
from Pyro4 import Proxy, URI

from map_reduce.server.nodes.threader_node import ThreaderNode
from map_reduce.server.logger import get_logger

logger = get_logger('flwr', logging.INFO, '\x1b[1;31m') # Red color.


@Pyro4.expose
class Follower(ThreaderNode):
    """
    Prime follower server.
    """

    def __init__(self, address: URI, master_address: URI) -> None:
        super().__init__(address)
        self._master_address = master_address

    # guarda un buff local. Ya no es local. (!)
    def save_buff(self, buff):
        
        # ...

        # Guardar en el DHT.
        with Pyro4.locateNS() as nameserver:
            dht_uri = nameserver.lookup('chord.dht')

        with Proxy(dht_uri) as dht:
            dht.insert('<key>', buff)

        # ...

    # remueve un buff local
    def remove_buff(self, buff):
        pass

    # filtrar y mappear
    def map(self, in_key, in_value, map_function):
        mapper = Mapper(self._address, in_key, in_value, map_function)
        mapper.run()
        inter_key, inter_values = mapper.result()
        buff = (inter_key, inter_values)
        self.save_buff(buff)

    # agrupar por llaves y calcular
    def reduce(self, inter_key, inter_values,  reduce_function):
        reducer = Reducer(self._address, inter_key, inter_values, reduce_function)
        reducer.run()
        out_values = reducer.result()
        buff = out_values
        self.save_buff(buff)


@Pyro4.expose
class Mapper(ThreaderNode):
    def __init__(self, address: URI, in_key, in_value, map_function) -> None:
        super().__init__(address)
        self._in_key = in_key
        self._in_value = in_value
        self._map_function = map_function

    def run(self):
        pass

    def result(self):
        pass


@Pyro4.expose
class Reducer(ThreaderNode):
    def __init__(self, address: URI, inter_key, inter_values, reduce_function) -> None:
        super().__init__(address)
        self._inter_key = inter_key
        self._inter_values = inter_values
        self._reduce_function = reduce_function

    def run(self):
        pass

    def result(self):
        pass

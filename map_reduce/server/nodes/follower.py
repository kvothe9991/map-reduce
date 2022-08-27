import Pyro4
import logging
from typing import List
from Pyro4 import Proxy, URI

from map_reduce.server.nodes.threader_node import ThreaderNode
from map_reduce.server.logger import get_logger

logger = get_logger('flwr')


@Pyro4.expose
class Follower(ThreaderNode):
    """
    Prime follower server.
    """

    def __init__(self, address: URI, master_address: URI) -> None:
        super().__init__(address)
        self._master_address = master_address

    def start(self):
        with Proxy(self._master_address) as proxy:
            proxy.request_role(self._address)

    # guarda un buff local
    # no hace falta
    def save_buff(self, buff):
        # ...
        # Guardar en el DHT.
        with Pyro4.locateNS() as nameserver:
            dht_uri = nameserver.lookup('chord.dht')

        with Proxy(dht_uri) as dht:
            dht.insert('<key>', buff)
        # ...

    # remueve un buff local
    # no hace falta
    def remove_buff(self, buff):
        pass

    # filtrar y mappear
    def map(self, split, map_function):
        mapper = Mapper(self._address, split, map_function)
        mapper.run()
        out_data = mapper.result()
        return out_data

    # agrupar por llaves y calcular
    def reduce(self, inter_split, reduce_function):
        reducer = Reducer(self._address, inter_split, reduce_function)
        reducer.run()
        out_data = reducer.result()
        return out_data

@Pyro4.expose
class Mapper(ThreaderNode):
    def __init__(self, address: URI, split, map_function) -> None:
        super().__init__(address)
        self._split = split
        self._map_function = map_function
        self._result = []

    def run(self):
        for in_key, in_value in self._split:
            self._result.append(self._map_function(in_key, in_value))

    def result(self):
        return self._result


@Pyro4.expose
class Reducer(ThreaderNode):
    def __init__(self, address: URI, inter_split, reduce_function) -> None:
        super().__init__(address)
        self._inter_split = inter_split
        self._reduce_function = reduce_function
        self._result = []

    def run(self):
        for inter_key, inter_value in self._inter_split:
            self._result.append(self._reduce_function(inter_key, inter_value))

    def result(self):
        return self._result

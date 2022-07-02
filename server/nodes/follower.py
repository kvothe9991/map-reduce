from server.nodes.threader_node import ThreaderNode
import Pyro4
from Pyro4 import URI
from typing import List


@Pyro4.expose
class Follower(ThreaderNode):
    """
    Prime follower server.
    """

    def __init__(self, address: URI, master_address: URI) -> None:
        super().__init__(address)
        self._master_address = master_address

    # guarda un buff local
    def save_buff(self, buff):
        pass

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

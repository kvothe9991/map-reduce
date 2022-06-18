from server.nodes.threader_node import ThreaderNode
from Pyro4 import URI


class Follower(ThreaderNode):
    def __init__(self, address: URI) -> None:
        super().__init__(address)

    def mapper(self, key, value):
        mapper = Mapper(self._address, key, value)
        pass

    def reducer(self, key, values_list):
        reducer = Reducer(self._address, key, values_list)
        pass


class Mapper(ThreaderNode):
    def __init__(self, address: URI, key, value) -> None:
        super().__init__(address)
        self._key = key
        self._value = value

    def run(self):
        pass


class Reducer(ThreaderNode):
    def __init__(self, address: URI, key, values_list) -> None:
        super().__init__(address)
        self._key = key
        self._values_list = values_list

    def run(self):
        pass

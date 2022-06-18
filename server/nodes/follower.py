from server.nodes.threader_node import ThreaderNode
from Pyro4 import URI


class Follower(ThreaderNode):
    def __init__(self, address: URI) -> None:
        super().__init__(address)

    def map(self, key, value):
        pass

    def reduce(self, key, values_list):
        pass

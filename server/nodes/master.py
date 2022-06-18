from server.nodes.threader_node import ThreaderNode
from Pyro4 import URI


def file_system():
    pass


class Master(ThreaderNode):
    def __init__(self, address: URI) -> None:
        super().__init__(address)

        self._followers = []
        self._filesystem = file_system()

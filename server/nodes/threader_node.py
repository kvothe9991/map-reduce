import Pyro4
from Pyro4 import URI


class ThreaderNode:
    def __init__(self, address: URI) -> None:
        self._address = address

import Pyro4
from Pyro4 import URI

Pyro4.expose
class ThreaderNode:
    """
    Prime ThreaderNode server.
    """
    def __init__(self, address: URI) -> None:
        self._address = address

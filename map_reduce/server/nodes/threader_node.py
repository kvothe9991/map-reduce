import Pyro4
from Pyro4 import URI

DAEMON_ADDRESS = URI("PYRO:<name>@0.0.0.0:7777")


@Pyro4.expose
class ThreaderNode:
    """
    Prime ThreaderNode server.
    """

    def __init__(self, address: URI) -> None:
        self._address = address
        self._address_book = []
        self._master = None
        self._follower = None

    # guarda una nueva address a la lista de address_book
    def save_address(self, address: URI):
        if not (address in self._address_book):
            self._address_book.append(address)

    # remover una nueva address a la lista de address_book
    def remove_address(self, address: URI):
        if address in self._address_book:
            self._address_book.remove(address)

    # empezar a cumplir el role de follower
    def accept_role(self, master_address: URI):
        follower = follower.Follower(self._address, master_address)
        self._follower = follower

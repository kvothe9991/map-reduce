from server.nodes.threader_node import ThreaderNode
import Pyro4
from Pyro4 import URI
from utils import alive


def file_system():
    pass

@Pyro4.expose
class Master(ThreaderNode):
    """
    Prime master server.
    """
    def __init__(self, address: URI) -> None:
        super().__init__(address)

        self._followers = []
        self._pending_fallowers = []
        self._filesystem = file_system()

    def request_pending_follower(self, address: URI):
        if not (address in self._pending_fallowers):
            self._pending_fallowers.append(address)
            #es mejor tenerlo todo en una sola función
            #declararlo follower en el instante que lo pide

    def accept_pendig_follower(self):
        for address in self._pending_fallowers:
            proxy = Pyro4.Proxy(address)
            if alive(proxy):
                self._followers.append(address)
                #notificar para que sea follower
            self._pending_fallowers.remove(address)
                
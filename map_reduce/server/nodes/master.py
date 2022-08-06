import Pyro4
import logging
from Pyro4 import URI
from utils import alive

from map_reduce.server.nodes.threader_node import ThreaderNode
from map_reduce.server.logger import get_logger

logger = get_logger('mstr', logging.INFO, '\x1b[1;31m') # Red color.

class FileSystem:
    def __init__(self) -> None:
        pass


@Pyro4.expose
class Master(ThreaderNode):
    """
    Prime master server.
    """

    def __init__(self, address: URI) -> None:
        super().__init__(address)

        self._followers = [] = []
        self._idle_followers = []
        self._filesystem = FileSystem()

        self._pending_map_tasks = []
        self._assigned_map_tasks = []
        self._completed_map_tasks = []

        self._pending_reduce_tasks = []
        self._assigned_reduce_tasks = []
        self._completed_reduce_tasks = []

    # threader node pide al master ser follower
    def request_role(self, address: URI):
        proxy = Pyro4.Proxy(address)
        if alive(proxy):
            self._followers.append(address)
            proxy.accept_role(self._address)  # notificar para que sea follower

    # asignar una tarea de map a un follower
    def assign_map_task(self, address: URI):
        proxy = Pyro4.Proxy(address)
        in_key, in_value, map_function = 0

        if alive(proxy):
            proxy.map(in_key, in_value, map_function)

    # asignar una tarea de reduce a un follower
    def assign_reduce_task(self, address: URI):
        proxy = Pyro4.Proxy(address)
        inter_key, inter_values, reduce_function = 0

        if alive(proxy):
            proxy.reduce(inter_key, inter_values,  reduce_function)

    # valida un buff local
    def validate_buff(self, buff):
        pass

    # revisa si existe el buff local
    def contains_buff(self, buff):
        pass

    # follower termina su tarea map y notifica sobre la ubicación de la salida
    def map_response(self):
        pass

    # follower termina su tarea reduce asignada y notifica sobre la ubicación de la salida
    def reduce_response(self):
        pass
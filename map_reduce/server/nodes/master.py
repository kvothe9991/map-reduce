import Pyro4
import logging
import time
from Pyro4 import URI
from utils import alive
from map_reduce.server.nodes.threader_node import ThreaderNode
from map_reduce.server.logger import get_logger

logger = get_logger('mstr')

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

        self._followers = []
        self._idle_followers = []
        self._filesystem = FileSystem()

    # para configurar e iniciar al master
    def start(self, input_data, map_function, reduce_function):
        logger.info(f'Master started.')
        self._splits = list(self.split_input_data(input_data, 16))
        self._inter_splits = []
        self._map_function = map_function
        self._reduce_function = reduce_function
        start_time = time.time()

        self._pending_map_tasks = []
        self._assigned_map_tasks = []
        self._completed_map_tasks = []

        self._pending_reduce_tasks = []
        self._assigned_reduce_tasks = []
        self._completed_reduce_tasks = []

        # revisamos cuantas tareas tenemos pendientes
        for id, data in self._splits:
            self._pending_map_tasks.append(id)

        # asignamos los splits a los followers para los maps
        # hacer ping a los asignados
        while(len(self._pending_map_tasks) > 0):
            id_map_task = self._pending_map_tasks[0]

            while(len(self._idle_followers) < 0):
                # esperar
                pass
            
            idle_follower = self._idle_followers.pop()
            split = self.get_split(id_map_task)
            
            if(self.assign_map_task(idle_follower, split)):
                self._assigned_map_tasks.append((id_map_task, idle_follower))
                self._pending_map_tasks.remove(id_map_task)
        
        while(len(self._completed_map_tasks) < len(self._splits)):
            # esperar
            # ...
            # los maps están completos
            pass
        
        # asignamos los splits a los followers para los reduce
        # hacer ping a los asignados
        while(len(self._pending_reduce_tasks) > 0):
            id_reduce_task = self._pending_reduce_tasks[0]

            while(len(self._idle_followers) < 0):
                # esperar
                pass
            
            idle_follower = self._idle_followers.pop()
            inter_split = self.get_inter_split(id_reduce_task)
            
            if(self.assign_reduce_task(idle_follower, inter_split)):
                self._assigned_reduce_tasks.append((id_reduce_task, idle_follower))
                self._pending_reduce_tasks.remove(id_reduce_task)

        while(len(self._completed_reduce_tasks) < len(self._inter_splits)):
            # esperar
            # ...
            # los reduces están completos
            pass
        
        end_time = time.time()
        print('Total time elapsed: %.2fsec' % (end_time - start_time))


    # threader node pide al master ser follower
    def request_role(self, address: URI):
        proxy = Pyro4.Proxy(address)
        if alive(proxy):
            self._followers.append(address) # revisar si este es el momento correcto para asumirlo idle
            proxy.accept_role(self._address)  # notificar para que sea follower

    # asignar una tarea de map a un follower
    def assign_map_task(self, address: URI, split):
        proxy = Pyro4.Proxy(address)
        map_function = self._map_function

        if alive(proxy):
            proxy.map(split, map_function)
            return True

        return False

    # asignar una tarea de reduce a un follower
    def assign_reduce_task(self, address: URI, split):
        proxy = Pyro4.Proxy(address)
        reduce_function = self._reduce_function

        if alive(proxy):
            proxy.reduce(split,  reduce_function)
            return True

        return False

    # valida un split local
    # creo que no hace falta
    def validate_split(self, split):
        pass

    # revisa si existe el split local
    # creo que no hace falta
    def contains_split(self, split):
        pass

    # follower termina su tarea map y notifica sobre la ubicación de la salida
    def map_response(self):
        pass

    # follower termina su tarea reduce asignada y notifica sobre la ubicación de la salida
    def reduce_response(self):
        pass

    # divide los datos de entrada en splits
    def split_input_data(self, input_data, size):
        id = 0 
        for i in range(0, len(input_data), size):
            yield (id, input_data[i:i + size])
            id = id + 1

    def get_split(self, id):
        for current_id, current_data in self._splits:
            if(current_id == id):
                return current_data
        return False
    
    def get_inter_split(self, id):
        for current_id, current_data in self._inter_splits:
            if(current_id == id):
                return current_data
        return False
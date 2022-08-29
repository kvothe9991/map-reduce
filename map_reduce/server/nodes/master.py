import time
from threading import Lock, Thread
from typing import Any

import Pyro4
from Pyro4 import Proxy, URI

from map_reduce.server.nodes.tasks import Task
from map_reduce.server.configs import ( DHT_NAME, MASTER_DATA, MASTER_BACKUP_KEY,
                                        MASTER_MAP_CODE, MASTER_REDUCE_CODE,
                                        REQUEST_TIMEOUT )
from map_reduce.server.utils import ( reachable, service_address, spawn_thread,
                                      kill_thread, LockedObject )
from map_reduce.server.logger import get_logger
logger = get_logger('mstr')


class TaskGroup:
    def __init__(self, pending = {}, assigned = {}, completed = {}):
        self.pending: dict = pending
        self.assigned: dict = assigned
        self.completed: dict = completed
    
    @property
    def any(self):
        return len(self.pending) > 0 or len(self.assigned) > 0
    
    @property
    def none(self):
        return len(self.pending) == 0 and len(self.assigned) == 0
    
    def set_as_complete(self, task_id):
        '''
        Searches for a task by id in the pending or assigned sections, then flags
        it as completed.
        '''
        for container in [self.pending, self.assigned]:
            if task_id in container:
                task = container.pop(task_id)
                self.completed[task_id] = task
                return True
        else:
            logger.error(f"Set task {task_id} as complete but couldn't find it")
            return False

    def reset(self):
        ''' Resets data to default. '''
        self.pending.clear()
        self.assigned.clear()
        self.completed.clear()
    
    def reset_assigned_to_pending(self):
        ''' Resets all assigned tasks to pending. '''
        self.pending.update(self.assigned)
        self.assigned.clear()

    def dump(self):
        ''' Returns data in tuple form. '''
        return (self.pending, self.assigned, self.completed)
    
    def load(self, ts: tuple):
        ''' Instances a new TaskGroup in tuple form. '''
        assert len(ts) == 3, 'Provided tuple must contain pending, assigned and completed tasks.'
        self.pending, self.assigned, self.completed = ts


@Pyro4.expose
@Pyro4.behavior('single')
class Master:
    '''
    Prime master server, redirects tasks to subscribed followers over the network.

    TODO:
        - Master backup in case of death.
            - Backup lock for awaiting.
        - Master recovery from backup.
        - Follower hang on master death.
    '''
    def __init__(self, address: URI):
        # Basic attribs.
        self._address = address
        
        # Tasking and workers.
        self._followers = set()
        self._idle_followers = set()
        self._map_tasks = TaskGroup()
        self._reduce_tasks = TaskGroup()
        self._results = []

        # Thread safety locks.
        self._followers_lock = Lock()
        self._map_tasks_lock = Lock()
        self._reduce_tasks_lock = Lock()

        # Map/reduce functions, these stay serialized.
        self._map_function: bytes = None
        self._reduce_function: bytes = None

        # Threads.
        self._master_thread: Thread = None
        self._backup_thread: Thread = None


    # Properties.
    @property
    def _dht_service(self) -> Proxy:
        ''' Returns a live proxy to the DHT service. '''
        with Pyro4.locateNS() as ns:
            return Proxy(service_address(ns.lookup(DHT_NAME)))


    # DHT layer.
    def _get_serialized_functions(self) -> tuple[bytes, bytes]:
        with self._dht_service as dht:
            map_serialized = dht.lookup(MASTER_MAP_CODE)
            reduce_serialized = dht.lookup(MASTER_REDUCE_CODE)
        if map_serialized is None or reduce_serialized is None:
            return None
        else:
            return (map_serialized, reduce_serialized)

    def _get_request_data(self) -> dict:
        with self._dht_service as dht:
            return dht.lookup(MASTER_DATA)
    
    def _get_backup(self):
        ''' Loads data from backup if available. '''
        with self._dht_service as dht:
            return dht.lookup(MASTER_BACKUP_KEY)


    # Exposed RPCs.
    def subscribe(self, follower_address: URI):
        '''
        Subscribes a follower to the master.
        '''
        self._idle_followers.add(follower_address)
        logger.info(f'{follower_address} subscribed to master.')
    
    def report_task(self, follower: URI, task_id: int, task_func: bytes, result: Any):
        '''
        RPC to report task completion from a remote follower.
        '''
        # Set follower to idle.
        with self._followers_lock:
            if follower in self._followers:
                self._followers.remove(follower)
                self._idle_followers.add(follower)
            else:
                idle = 'marked as idle' if follower in self._idle_followers else 'not found'
                logger.error(f'Follower reported a task but was {idle}.')
        
        # Find the task's group and mark it as done.
        if task_func == self._map_function:
            # Get map result then group values by the result's key.
            with self._map_tasks_lock:
                self._map_tasks.set_as_complete(task_id)
            with self._reduce_tasks_lock:
                out_key, inter_val = result
                self._reduce_tasks.pending.setdefault(out_key, []).append(inter_val)
        elif task_func == self._reduce_function:
            # Get reduce results.
            with self._reduce_tasks_lock:
                self._reduce_tasks.set_as_complete(task_id)
                out_val = result
                self._results.append(out_val)
        else:
            raise ValueError('Received a task function that is not map or reduce.')

    def start(self):
        '''
        Starts up the master server. Useful for delegating the start to other logic,
        such as the nameserver.
        '''
        if sf := self._get_serialized_functions():
            logger.info('Found map-reduce request.')

            # Stage start time. TODO: stage to DHT.
            self._start_time = time.time()

            # Store the tasks' functions, serialized.
            self._map_function, self._reduce_function = sf

            # Check if a previous master died.
            if backup := self._get_backup():                
                # Load tasks, assume the assigned tasks have to be redone.
                self._map_tasks.load(backup[0])
                self._map_tasks.reset_assigned_to_pending()
                self._reduce_tasks.load(backup[1])
                self._reduce_tasks.reset_assigned_to_pending()

                # Load followers, assume all as idle.
                self._followers.clear()
                self._idle_followers = backup[2]

                logger.info('Loaded backup from previous master.')
            else:
                # Split the input data into smaller chunks, which will be mapped.
                self._map_tasks.reset()
                self._reduce_tasks.reset()
                self._map_tasks.pending = self._get_request_data()

                logger.info('No backup found. Started from scratch.')
            
            # Start the master task-routing loop.
            self._master_thread = spawn_thread(self._master_loop)
            self._backup_thread = spawn_thread(self._backup_loop)
        else:
            logger.error(f"Can't startup master if no request is staged on DHT.")

    def _assign_task(self, tasks: TaskGroup, func: bytes) -> bool:
        '''
        Assign any pending task from the provided group to any idle worker.
        '''
        with self._followers_lock, self._map_tasks_lock, self._reduce_tasks_lock:
            if self._idle_followers:
                worker_addr = self._idle_followers.pop()
                if reachable(worker_addr):
                    with Proxy(worker_addr) as worker:
                        if tasks.pending:
                            task_id, task = tasks.pending.popitem()
                            tasks.assigned[task_id] = task
                            self._followers.add(worker_addr)
                            worker.do(task, func)
                            return True
        return False

    def _master_loop(self):
        '''
        Main loop of the master server.
        '''
        # Await all map tasks.
        while self._map_tasks.any:
            self._assign_task(self._map_tasks, self._map_function)
            time.sleep(REQUEST_TIMEOUT)

        # Await all reduce tasks.
        while self._reduce_tasks.any:
            self._assign_task(self._reduce_tasks, self._reduce_function)
            time.sleep(REQUEST_TIMEOUT)
        
        # Post results to DHT and notify the request if finished.
        with self._dht_service as dht:
            dht.insert('map-reduce/final-results')
        


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
    def validate_split(self, split):
        pass

    # revisa si existe el split local
    def contains_split(self, split):
        pass

    # follower termina su tarea map y notifica sobre la ubicación de la salida
    def map_response(self):
        pass

    # follower termina su tarea reduce asignada y notifica sobre la ubicación de la salida
    def reduce_response(self):
        pass

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
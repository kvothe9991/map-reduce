from logging import LoggerAdapter
import time
from threading import Lock, Thread
from typing import Any

import Pyro4
import Pyro4.errors
from Pyro4 import Proxy, URI

from map_reduce.server.configs import ( DHT_NAME, MASTER_DATA, MASTER_BACKUP_KEY,
                                        MASTER_MAP_CODE, MASTER_REDUCE_CODE,
                                        REQUEST_TIMEOUT, MASTER_BACKUP_INTERVAL, RESULTS_KEY, RQ_HANDLER_NAME )
from map_reduce.server.utils import ( reachable, service_address, spawn_thread,
                                      kill_thread )
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
        - Follower hang on master death.
    '''
    def __init__(self, address: URI):
        # Basic attribs.
        self._address = address
        
        # Tasking and followers.
        self._followers = set()
        self._idle_followers = set()
        self._map_tasks = TaskGroup()
        self._reduce_tasks = TaskGroup()
        self._results = {}

        # Thread safety locks.
        self._followers_lock = Lock()
        self._map_tasks_lock = Lock()
        self._reduce_tasks_lock = Lock()
        self._results_lock = Lock()

        # Map/reduce functions, these stay serialized.
        self._map_function: bytes = None
        self._reduce_function: bytes = None

        # Threads.
        self._alive = False
        self._master_thread: Thread = None
        self._backup_thread: Thread = None

        global logger
        logger = LoggerAdapter(logger, {'IP': self._address.host})


    # Properties.
    @property
    def _dht_service(self) -> Proxy:
        ''' Returns a live proxy to the DHT service. '''
        with Pyro4.locateNS() as ns:
            return Proxy(service_address(ns.lookup(DHT_NAME)))


    # DHT layer.
    def _get_serialized_functions(self) -> tuple[bytes, bytes]:
        try:
            with self._dht_service as dht:
                map_serialized = dht.lookup(MASTER_MAP_CODE)
                reduce_serialized = dht.lookup(MASTER_REDUCE_CODE)
            if map_serialized is None or reduce_serialized is None:
                return None
            else:
                return (map_serialized, reduce_serialized)
        except Pyro4.errors.CommunicationError:
            return None

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
        logger.info(f'{follower_address!s} subscribed to master.')
    
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
                for out_key, inter_val in result:
                    self._reduce_tasks.pending.setdefault(out_key, []).append(inter_val)
        elif task_func == self._reduce_function:
            # Get reduce results.
            with self._reduce_tasks_lock, self._results_lock:
                self._reduce_tasks.set_as_complete(task_id)
                out_key, out_vals = task_id, result
                self._results[out_key] = (out_vals)
        else:
            raise ValueError('Received a task function that is not map or reduce.')

    def start(self):
        '''
        Starts up the master server. Useful for delegating the start to other logic,
        such as the nameserver.
        '''
        # Start the master task-routing loop.
        logger.info('Started master.')
        self._alive = True
        self._master_thread = spawn_thread(self._master_loop)

    def stop(self):
        '''
        Stops the master server. Useful for delegating the stop to other logic,
        such as the nameserver.
        '''
        self._alive = False
        if self._master_thread and self._master_thread.is_alive():
            kill_thread(self._master_thread, logger, name='master', timeout=10)
        if self._backup_thread and self._backup_thread.is_alive():
            kill_thread(self._backup_thread, logger, name='backup', timeout=10)
        logger.info('Stopped master.')


    # Helper methods.
    def _assign_task(self, tasks: TaskGroup, func: bytes) -> bool:
        '''
        Assign any pending task from the provided group to any idle follower.
        '''
        with self._followers_lock, self._map_tasks_lock, self._reduce_tasks_lock:
            if self._idle_followers:
                follower_addr = self._idle_followers.pop()
                if reachable(follower_addr):
                    with Proxy(follower_addr) as follower:
                        if tasks.pending:
                            task_id, data = tasks.pending.popitem()
                            tasks.assigned[task_id] = data
                            self._followers.add(follower_addr)
                            if func == self._map_function:
                                follower.map(task_id, data, func)
                            else:
                                follower.reduce(task_id, data, func)
                            logger.info(f'Dispatched task {task_id} to follower {follower_addr.host}.')
                            return True
        return False

    def _master_loop(self):
        '''
        Main loop of the master server.
        '''
        # Timeout 
        time.sleep(1)

        # Await nameserver, DHT and a request.
        while self._alive:
            try:
                if sf := self._get_serialized_functions():
                    logger.info('Found map-reduce request.')
                    self._map_function, self._reduce_function = sf
                    break
            except (Pyro4.errors.CommunicationError, Pyro4.errors.NamingError):
                time.sleep(REQUEST_TIMEOUT)

        # Startup begins.
        # self._start_time = time.time()

        # Check for backup.
        if self._alive:
            if backup := self._get_backup():
                # Load tasks, assume the assigned tasks have to be redone.
                self._map_tasks.load(backup[0])
                self._map_tasks.reset_assigned_to_pending()
                self._reduce_tasks.load(backup[1])
                self._reduce_tasks.reset_assigned_to_pending()

                # Load followers, assume all as idle.
                self._followers.clear()
                self._idle_followers = backup[2]

                # Load results.
                self._results.clear()
                self._results = backup[3]

                logger.info('Loaded backup from previous master.')
            else:
                # Split the input data into smaller chunks, which will be mapped.
                self._map_tasks.reset()
                self._reduce_tasks.reset()
                self._map_tasks.pending = self._get_request_data()

                logger.info('No backup found. Started from scratch.')

        # Start backing up data.
        if self._alive:
            self._backup_thread = spawn_thread(self._backup_loop)

        # Await all map tasks.
        if self._alive:
            logger.info('Started map tasks.')
            while self._alive and self._map_tasks.any:
                self._assign_task(self._map_tasks, self._map_function)
                time.sleep(REQUEST_TIMEOUT)

        # Await all reduce tasks.
        if self._alive:
            logger.info('Started reduce tasks')
            while self._alive and self._reduce_tasks.any:
                self._assign_task(self._reduce_tasks, self._reduce_function)
                time.sleep(REQUEST_TIMEOUT)
        
        # Post results to DHT and notify the request if finished.
        if self._alive:
            logger.info('Committing final results to DHT.')
            with self._dht_service as dht:
                dht.insert(RESULTS_KEY, self._results)

            # Notify results to request handler.
            with Pyro4.locateNS() as ns:
                with Proxy(ns.lookup(RQ_HANDLER_NAME)) as rqh:
                    rqh.notify_results()
    
    def _backup_loop(self):
        '''
        Main loop of the master server periodic backup task.
        '''
        logger.info('Started backup thread.')
        while self._alive:
            try:
                # Backup the current state. Lock up all threads to prevent interference.
                with ( self._followers_lock, self._results_lock,
                       self._map_tasks_lock, self._reduce_tasks_lock,
                       self._dht_service as dht ):
                    dht.insert(MASTER_BACKUP_KEY, (self._map_tasks.dump(),
                                                   self._reduce_tasks.dump(),
                                                   self._followers | self._idle_followers,
                                                   self._results))
            except Pyro4.errors.CommunicationError:
                logger.info("Couldn't backup data.") 
            time.sleep(MASTER_BACKUP_INTERVAL)
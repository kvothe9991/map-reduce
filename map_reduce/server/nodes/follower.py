from threading import Lock, Thread
import Pyro4
import Pyro4.errors
import logging
from typing import Any, Callable, List
from Pyro4 import Proxy, URI
from map_reduce.server.configs import MASTER_NAME

from map_reduce.server.logger import get_logger
from map_reduce.server.utils import ( kill_thread, spawn_thread )

logger = get_logger('flwr')

@Pyro4.expose
class Follower:
    '''
    Prime follower server for tasking.
    '''
    def __init__(self, address: URI):
        self._address = address
        self._task_id = None
        self._task_data = None
        self._task_type = None
        self._task_result = None
        self._task_function = None
        
        self._task_lock = Lock()
        self._task_thread: Thread = None
        self._announce_thread: Thread = spawn_thread(self._announce_to_master_loop)
        
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': address.host})


    # Exposed RPCs.
    @Pyro4.oneway
    def map(self, task_id: str, task_chunk: list[Any], func):
        ''' Map function over data chunk. '''
        logger.info(f'Received map chunk {task_id!r} of size {len(task_chunk)}')
        self._acknowledge_task(task_id, task_chunk, func, 'map')
    
    @Pyro4.oneway
    def reduce(self, task_id: str, task_group: list[Any], func):
        ''' Apply reduce function on data group. '''
        logger.info(f'Received reduce task {task_id!r}')
        self._acknowledge_task(task_id, task_group, func, 'reduce')


    # Helper methods.
    def _acknowledge_task(self, task_id, task_data, func, ttype):
        ''' Internally acknowledge the map/reduce task. '''
        # Stop doing previous task.
        if self._task_lock.locked():
            self._task_lock.release()
        if self._task_thread:
            kill_thread(self._task_thread)

        # Restart task.
        with self._task_lock:
            self._task_type = ttype
            self._task_id = task_id
            self._task_data = task_data
            self._task_function = func
            self._task_result = None
        
        # Do task on thread.
        self._task_thread = spawn_thread(self._do_task_and_report_results)

    def _do_task_and_report_results(self):
        ''' Report the results to master. Not to be used directly on main thread. '''
        assert self._task_type in ['map', 'reduce'], "Request type must be 'map' or 'reduce'."

        with self._task_lock:
            if self._task_type == 'map':
                self._task_result = []
                for shard in self._task_data:
                    partial = self._task_function(self._task_id, shard)
                    if not hasattr(partial, '__iter__'):
                        raise ValueError('Map function return type must be at least iterable.')
                    self._task_result.extend(partial)
            else:
                self._task_result = self._task_function(self._task_id, self._task_data)
            logger.info(f'Completed {self._task_type} task {self._task_id!r}.')
            if self._task_result is not None:
                with Pyro4.locateNS() as ns:
                    with Proxy(ns.lookup(MASTER_NAME)) as master:
                        master.report_task(self._address,
                                           self._task_id,
                                           self._task_function,
                                           self._task_result)
            else:
                logger.error('Task errored, results were None.')

    def _announce_to_master_loop(self):
        '''
        Loop which awaits for a master to appear before announcing self as worker.
        '''
        while True:
            try:
                with Pyro4.locateNS() as ns:
                    with Proxy(ns.lookup(MASTER_NAME)) as master:
                        master.subscribe(self._address)
                        break
            except (Pyro4.errors.CommunicationError, Pyro4.errors.NamingError):
                pass
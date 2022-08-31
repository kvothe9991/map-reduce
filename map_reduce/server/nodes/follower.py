from threading import Lock, Thread
import Pyro4
import logging
from typing import Any, Callable, List
from Pyro4 import Proxy, URI
from map_reduce.server.configs import MASTER_NAME

from map_reduce.server.logger import get_logger
from map_reduce.server.utils import ( kill_thread, spawn_thread, deserialize_function )

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
        self._task_result = None
        self._task_function = None
        
        self._task_lock = Lock()
        self._task_thread: Thread = None
        
        global logger
        logger = logging.LoggerAdapter(logger, {'address': address})

    @Pyro4.oneway
    def do(self, task_id: str, task_data: Any, func: bytes):
        ''' Do the map or reduce task. '''
        # Stop doing previous task.
        self._task_lock.release()
        if self._task_thread:
            kill_thread(self._task_thread)

        # Restart task.
        with self._task_lock:
            self._task_id = task_id
            self._task_data = task_data
            self._task_function = func
            self._task_result = None
        
        self._do_task_on_thread()
    

    # Helper methods.
    def _do_task_on_thread(self):
        ''' Do the task on a parallel thread. '''
        self._task_thread = spawn_thread(self._do_task_and_report_results)


    def _do_task_and_report_results(self):
        ''' Report the results to master. Not to be used directly on main thread. '''
        with self._task_lock:
            func = deserialize_function(self._task_function)
            self._task_result = func(self._task_data)
            if self._task_result is not None:
                with Pyro4.locateNS() as ns, ns.lookup(MASTER_NAME) as master:
                    master.report_task(self._address,
                                       self._task_id,
                                       self._task_function,
                                       self._task_result)
            else:
                logger.error('Task errored, results were None.')

import logging, time
from typing import Union
from threading import Thread

import Pyro4
import Pyro4.errors
import Pyro4.naming
from Pyro4 import URI, Proxy
from Pyro4.naming import NameServerDaemon, BroadcastServer
from map_reduce.server.configs import NS_CONTEST_INTERVAL

from map_reduce.server.utils import alive, reachable, id
from map_reduce.server.logger import get_logger


logger = get_logger('ns')

class NameServer:
    '''
    A wrapper around a Pyro nameserver connection.

    Searches for a nameserver to bind to in the local network, otherwise starts up
    a nameserver thread from this machine. Multiple, simultaneous nameservers in the
    network are contested by hash/id precedence.

    TODO:
    [ ] Nameserver persistance (perhaps on DHT?).
    [ ] Online/offline API as an external delegation alternative.
        [ ] Perhaps this entails a serious feature extrapolation from this class to another.
    [ ] After previous item:
        [ ] Implement master nodes on top of nameserver.
    '''
    def __init__(self, ip, port=8008):
        ''' Instantiates a new nameserver, then starts it up. '''
        # Self attributes.
        self._ip = ip
        self._port = port
        self._alive = False
        self._uri: URI = None
        self._ns_daemon: NameServerDaemon = None
        self._ns_broadcast: BroadcastServer = None
        self._ns_thread: Thread = None
        self._broadcast_thread: Thread = None
        self._stabilization_thread: Thread = None

        # Logger config.
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': ip})
        
    def __str__(self):
        status = 'remote' if self.is_remote else 'local'
        return f'{self.__class__.__name__}({status})@[{self._uri}]'
    
    def __repr__(self):
        return str(self)
    
    def __enter__(self):
        return self
    
    def __exit__(self, *_):
        self.stop()
    
    @property
    def is_remote(self) -> bool:
        return self._ip != self._uri.host
    
    @property
    def is_local(self) -> bool:
        return not self.is_remote
    
    @property
    def servers(self) -> tuple[Pyro4.Daemon, Pyro4.naming.BroadcastServer]:
        return self._ns_daemon, self._ns_broadcast
    
    def _locate_nameserver(self) -> Union[URI, None]:
        '''
        Attempts to locate a remote nameserver. Returns its URI if found.
        '''
        try:
            with Pyro4.locateNS() as ns:
                return ns._pyroUri
        except Pyro4.errors.NamingError:
            return None
    
    def _start_local_nameserver(self):
        '''
        Starts the local nameserver on a parallel thread.
        '''
        logger.info(f'Local nameserver started.')
        self._uri, self._ns_daemon, self._ns_broadcast = Pyro4.naming.startNS(self._ip,
                                                                              self._port)
        self._ns_thread = Thread(target=self._ns_daemon.requestLoop)
        self._ns_thread.setDaemon(True)
        self._ns_thread.start()

        self._broadcast_thread = Thread(target=self._ns_broadcast.runInThread)
        self._broadcast_thread.setDaemon(True)
        self._broadcast_thread.start()
    
    def _stop_local_nameserver(self):
        '''
        Stops the local nameserver (killing its thread and daemons).
        '''
        self._ns_daemon.shutdown()
        self._ns_daemon = None
        if self._ns_thread.join(0.1) and self._ns_thread.is_alive():
            logger.error("Couldn't kill nameserver thread.")
        
        self._ns_broadcast.close()
        self._ns_broadcast = None
        if self._broadcast_thread.join(0.1) and self._broadcast_thread.is_alive():
            logger.error("Couldn't kill broadcast thread.")
        
        logger.info(f'Local nameserver stopped.')
    
    def refresh_nameserver(self):
        '''
        Stabilizes the nameserver reference, either when a remote nameserver dies, or
        when a remote nameserver appears that should be leader instead of the local one.
        Note that this can kill the nameserver daemon and its broadcast server so
        it should be used carefully.

        This method is called periodically by the start() method, but can be used
        externally when sequential checks are needed instead of a parallel thread.
        '''
        if self.is_remote:
            if not reachable(self._uri):
                logger.warning(f'Remote nameserver @{self._uri.host} is not reachable.')
                if (new_uri := self._locate_nameserver()) is not None:
                    logger.info(f'Found new nameserver @{new_uri.host}.')
                    self._uri = new_uri
                else:
                    logger.info(f'No new nameserver found. Announcing self.')
                    self._start_local_nameserver()
        else:
            if (new_uri := self._locate_nameserver()) is not None and new_uri != self._uri:
                logger.info(f'Found contesting nameserver @{new_uri.host}.')
                if id(self._uri) >= id(new_uri):
                    logger.info(f'I no longer am the nameserver, long live {new_uri.host}.')
                    self._uri = new_uri
                    self._stop_local_nameserver()
                else:
                    logger.info(f'I am still the nameserver.')

    def _nameserver_loop(self):
        self._alive = True
        while self._alive:
            self.refresh_nameserver()
            time.sleep(NS_CONTEST_INTERVAL)

    def start(self):
        '''
        Starts the nameserver wrapper. Spawns a checker thread that assures only one
        nameserver is alive in the network at any given moment.
        '''
        self._start_local_nameserver()

        logger.info('Nameserver stabilization loop starting...')
        self._alive = True
        self._stabilization_thread = Thread(target=self._nameserver_loop)
        self._stabilization_thread.setDaemon(True)
        self._stabilization_thread.start()
    
    def stop(self):
        '''
        Stops the nameserver wrapper and the created threads.
        '''
        self._alive = False
        if self._stabilization_thread.join(timeout=0.5) and self._stabilization_thread.is_alive():
            logger.error("Nameserver stabilization thread won't die after join.")
        if self.is_local:
            self._stop_local_nameserver()

    def bind(self) -> Pyro4.naming.NameServer:
        '''
        Returns a proxy bound to the nameserver, local or remote. Should be used
        with a context manager.
        '''
        assert self._alive, 'NameServer instance must be running before binding.'
        return Proxy(self._uri)
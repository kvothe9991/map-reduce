import time
import logging
from threading import Thread
from typing import Any, Callable, Union

import Pyro4
import Pyro4.errors
import Pyro4.naming
from Pyro4 import URI, Proxy
from Pyro4.naming import NameServerDaemon, BroadcastServer
from map_reduce.server.configs import ( DHT_SERVICE_NAME, NS_BACKUP_KEY,
                                        NS_BACKUP_INTERVAL, NS_CONTEST_INTERVAL )
from map_reduce.server.utils import ( reachable, id, kill_thread, spawn_thread,
                                      daemon_address )
from map_reduce.server.logger import get_logger


logger = get_logger('ns')

class NameServer:
    '''
    A wrapper around a Pyro nameserver connection.

    Searches for a nameserver to bind to in the local network, otherwise starts up
    a nameserver thread from this machine. Multiple, simultaneous nameservers in the
    network are contested by hash/id precedence.

    TODO:
    [ ] Implement master nodes on top of nameserver.
    '''
    def __init__(self, ip, port):
        ''' Instantiates a new nameserver with the given ip/port combination. '''
        # Self attributes.
        self._ip = ip
        self._port = port
        self._alive = False
        self._uri: URI = None
        self._dht_address: URI = daemon_address(DHT_SERVICE_NAME)

        # Internal servers.
        self._ns_daemon: NameServerDaemon = None
        self._ns_broadcast: BroadcastServer = None

        # Callback delegation.
        self._on_startup_events = {}
        self._on_shutdown_events = {}

        # Threads.
        self._ns_thread: Thread = None
        self._broadcast_thread: Thread = None
        self._ns_backup_thread: Thread = None
        self._stabilization_thread: Thread = None

        # Logger config.
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': ip})
        
    def __str__(self):
        status = 'remote' if self.is_remote else 'local'
        return f'{self.__class__.__name__}({status})@[{self._uri}]'
    
    def __repr__(self):
        return str(self)
    
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
        self._ns_thread = spawn_thread(target=self._ns_daemon.requestLoop)
        self._broadcast_thread = spawn_thread(target=self._ns_broadcast.runInThread)

        # Read backup data from DHT if available.
        logger.debug(f'Attempting to read nameserver data from DHT.')
        if reachable(self._dht_address):
            logger.debug(f'DHT is reachable.')
            with Proxy(self._dht_address) as dht:
                if ns_data := dht.lookup(NS_BACKUP_KEY):
                    if isinstance(ns_data, dict):
                        for name, addr in ns_data.items():
                            try:
                                self._ns_daemon.nameserver.register(name, addr, safe=True)
                            except Pyro4.errors.NamingError as e:
                                logger.debug(f'{e}')
                                continue
                        logger.info(f'Staged data from DHT loaded.')
                    else:
                        logger.error(f'Error extracting backup from DHT: backup={ns_data}.')

        # Callback delegation.
        for addr, callback in self._on_startup_events.items():
            logger.info(f'Calling startup on {addr.object}')
            self._ns_daemon.nameserver.register(addr.object, addr)
            callback()

        # Start backup thread.
        def nameserver_backup_loop():
            while True:
                self._backup_nameserver()
                time.sleep(NS_BACKUP_INTERVAL)

        self._ns_backup_thread = spawn_thread(target=nameserver_backup_loop)

    def _backup_nameserver(self):
        '''
        Posts a backup of the nameserver data to the DHT if available.
        '''
        if reachable(self._dht_address):
            with Proxy(self._dht_address) as dht:
                if self._ns_daemon is not None and self._ns_daemon.nameserver is not None:
                    dht.insert(NS_BACKUP_KEY, self._ns_daemon.nameserver.list())
                    logger.debug(f'Nameserver data backed up to DHT.')
                # TODO: Maybe integrate delegate backups as well.
        

    def _stop_local_nameserver(self, forward_to: URI = None):
        '''
        Stops the local nameserver (killing its thread and daemons).
        '''
        if forward_to is not None:
            # Forward registered objects to new nameserver.
            new_ns = forward_to
            try:
                sender = self._ns_daemon.nameserver
                with Proxy(new_ns) as receiver:
                    for name, addr in sender.list().items():
                        try:
                            receiver.register(name, addr, safe=True)
                        except Pyro4.errors.NamingError as e:
                            logger.info(f'{e}')
                            continue
            except Exception as e:
                logger.error(f"Error forwarding registry to nameserver {new_ns.host!r}: {e}")
            
            # Overwrite the binding address.
            self._uri = new_ns

        # Callback delegation.
        for addr, callback in self._on_shutdown_events.items():
            logger.info(f'Calling shutdown on {addr.object}')
            callback()

        # Shutdown the backup task.
        kill_thread(self._ns_backup_thread, logger)

        # Shutdown the nameserver.
        self._ns_daemon.shutdown()
        self._ns_daemon = None
        kill_thread(self._ns_thread, logger)
        
        # Shutdown the broadcast utility server.
        self._ns_broadcast.close()
        self._ns_broadcast = None
        kill_thread(self._broadcast_thread, logger)
        
        logger.info(f'Local nameserver stopped.')
    
    def _refresh_nameserver(self):
        '''
        Stabilizes the nameserver reference, either when a remote nameserver dies, or
        when a remote nameserver appears that should be leader instead of the local one.
        Note that this can kill the nameserver daemon and its broadcast server so
        it should be used carefully.

        This method is called periodically by the start() method, but can be used
        externally when sequential checks are needed instead of a parallel thread.
        '''
        curr_ns = self._uri
        new_ns = self._locate_nameserver()

        if self.is_remote:
            if not reachable(curr_ns):
                logger.warning(f'Remote nameserver @{curr_ns.host} is not reachable.')
                if new_ns is not None:
                    logger.info(f'Found new nameserver @{new_ns.host}.')
                    self._uri = new_ns
                else:
                    logger.info(f'No new nameserver found. Announcing self.')
                    self._start_local_nameserver()
        else:
            if new_ns is not None and new_ns != curr_ns:
                logger.info(f'Found contesting nameserver @{new_ns.host}.')
                if id(curr_ns) >= id(new_ns):
                    logger.info(f'I no longer am the nameserver, long live {new_ns.host}.')
                    self._stop_local_nameserver(forward_to=new_ns)
                else:
                    logger.info(f'I am still the nameserver.')

    # Main API.
    def start(self):
        '''
        Starts the nameserver wrapper. Spawns a checker thread that assures only one
        nameserver is alive in the network at any given moment.
        '''
        self._start_local_nameserver()

        def nameserver_loop():
            self._alive = True
            while self._alive:
                self._refresh_nameserver()
                time.sleep(NS_CONTEST_INTERVAL)

        logger.info('Nameserver checker loop starting...')
        self._stabilization_thread = spawn_thread(target=nameserver_loop)
    
    def stop(self):
        ''' Stops the nameserver wrapper and the created threads. '''
        self._alive = False
        kill_thread(self._stabilization_thread, logger)
        if self.is_local:
            self._stop_local_nameserver()

    def delegate(self, address: URI, on_startup: Callable, on_shutdown: Callable):
        ''' Subscribes delegate callbacks on local nameserver startup/shutdown. '''
        self._on_startup_events[address] = on_startup
        self._on_shutdown_events[address] = on_shutdown

    def bind(self) -> Pyro4.naming.NameServer:
        '''
        Returns a proxy bound to the nameserver, local or remote. Should be used
        with a context manager.
        '''
        assert self._alive, 'NameServer instance must be started before binding.'
        return Proxy(self._uri)
import logging, time
from typing import Union
from threading import Thread

import Pyro4
import Pyro4.errors
import Pyro4.naming
from Pyro4 import URI, Proxy
from Pyro4.naming import NameServerDaemon, BroadcastServer

from server.utils import alive, reachable, id
from server.nameserver.logger import logger


class NameServer:
    '''
    A wrapper around a Pyro nameserver connection.

    Searches for a nameserver to bind to in the local network, otherwise starts up
    a nameserver thread from this machine. Multiple, simultaneous nameservers in the
    network are contested by hash/id precedence.

    TODO:
    - Class-wide cleanup and documentation.
    - Nameserver persistance (perhaps on DHT?).
    - Online/offline API as an external delegation alternative.
        + Perhaps this entails a serious feature extrapolation from this class to another.
    - After previous item:
        + Implement master nodes on top of nameserver.
    '''
    
    def __init__(self, ip: str, port = 8008):
        ''' Initialize the nameserver wrapper, then start it up. '''

        # Logger config.
        global logger
        logger = logging.LoggerAdapter(logger, {'URI': ip})

        # Self attributes.
        self._ip = ip
        self._port = port
        self._ns_thread: Thread = None
        self._ns_daemon: NameServerDaemon = None
        self._ns_broadcast: BroadcastServer = None
        self._keep_checking_loop = True

        # Initial search for nameserver.
        self.uri: URI = None
        if uri := self._locate_NS():
            logger.info(f'Found existing nameserver {uri}.')
            self.uri = uri
        else:
            self.start()

    def __str__(self):
        status = 'remote' if self.is_remote else 'local'
        return f'{self.__class__.__name__}({status})@[{self.uri}]'
    
    def __repr__(self):
        return str(self)
    
    @property
    def is_remote(self) -> bool:
        return self._ns_thread and not self._ns_thread.is_alive()

    def _locate_NS(self) -> Union[URI, None]:
        ''' Attempts to locate a remote nameserver. Returns its URI if found. '''
        try:
            with Pyro4.locateNS() as ns:
                return ns._pyroUri
        except Pyro4.errors.NamingError:
            return None
    
    def start(self):
        ''' Starts the local nameserver. '''
        self.uri, self._ns_daemon, self._ns_broadcast = Pyro4.naming.startNS(self._ip,
                                                                             self._port)
        self._ns_daemon.combine(self._ns_broadcast)
        
        self._ns_thread = Thread(target=self._ns_daemon.requestLoop)
        self._ns_thread.setDaemon(True)
        self._ns_thread.start()
        logger.info(f'Local nameserver started at {self.uri}.')

    def stop(self):
        ''' Stops the local nameserver. '''
        self._ns_daemon.shutdown()
        if self._ns_thread.join(0.1) and self._ns_thread.is_alive():
            logger.error('Nameserver thread did not stop after daemon shutdown order.')
        else:
            self._ns_thread = None
            self._ns_daemon = self._ns_broadcast = None
            logger.info(f'Local nameserver stopped.')
    
    def check_NS(self):
        ''' Checks for a nameserver in the network. Announces self otherwise. '''
        if self.is_remote:
            if reachable(self.uri):
                logger.debug(f'Remote nameserver {self.uri} is reachable as expected.')
            else:
                logger.warning(f'Remote nameserver {self.uri} is not reachable.')
                if new_uri := self._locate_NS():
                    logger.info(f'Found new nameserver {new_uri}.')
                    self.uri = new_uri
                else:
                    logger.warning(f'No new nameserver found. Announcing self.')
                    self.start()
        else:
            if (new_uri := self._locate_NS()) is not None and new_uri != self.uri:
                logger.info(f'Found contesting nameserver {new_uri}.')
                if id(self.uri) < id(new_uri):
                    logger.info(f'I am still the nameserver.')
                else:
                    logger.info(f'I no longer am the nameserver, long live {new_uri}.')
                    self.uri = new_uri
                    self.stop()

    def check_NS_loop(self):
        '''
        Higher level loop to run in a daemonized thread. Checks for a nameserver
        and spawns one if there is none running in the network.  The
        `self._keep_checking_loop` attribute is used to stop the loop externally.
        '''
        logger.info('Starting nameserver check loop.')
        self._keep_checking_loop = True
        while self._keep_checking_loop:
            time.sleep(0.1)
            self.check_NS()
        self.stop()
    
    def kill_check_NS_loop(self):
        ''' Kills high-level running loop `check_NS_loop` for when it is thread-bound. '''
        self._keep_checking_loop = False

    def bind(self) -> Pyro4.naming.NameServer:
        ''' Returns a proxy bound to the nameserver, local or remote. Should be used
        with a context manager. '''
        return Proxy(self.uri)
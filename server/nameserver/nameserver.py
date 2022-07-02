import logging
import Pyro4
import Pyro4.errors
import Pyro4.naming
from typing import Union
from Pyro4 import URI, Proxy
from Pyro4.naming import NameServerDaemon, BroadcastServer

from server.utils import alive, reachable, id
from server.nameserver.logger import logger


class BoundNameServer:
    ''' A wrapper around a Pyro nameserver connection. '''
    def __init__(self, ip: str, port: int = 8008):
        self._ip = ip
        self._port = port
        self._quickname = f'PYRO:(ns.binder)@{self._ip}:{self._port}'
        logger = logging.LoggerAdapter(logger, {'URI': self._quickname})

        self.URI = None
        self._ns_thread = None
        self._ns_daemon = None
        self._ns_broadcast = None
        self._remote = False

        # Initial search for nameserver.
        if uri := self.locate_NS():
            logger.info(f'Found existing nameserver {uri}.')
            self.URI = uri
            self._remote = True
        else:
            self.start()

    def __str__(self):
        status = 'remote' if self._remote else 'local'
        return f'BoundNameServer<{status}>@[{self.URI}]'
    
    def __repr__(self):
        return str(self)

    def locate_NS(self) -> Union[URI, None]:
        ''' Attempts to locate a remote nameserver. Returns its URI if found. '''
        try:
            with Pyro4.locateNS() as ns:
                return ns._pyroUri
        except Pyro4.errors.NamingError:
            return None
    
    def start(self):
        ''' Starts the local nameserver. '''
        self.URI, self._ns_daemon, self._ns_broadcast = Pyro4.naming.startNS(
            host=self._ip, port=self._port)
        self._ns_daemon.combine(self._ns_broadcast)
        self._remote = False
        logger.info(f'Local nameserver started at {self.URI}.')

    def stop(self):
        ''' Stops the local nameserver. '''
        self._ns_daemon.shutdown()
        self._remote = True
        self.info(f'Local nameserver stopped.')
    
    def check_NS(self) -> tuple[bool, URI]:
        ''' Checks for a nameserver in the network. Announces self otherwise. '''
        if self._remote:
            if reachable(self.URI):
                logger.info(f'Remote nameserver {self.URI} is reachable as expected.')
            else:
                logger.warning(f'Remote nameserver {self.URI} is not reachable.')
                if new_uri := self.locate_NS():
                    logger.info(f'Found new nameserver {new_uri}.')
                    self.URI = new_uri
                else:
                    logger.warning(f'No new nameserver found. Announcing self.')
                    self.start()
        else:
            if (new_uri := self.locate_NS()) is not None and new_uri != self.URI:
                logger.info(f'Found contesting nameserver {new_uri}.')
                if id(self.URI) < id(new_uri):
                    logger.info(f'I am still the nameserver.')
                else:
                    logger.info(f'I no longer am the nameserver, long live {new_uri}.')
                    self.URI = new_uri
                    self.stop()
        return (self._remote, self.URI)

    def bind(self) -> Proxy:
        ''' Returns a proxy bound to the nameserver, local or remote. Should be used
        with a context manager. '''
        return Proxy(self.URI)

    def local_servers(self) -> tuple[URI, NameServerDaemon, BroadcastServer]:
        ''' Returns a tuple of URI and both local raised servers by Pyro. '''
        return self.URI, self._ns_daemon, self._ns_broadcast
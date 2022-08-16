import logging

import Pyro4
import Pyro4.errors
from Pyro4 import URI, Proxy

from map_reduce.server.logger import get_logger
from map_reduce.server.utils import reachable, unpack


logger = get_logger('dht:s')

@Pyro4.expose
@Pyro4.behavior('single')
class ChordService:
    def __init__(self, address: URI, node_address: URI):
        '''
        Instances a CHORD data layer service. Its `_node_address` attribute contains
        the provided Pyro4 URI belonging to the underlying node structure, meanwhile
        the `address` attribute contains the actual accessor to this class, with an
        appended 'service' on the name object.
        '''
        self._address = address
        self._node_address = node_address
        self._items = {}

        # Logging setup.
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': address.host})
    
    def __repr__(self):
        addr = self._address
        count = len(self._items)
        return f'{self.__class__.__name__}<{addr=}, {count=}>'
    
    def __str__(self):
        return repr(self)
    
    @property
    def address(self) -> URI:
        return self._address
    
    @property
    def items(self) -> dict:
        return self._items
    
    @property
    def node(self) -> Proxy:
        return Proxy(self._node_address)
    
    @Pyro4.oneway
    def insert(self, key, value, append=False, safe=False):
        ''' Inserts a new key-value pair into the partial hash table. '''

        if key is None or value is None:
            logger.error(f'Cannot insert a None value or key.')
            return
        
        key_id = key if isinstance(key, int) else id(key)
        addr = self._find_successor(key_id)
        if addr:
            logger.debug(f'Inserting key {key!r} into {addr.host}.')
            if addr == self._node_address:
                if not safe or key_id not in self._items:
                    self._items[key] = value
                # TODO: Replication.
                logger.info(f'Inserted {key!r}:{value!r}.')
            elif reachable(addr):
                serv_addr = service_address(addr)
                logger.info(f'Insertion redirected to {serv_addr}.')
                with Proxy(serv_addr) as other:
                    other.insert(key, value, append, safe)
            else:
                logger.info(f'Tried to store {key!r}:{value!r} in node {addr.host} but it was unreachable.')
    
    def lookup(self, key, default=None):
        ''' Searches the partial hash table for a key. Exception-safe as it returns
        `default` if the key is not found. '''

        if key is None:
            logger.error('Provided key for lookup must not be None.')
            return
        
        key_id = key if isinstance(key, int) else id(key)
        addr = self._find_successor(key_id)
        if addr:
            if addr == self._node_address:
                return self._items.get(key, default)
            elif reachable(addr):
                serv_addr = service_address(addr)
                logger.info(f'Lookup redirected to {serv_addr}.')
                with Proxy(serv_addr) as other:
                    return other.lookup(key, default)
            else:
                logger.info(f'Tried to lookup {key!r} from node {addr.host} but it was unreachable.')

    @Pyro4.oneway
    def remove(self, key):
        ''' Searches the partial hash table for a key and its value and removes them. '''
        if key is None:
            logger.error('Provided key for lookup must not be None.')
            return
        
        key_id = key if isinstance(key, int) else id(key)
        addr = self._find_successor(key_id)
        if addr:
            if addr == self._node_address:
                if self._items.pop(key, None) is None:
                    logger.error(f'Key {key!r} not found.')
            elif reachable(addr):
                serv_addr = service_address(addr)
                logger.info(f'Removal redirected to {serv_addr}.')
                with Proxy(serv_addr) as other:
                    return other.remove(key)
            else:
                logger.info(f'Tried to remove {key!r} from node {addr.host} but it was unreachable.')

    def _find_successor(self, key_id):
        ''' Proxy exception-safe call for `self.node.find_successor()`. '''
        try:
            with Proxy(self._node_address) as node:
                owner = node.find_successor(key_id)
                logger.info(f'Owner of key is {owner}.')
                return owner
        except Pyro4.errors.CommunicationError as e:
            logger.error(f"Error accessing own node: '{type(e).__name__}: {e}'")


def service_address(uri: URI) -> URI:
    name, host, port = unpack(uri)
    return URI(f'PYRO:{name}.service@{host}:{port}')
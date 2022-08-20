import logging

import threading
from threading import Lock
from typing import TypeVar

import Pyro4
import Pyro4.errors
from Pyro4 import URI, Proxy

from map_reduce.server.dht.locked_object import LockedObject
from map_reduce.server.logger import get_logger
from map_reduce.server.utils import reachable, service_address, id


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

        TODO: Mutual exclusion on self._items requests.
        '''
        self._address = address
        self._node_address = node_address
        self._items = LockedObject({})

        # Logging setup.
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': address.host})
    
    def __repr__(self):
        addr = self._address
        count = len(self.items)
        return f'{self.__class__.__name__}<{addr=}, {count=}>'
    
    def __str__(self):
        return repr(self)
    
    @property
    def address(self) -> URI:
        return self._address
    
    @property
    def items(self) -> dict:
        return self._items.obj
    
    @property
    def node(self) -> Proxy:
        return Proxy(self._node_address)

    # Exposed RPCs.
    @Pyro4.oneway
    def insert(self, key, value, append=False, safe=False):
        '''
        Inserts a new key-value pair into the partial hash table.
        '''
        self._assert_key(key)
        self._assert_value(key, value)
        
        with self._items as items:
            key_id = self._obtain_key_id(key)
            addr = self._find_successor(key, key_id)
            if addr:
                logger.debug(f'Inserting key {key!r} into {addr.host}.')
                if addr == self._node_address:
                    if not safe or key not in items:
                        items[key] = value
                    logger.info(f'Inserted {key!r}:{value!r}.')
                    # TODO: Replication.
                elif reachable(addr):
                    serv_addr = service_address(addr)
                    logger.info(f'Insertion of {key!r} redirected to {serv_addr}.')
                    with Proxy(serv_addr) as other:
                        other.insert(key, value, append, safe)
                else:
                    logger.info(f'Tried to store {key!r}:{value!r} in node {addr.host} but it was unreachable.')
            else:
                logger.error(f'Successor not found, was None.')


    def lookup(self, key, default=None):
        '''
        Searches the partial hash table for a key. Exception-safe as it returns
        `default` if the key is not found.
        '''
        self._assert_key(key)

        key_id = self._obtain_key_id(key)
        addr = self._find_successor(key, key_id)

        with self._items as items:
            if addr:
                if addr == self._node_address:
                    value = items.get(key, default)
                    logger.info(f'Found {key!r}:{value!r} in local table.')
                elif reachable(addr):
                    serv_addr = service_address(addr)
                    logger.info(f'Lookup of {key!r} redirected to {serv_addr}.')
                    with Proxy(serv_addr) as other:
                        value = other.lookup(key, default)
                else:
                    logger.info(f'Tried to lookup {key!r} from node {addr.host} but it was unreachable.')
            else:
                logger.error(f'Successor not found, was None.')
            
            return value

    @Pyro4.oneway
    def remove(self, key):
        '''
        Searches the partial hash table for a key and its value and removes them.
        '''
        self._assert_key(key)

        key_id = self._obtain_key_id(key)
        addr = self._find_successor(key, key_id)

        with self._items as items:
            if addr:
                if addr == self._node_address:
                    if items.pop(key, None) is None:
                        logger.error(f'Key {key!r} not found.')
                elif reachable(addr):
                    serv_addr = service_address(addr)
                    logger.info(f'Removal of {key!r} redirected to {serv_addr}.')
                    with Proxy(serv_addr) as other:
                        other.remove(key)
                else:
                    logger.info(f'Tried to remove {key!r} from node {addr.host} but it was unreachable.')
            else:
                logger.error(f'Successor not found, was None.')

    @Pyro4.oneway
    def refresh(self):
        ''' Refreshes the local partial hash table. '''
        logger.info('Redistributing local partial hash table through ring.')
        with self._items as items:
            data = items
            items.clear()
        for k,v in data.items():
            self.insert(k, v, safe=True)

    # Helper methods.
    def _assert_key(self, key):
        ''' Asserts that the provided key is not None. Raises ValueError exceptions.'''
        if key is None:
            raise ValueError('Provided key must not be None.')

    def _assert_value(self, key, value):
        ''' Asserts that the provided value is not None. Raises ValueError exceptions.'''
        if value is None:
            raise ValueError(f'Provided value for key {key!r} must not be None.')
    
    def _obtain_key_id(self, key):
        ''' Obtains the SHA-1 hash id for a given key. '''
        key_id = id(key)
        logger.info(f'Hashed key {key!r} as {key_id}.')
        return key_id

    def _find_successor(self, key, key_id):
        ''' Proxy exception-safe call for `self.node.find_successor()`. '''
        try:
            with Proxy(self._node_address) as node:
                owner = node.find_successor(key_id)
                logger.info(f'Owner of {key!r} is {owner.host}.')
                return owner
        except Pyro4.errors.CommunicationError as e:
            logger.error(f"Error accessing own node: '{type(e).__name__}: {e}'")

    # Debug methods.
    def debug_dump_items(self):
        ''' Prints the local partial hash table. '''
        print(self.items)
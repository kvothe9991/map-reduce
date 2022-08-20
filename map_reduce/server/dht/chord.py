import time
import logging
import threading
from typing import Any, Union

import Pyro4
import Pyro4.errors
from Pyro4 import URI, Proxy
from Pyro4.errors import CommunicationError

from map_reduce.server.logger import get_logger
from map_reduce.server.configs import ( DHT_FINGER_TABLE_SIZE, DHT_STABILIZATION_INTERVAL,
                                        DHT_NAME, DHT_RECHECK_INTERVAL )
from map_reduce.server.utils import ( id, in_arc, reachable, SHA1_BIT_COUNT, spawn_thread,
                                      service_address )


logger = get_logger('dht', extras=True)

@Pyro4.expose
@Pyro4.behavior('single')
class ChordNode:
    def __init__(self, address: URI):
        '''
        Instances a new CHORD node with identifier obtained from its IP address,
        it's `address` attribute is a Pyro4 URI: `PYRO:{domain}@{ip}:{port}`.

        The `successor_cache_size` argument defines the amount of successors accounted
        for to mantain robustness of CHORD's stability in case of several contigous
        nodes failing simultaneously.
        '''
        self._address = address # Pyro4 URI of this node.
        self._id = id(address)  # Numerical identifier of this node.
        self._predecessor = None        # Pyro4 URI of this node's predecessor.
        self._successor = self._address # Pyro4 URI of this node's successors.
        
        # Logging setup:
        global logger
        logger = logging.LoggerAdapter(logger, {'IP': self._address.host,
                                                'extra': f'self={self._id:.2e}'})

        # Finger table and its stabilization.
        self._finger_table = [None] * DHT_FINGER_TABLE_SIZE
        self._current_finger_ = 0   # Self updating counter for finger table.

        # Ring registration on nameserver.
        self._ring = None

        # Handle periodic calls.
        self._stabilize_thread = spawn_thread(target=self._handle_periodic_calls)

    def __repr__(self):
        addr = self._address
        count = len(self._items)
        id_ = f'{self._id:.2e}'
        return f'{self.__class__.__name__}<({count}), {addr=}, id={id_}>'

    def __str__(self):
        return repr(self)

    @property
    def _current_finger(self) -> int:
        ''' Cycles periodically between all `m` fingers of the table. '''
        self._current_finger_ += 1
        self._current_finger_ %= DHT_FINGER_TABLE_SIZE
        return self._current_finger_

    # Exposed attributes:
    @property
    def id(self) -> int:
        return self._id

    @property
    def address(self) -> URI:
        return self._address

    @property
    def successor(self, k=0) -> URI:
        return self._successor
    
    @successor.setter
    def successor(self, n: URI):
        ''' Setter for the successor attribute. '''
        self._successor = n

        # Logging.
        if n is not None:
            name = f'{n.host}[id={id(n):.1e}]'
            if self.address == n:
                name = 'self'
            elif self.address == self.predecessor:
                name = 'predecessor'
            logger.info(f'Successor set to {name}.')

    @property
    def predecessor(self) -> URI:
        return self._predecessor

    @predecessor.setter
    def predecessor(self, n: URI):
        ''' Setter for the predecessor attribute. '''
        self._predecessor = n

        # Logging.
        if n is not None:
            name = f'{n.host}[id={id(n):.1e}]'
            if self.address == n:
                name = 'self'
            elif self.address == self.successor:
                name = 'successor'
            logger.info(f'Predecessor set to {name}.')


    # Exposed RPCs:
    def find_successor(self, x: int) -> URI:
        ''' Return immediate successor of id `x`, in address form. '''
        
        # Trivial cases for the ring being one-node long.
        if self.successor is None or self.successor == self.address:
            return self.address
        
        # Search if `x` belongs to this node's domain, otherwise check the finger table.
        if in_arc(x, l=self.id, r=id(self.successor)):
            return self.successor
        elif (cp_addr :=  self.closest_preceding_node(x)) == self.address:
            return self.address
        else:
            # closest_preceding_node() guarantees that the returned node is alive.
            with Proxy(cp_addr) as n:
                return n.find_successor(x)

    def closest_preceding_node(self, x: int) -> URI:
        ''' Search the local finger table for the closest predecessor of id `x`,
        running the table in reverse order for convenient convergence to the furthest
        node from self available, which is returned in address form.
        
        For convenience, the closest preceding *alive* node is returned.'''
        
        # Iterate finger table in reverse order, skipping the last one.
        for f_addr in reversed(self._finger_table):
            if f_addr and in_arc(id(f_addr), l=self.id, r=x) and reachable(f_addr):
                return f_addr
        return self.address

    def join(self, addr: URI):
        ''' Join a CHORD ring containing node `n`. '''
        if reachable(addr):
            with Proxy(addr) as n:
                logger.info(f'Joined a DHT ring containing {addr}.')
                self._ring = addr
                self.predecessor = None
                self.successor = n.find_successor(self.id)
                with Proxy(service_address(self._address)) as service:
                    service.refresh()
        else:
            logger.error(f'Could not join the ring, node {addr}<id={id(addr):.2e}> unreachable.')

    def notify(self, n: URI):
        ''' Remote procedure call from node `n` announcing it might be this node's
        predecessor. Assumes `n` alive since it's the one who invoked this method. '''
        if (self.predecessor is None
        or not reachable(self.predecessor)
        or in_arc(id(n), l=id(self.predecessor), r=self.id)):
            self.predecessor = n
    

    # Periodic methods:
    def _stabilize(self):
        ''' Verify it's own immediate succesor, checking for new nodes that may have
        inserted themselves unannounced. This method is called periodically. '''
        
        if self.successor is None:
            logger.info(f'Successor is None.')
            raise NotImplementedError()
        if self.successor == self.address:
            if self.predecessor:
                self.successor = self.predecessor
            return
        
        try:
            with Proxy(self.successor) as s:
                x = s.predecessor
                if (x is not None
                and in_arc(id(x), l=self.id, r=id(self.successor))
                and reachable(x)):
                    self.successor = x
                s.notify(self.address)
        except Pyro4.errors.CommunicationError:
            logger.error(f'Could not reach successor {self.successor}.')
            self.successor = self.address

    def _fix_fingers(self):
        ''' Refresh finger table entries. This method is called periodically. '''
        i = self._current_finger
        n = (self.id + 2**i) % (2**SHA1_BIT_COUNT)
        self._finger_table[i] = self.find_successor(n)
        assert isinstance(self._finger_table[i], URI), 'finger table entry is not a URI object.'
    
    def _check_predecessor(self):
        ''' Check for predecessor failure. '''
        if self.predecessor is not None and not reachable(p := self.predecessor):
            logger.info(f'Predecessor {p.host}<id={id(p):.2e}> is unreachable.')
            self.predecessor = None

    def _check_ring_availability(self):
        '''
        Check periodically for the ring in the nameserver.
        '''
        with Pyro4.locateNS() as ns:
            try:
                ring = ns.lookup(DHT_NAME)
                if ring != self.address and ring != self._ring:
                    self.join(ring)
            except Pyro4.errors.NamingError:
                logger.info(f'No ring found in the nameserver. Announcing self.')
                ns.register(DHT_NAME, self.address)

    def _handle_periodic_calls(self):
        ''' Periodically call all periodic methods. '''
        # TODO: Somewhere here spawned error: "'NoneType' object has no attribute 'encode'".
        while True:
            try:
                self._check_predecessor()
                self._stabilize()
                self._fix_fingers()
                self._check_ring_availability()
                time.sleep(DHT_STABILIZATION_INTERVAL)
            except CommunicationError as e:
                logger.error(str(e))
            except Exception as e:
                logger.error(f'{e.__class__.__name__}: {e}.')


    # Helper / debugging methods:
    def debug_dump_successors(self) -> list:
        ''' Debugging method to dump the successors. '''
        logger.debug(f'successors: {[self._successor]}')
    
    def debug_get_ring_topology(self) -> tuple[list, bool]:
        ''' Debugging method to get all reachable nodes of the CHORD ring. Returns
        a tuple containing a list of the found succeding nodes of the ring and
        whether the ring was completely reachable. '''
        ring = [self.address]
        curr = self.successor
        while curr is not None and curr != self.address:
            ring.append(curr)
            if not reachable(curr):
                break
            with Proxy(curr) as n:
                curr = n.successor
        return ring, (curr == self.address)

    def debug_to_list(self, partial: list = []) -> list:
        ''' Return a list of the DHT members. '''
        if self.successor in partial:
            if self.successor != partial[0]:
                L = partial + [self._address, self.successor]
                logger.warning(f'Non-circular, but cyclical ring found: {L}')
            return partial
        with Proxy(self.successor) as s:
            return s.debug_to_list(partial + [self._address])
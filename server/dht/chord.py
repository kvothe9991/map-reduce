import time
import logging
import threading
import Pyro4
import Pyro4.errors
from Pyro4 import URI, Proxy

from server.utils import alive, reachable, id, in_circular_interval, SHA1_BIT_COUNT
from server.dht.logger import logger

DHT_NAME = 'chord.dht'
FINGER_TABLE_SIZE = SHA1_BIT_COUNT // 2



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
        packed_data = {'URI': f'{self.address.host}, {self._id:.1e}'}
        logger = logging.LoggerAdapter(logger, packed_data)
        
        # Finger table and its stabilization.
        self._finger_table = [None] * FINGER_TABLE_SIZE
        self._current_finger_ = 0   # Self updating counter for finger table.

        # Handle periodic stabilization calls.
        self._stabilize_thread = threading.Thread(target=self._handle_periodic_calls)
        self._stabilize_thread.setDaemon(True)
        self._stabilize_thread.start()

    def __repr__(self):
        return f'{self.__class__.__name__}<addr={self._address}, id={self._id:.2e}>'

    def __str__(self):
        return repr(self)

    @property
    def _current_finger(self) -> int:
        ''' Cycles periodically between all `m` fingers of the table. '''
        self._current_finger_ += 1
        self._current_finger_ %= FINGER_TABLE_SIZE
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
            logger.debug(f'No successor to find.')
            return self.address
        
        # Search if `x` belongs to this node's domain, otherwise check the finger table.
        if in_circular_interval(x, l=self.id, r=id(self.successor)):
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
            if f_addr and in_circular_interval(id(f_addr), l=self.id, r=x) and reachable(f_addr):
                return f_addr
        return self.address

    def join(self, addr: URI):
        ''' Join a CHORD ring containing node `n`. '''
        if reachable(addr):
            with Proxy(addr) as n:
                logger.info(f'Joined a DHT ring containing {addr}.')
                self.predecessor = None
                self.successor = n.find_successor(self.id)
        else:
            logger.error(f'Could not join the ring, node {addr}<id={id(addr):.2e}> unreachable.')

    def notify(self, n: URI):
        ''' Remote procedure call from node `n` announcing it might be this node's
        predecessor. Assumes `n` alive since it's the one who invoked this method. '''
        if not self.predecessor or in_circular_interval(id(n), l=id(self.predecessor), r=self.id):
            self.predecessor = n


    # Periodic methods:
    def _stabilize(self):
        ''' Verify it's own immediate succesor, checking for new nodes that may have
        inserted themselves unannounced. This method is called periodically. '''
        
        if self.successor is None:
            logger.info(f'successor is dead.')
            raise NotImplementedError()
        if self.successor == self.address:
            if self.predecessor:
                self.successor = self.predecessor
            return
        try:
            with Proxy(self.successor) as s:
                x = s.predecessor
                if ((x is not None)
                and in_circular_interval(id(x), l=self.id, r=id(self.successor))
                and reachable(x)):
                    self.successor = x
                s.notify(self.address)
        except Pyro4.errors.CommunicationError:
            logger.error(f'Could not reach successor {self.successor}.')
            self.successor = self.address

    def _fix_fingers(self):
        ''' Refresh finger table entries. This method is called periodically. '''
        i = self._current_finger
        n = (self.id + 2**i) % 2**SHA1_BIT_COUNT
        self._finger_table[i] = self.find_successor(n)
        assert isinstance(self._finger_table[i], URI), 'finger table entry is not a URI object.'
    
    def _check_predecessor(self):
        ''' Check for predecessor failure. '''
        if self.predecessor and not reachable(p := self.predecessor):
            logger.info(f'Predecessor {p.host}<id={id(p):.2e}> is dead.')
            self.predecessor = None

    def _handle_periodic_calls(self):
        ''' Periodically call all periodic methods. '''
        while True:
            try:
                self._check_predecessor()
                self._stabilize()
                self._fix_fingers()
                time.sleep(1)
            except Pyro4.errors.ConnectionClosedError as e:
                logger.error(str(e))
                continue


    # Helper / debugging methods:
    def debug_dump_finger_table(self):
        ''' Debugging method to dump the finger table. '''
        logger.debug(f'finger table: {self._finger_table}')

    def debug_dump_successors(self):
        ''' Debugging method to dump the successors. '''
        logger.debug(f'successors: {self._successor}')
    
    def debug_get_ring_topology(self) -> tuple[list, bool]:
        ''' Debugging method to get all reachable nodes of the CHORD ring. Returns
        a tuple containing a list of the found succeding nodes of the ring and
        whether the ring was completely reachable. '''
        ring = [self.address]
        curr = self.successor
        while curr and curr != self.address and reachable(curr):
            ring.append(curr)
            with Proxy(curr) as n:
                curr = n.successor
        return ring, (curr == self.address)
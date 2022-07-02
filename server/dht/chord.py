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
        self.logger = logging.LoggerAdapter(logger, {'URI': f'{self._address}<{self._id:.2e}>'})
        
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

    @property
    def predecessor(self) -> URI:
        return self._predecessor


    # Exposed RPCs:
    def find_successor(self, x: int) -> URI:
        ''' Return immediate successor of id `x`, in address form. '''
        
        # Trivial cases for the ring being one-node long.
        if self.successor is None or self.successor == self.address:
            self.logger.debug(f'there is no successor.')
            return self.address
        
        # Search if `x` belongs to this node's domain, otherwise check the finger table.
        # if self.id < x <= id(self.successor):
        #     return self.successor
        # elif self.id > id(self.successor) and (self.id < x or x <= id(self.successor)): 
        if in_circular_interval(x, l=self.id, r=id(self.successor)):
            return self.successor
        else:
            with Proxy(self.closest_preceding_node(x)) as n:
                # closest_preceding_node() guarantees that the returned node is alive.
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
            else:
                continue
        return self.address

    def join(self, addr: URI):
        ''' Join a CHORD ring containing node `n`. '''
        if reachable(addr):
            self.logger.info(f'attempting to join node {addr}<id={id(addr):.2e}>.')
            with Proxy(addr) as n:
                self._predecessor = None
                self._successor = n.find_successor(self.id)
                self.logger.info(f'Joined the ring with successor {addr}<id={id(addr):.2e}>.')
        else:
            self.logger.error(f'Could not join the ring, node {addr}<id={id(addr):.2e}> unreachable.')

    def notify(self, n: URI):
        ''' Remote procedure call from node `n` announcing it might be this node's
        predecessor. Assumes `n` alive since it's the one who invoked this method. '''
        if not self.predecessor or (id(self.predecessor) < id(n) < self.id):
            self._predecessor = n
            self.logger.info(f'Found new predecessor {n}<id={id(n):.2e}>.')


    # Periodic methods:
    def _stabilize(self):
        ''' Verify it's own immediate succesor, checking for new nodes that may have
        inserted themselves unannounced. This method is called periodically. '''
        
        if self.successor is None:
            self.logger.info(f'successor is dead.')
            raise NotImplementedError()
        if self.successor == self.address:
            if self.predecessor:
                self._successor = self.predecessor
            return
        try:
            with Proxy(self.successor) as s:
                x = s.predecessor
                if (x is not None) and (self.id < id(x) < id(self.successor)) and reachable(x):
                    self._successor = x
                    self.logger.info(f'found new successor {x}<id={id(x):.2e}>.')
            with Proxy(self.successor) as s:
                s.notify(self.address)
        except Pyro4.errors.CommunicationError:
            self.logger.error(f'Could not reach successor {self.successor}.')
            self._successor = self.address

    def _fix_fingers(self):
        ''' Refresh finger table entries. This method is called periodically. '''
        i = self._current_finger
        n = (self.id + 2**i) % 2**SHA1_BIT_COUNT
        self._finger_table[i] = self.find_successor(n)
    
    def _check_predecessor(self):
        ''' Check for predecessor failure. '''
        if self.predecessor and not reachable(p := self.predecessor):
            self._predecessor = None
            self.logger.info(f'predecessor {p}<id={id(p):.2e}> is dead.')

    def _handle_periodic_calls(self):
        ''' Periodically call all periodic methods. '''
        while True:
            try:
                self._check_predecessor()
                self._stabilize()
                self._fix_fingers()
                time.sleep(0.01)
            except Pyro4.errors.ConnectionClosedError as e:
                self.logger.error(str(e))
                continue


    # Helper / debugging methods:
    def debug_dump_finger_table(self):
        ''' Debugging method to dump the finger table. '''
        self.logger.debug(f'finger table: {self._finger_table}')

    def debug_dump_successors(self):
        ''' Debugging method to dump the successors. '''
        self.logger.debug(f'successors: {self._successor}')
    
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
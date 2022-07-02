import Pyro4
from Pyro4 import URI, Proxy

from server.utils import alive, reachable, id, SHA1_BIT_COUNT
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
        
        # Finger table and its stabilization.
        self._finger_table = [None] * FINGER_TABLE_SIZE
        self._current_finger_ = 0   # Self updating counter for finger table.

    def __repr__(self):
        return f'{self.__class__.__name__}<addr={self._address}, id={self._id}>'

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
        
        # Trivial case for the ring being one-node long.
        if self.successor is None:
            logger.info(f'[{self}] successor is dead.')
            return self.address
        
        # Search if `x` belongs to this node's domain, otherwise check the finger table.
        if self.id < x <= id(self.successor):
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
            if self.id < id(f_addr) < x and reachable(f_addr):
                return f_addr
            else:
                continue
        return self.address

    def join(self, addr: URI):
        ''' Join a CHORD ring containing node `n`. '''
        if reachable(addr):
            with Proxy(addr) as n:
                self.predecessor = None
                self.successor = n.find_successor(self.id)
                logger.info(f'[{self}] has joined the ring with successor {n}<id={id(n)}>.')
        else:
            logger.error(f'[{self}] Could not join the ring, node {addr}<id={id(addr)}> unreachable.')

    def notify(self, n: URI):
        ''' Remote procedure call from node `n` announcing it might be this node's
        predecessor. Assumes `n` alive since it's the one who invoked this method. '''
        if not self.predecessor or (id(self.predecessor) < id(n) < self.id):
            self.predecessor = n
            logger.info(f'[{self}] found new predecessor {n}<id={id(n)}>.')


    # Periodic methods:
    def _stabilize(self):
        ''' Verify it's own immediate succesor, checking for new nodes that may have
        inserted themselves unannounced. This method is called periodically. '''
        
        if self.successor is None:
            logger.info(f'[{self}] successor is dead.')
            raise NotImplementedError()
        with Proxy(self.successor) as s:
            x = s.predecessor
            if (x is not None) and (self.id < id(x) < id(s)) and reachable(x):
                self._successor = x
                logger.info(f'[{self}] found new successor {x}<id={id(x)}>.')
        with Proxy(self.successor) as s:
            s.notify(self.address)

    def _fix_fingers(self):
        ''' Refresh finger table entries. This method is called periodically. '''
        i = self._current_finger
        self._finger_table[i] = self.find_successor(self.id + 2**i)
    
    def _check_predecessor(self):
        ''' Check for predecessor failure. '''
        if self.predecessor and not reachable(p := self.predecessor):
            self.predecessor = None
            logger.info(f'[{self}] predecessor {p}<id={id(p)}> is dead.')


    # Helper / debugging methods:
    def debug_dump_finger_table(self):
        ''' Debugging method to dump the finger table. '''
        logger.debug(f'[{self}] finger table: {self._finger_table}')

    def debug_dump_successors(self):
        ''' Debugging method to dump the successors. '''
        logger.debug(f'[{self}] successors: {self._successor}')
    
    def debug_get_ring_topology(self) -> tuple[list, bool]:
        ''' Debugging method to get all reachable nodes of the CHORD ring. Returns
        a tuple containing a list of the found succeding nodes of the ring and
        whether the ring was completely reachable. '''
        ring = [self.address]
        curr = self.successor
        while curr != self.address and reachable(curr):
            ring.append(curr)
            with Proxy(curr) as n:
                curr = n.successor
        return ring, (curr == self.address)
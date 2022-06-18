from __future__ import annotations
import Pyro4
from Pyro4 import URI, Proxy
from typing import Union, Optional
from server.dht.utils import id, alive, BIT_COUNT


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
        self._address = address
        self._id = id(address)

        self._predecessor = None
        self._successor = self._address
        self._finger_table = [None] * BIT_COUNT
        self._next_finger_to_fix = 0

    def __repr__(self):
        return f'{self.__class__.__name__}<{self._id}>'

    def __str__(self):
        return repr(self)

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
        if self.id < x <= id(self.successor):
            return self.successor
        else:
            with Proxy(self.closest_preceding_node(x)) as n:
                return n.find_successor(x)

    def closest_preceding_node(self, x: int) -> URI:
        ''' Search the local finger table for the closest predecessor of id `x`,
        running the table in reverse order for convenient convergence to the furthest
        node from self available, which is returned in address form. '''
        for finger in self._finger_table[:0:-1]:
            if self.id < id(finger) < x:
                return finger
        return self.address

    def join(self, address: URI):
        ''' Join a CHORD ring containing node `n`. '''
        with Proxy(address) as n:
            self.predecessor = None
            self.successor = n.find_successor(self.id)

    def notify(self, n: URI):
        ''' Remote procedure call from node `n` announcing it might be this node's
        predecessor. '''
        if not self.predecessor or (id(self.predecessor) < id(n) < self.id):
            self.predecessor = n


    # Periodic methods:
    def _stabilize(self):
        ''' Verify it's own immediate succesor, checking for new nodes that
        may have inserted themselves unannounced. This method is called periodically. '''
        with Proxy(self.successor) as s:
            x = s.predecessor
            if self.id < id(x) < id(s):
                self._successor = x
        with Proxy(self.successor) as s:
            s.notify(self.address)
    
    def _update_next_finger(self) -> int:
        self._next_finger_to_fix += 1
        self._next_finger_to_fix %= BIT_COUNT
        return self._next_finger_to_fix

    def _fix_fingers(self):
        ''' Refresh finger table entries. This method is called periodically. '''
        i = self._update_next_finger()
        self._finger_table[i] = self.find_successor(self.id + 2**i)
    
    def _check_predecessor(self):
        ''' Check for predecessor failure. '''
        with Proxy(self.predecessor) as p:
            if not alive(p):
                self._predecessor = None
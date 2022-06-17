from __future__ import annotations
import Pyro4
from typing import Union
from server.dht.utils import id_from, HASH_BIT_COUNT


@Pyro4.expose
@Pyro4.behavior('single')
class ChordNode:
    def __init__(self,
                 ip_address: str,
                 successor_cache_size: int = 4
                 ):
        '''
        Instances a new CHORD node with identifier obtained from its IP address.

        The `successor_cache_size` argument defines the amount of successors accounted
        for to mantain robustness of CHORD's stability in case of several contigous
        nodes failing simultaneously.
        '''
        self._address = ip_address
        self._id = id_from(ip_address)
        
        self._predecessor: ChordNode = None
        self._successors: list[ChordNode] = [None] * successor_cache_size
        self._finger_table: list[ChordNode] = [None] * HASH_BIT_COUNT

    def __repr__(self):
        return f'{self.__class__.__name__}<{self._id}>'

    def __str__(self):
        return repr(self)

    def __lt__(self, other: Union[int, ChordNode]):
        if isinstance(other, int):
            return self.id < other
        elif isinstance(other, ChordNode):
            return self.id < other.id
        else:
            t1, t2 = type(self), type(other)
            raise TypeError(f"'<' is not supported between instances of {t1} and {t2}.")

    def __le__(self, other: Union[int, ChordNode]):
        if isinstance(other, int):
            return self.id <= other
        elif isinstance(other, ChordNode):
            return self.id <= other.id
        else:
            t1, t2 = type(self), type(other)
            raise TypeError(f"'<=' is not supported between instances of {t1} and {t2}.")
    
    def __gt__(self, other: Union[int, ChordNode]):
        if isinstance(other, int):
            return self.id > other
        elif isinstance(other, ChordNode):
            return self.id > other.id
        else:
            t1, t2 = type(self), type(other)
            raise TypeError(f"'>' is not supported between instances of {t1} and {t2}.")
    
    def __ge__(self, other: Union[int, ChordNode]):
        if isinstance(other, int):
            return self.id >= other
        elif isinstance(other, ChordNode):
            return self.id >= other.id
        else:
            t1, t2 = type(self), type(other)
            raise TypeError(f"'>=' is not supported between instances of {t1} and {t2}.")

    @property
    def id(self) -> int:
        return self._id

    @property
    def successor(self, k=0) -> ChordNode:
        return self._successors[k]

    @property
    def predecessor(self) -> ChordNode:
        return self._predecessor

    def find_successor(self, id: int) -> ChordNode:
        if self < id <= self.successor:
            return self.successor
        else:
            n = self.closest_preceding_node(id)
            return n.find_successor(id)

    def closest_preceding_node(self, id: int) -> ChordNode:
        for finger in self._finger_table[:0:-1]:
            if self < finger < id:
                return finger
        return self

    
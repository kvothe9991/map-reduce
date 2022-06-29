from hashlib import sha1
from typing import Callable, Union

from Pyro4 import Proxy, URI
from Pyro4.errors import CommunicationError

SHA1_BIT_COUNT = 160


def alive(proxy: Union[Proxy,URI]) -> bool:
    if isinstance(proxy, URI):
        with Proxy(proxy) as p:
            return p.ping()
    is_alive = True
    try:
        proxy._pyroBind()
    except CommunicationError:
        is_alive = False
    finally:
        return is_alive

def id(key: Union[str,URI], hash: Callable = sha1) -> int:
    ''' Returns a numerical identifier obtained from hashing an string key.
    Additional support for Pyro URIs for convenience. '''
    if isinstance(key, URI):
        key = key.host
    hex_hash = hash(key.encode()).hexdigest()
    id = int(hex_hash, base=16)
    return id

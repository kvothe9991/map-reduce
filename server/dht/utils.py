from hashlib import sha1
from typing import Callable, Union

from Pyro4 import URI, Proxy
from Pyro4.errors import CommunicationError

BIT_COUNT = 160


def alive(proxy: Proxy):
    try:
        proxy._pyroBind()
    except CommunicationError:
        return False
    finally:
        return True

def id(key: Union[str,URI], hash: Callable = sha1) -> int:
    ''' Returns a numerical identifier obtained from hashing an string key.
    Additional support for Pyro URIs for convenience. '''
    if isinstance(key, URI):
        key = key.host
    hex_hash = hash(key.encode()).hexdigest()
    id = int(hex_hash, base=16)
    return id

from hashlib import sha1
from typing import Callable, Union

from Pyro4 import Proxy, URI
from Pyro4.errors import CommunicationError

SHA1_BIT_COUNT = 160


def alive(proxy: Proxy) -> bool:
    ''' Returns True if the given proxy is alive. '''
    if proxy is None:
        raise ValueError('Proxy to live-check must not be None.')
    try:
        proxy._pyroBind()
        return True
    except CommunicationError as e:
        return False

def reachable(addr: URI) -> bool:
    ''' Returns True if the given URI is exposed by the Pyro daemon. '''
    if addr is None:
        raise ValueError('Address to check for reachable must not be None.')
    try:
        with Proxy(addr) as proxy:
            proxy._pyroBind()
    except CommunicationError as e:
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

from hashlib import sha1
from typing import Callable, Union
from Pyro4 import Proxy, URI
from Pyro4.errors import CommunicationError, ConnectionClosedError

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
        return True
    except Exception as e:
        return False

def id(key: URI, hash: Callable = sha1) -> int:
    ''' Returns a numerical identifier obtained from hashing an string key.
    Additional support for Pyro URIs for convenience. '''
    assert isinstance(key, URI), 'address to id must be provided in URI form.'
    key = key.host
    hex_hash = hash(key.encode('utf8')).hexdigest()
    return int(hex_hash, base=16)

def in_arc(x: int, l: int, r: int):
    assert l != r, f'Left and right bounds must not be equal for circular comparison.'
    return (l < x <= r) or ((l > r) and (l < x or x <= r))
    # return (l < x <= r) if l <= r else (l < x or x <= r)
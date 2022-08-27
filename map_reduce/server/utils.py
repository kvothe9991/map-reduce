import logging
from hashlib import sha1
from threading import Thread
from typing import Callable

import Pyro4
import Pyro4.errors
from Pyro4 import Proxy, URI

from map_reduce.server.configs import IP, DAEMON_PORT

SHA1_BIT_COUNT = 160


def alive(proxy: Proxy) -> bool:
    ''' Returns True if the given proxy is alive. '''
    if proxy is None:
        raise ValueError('Proxy to live-check must not be None.')
    try:
        proxy._pyroBind()
        return True
    except Pyro4.errors.CommunicationError as e:
        return False

def reachable(addr: URI) -> bool:
    ''' Returns True if the given URI is exposed by the Pyro daemon. '''
    if addr is None:
        raise ValueError('Address to check for reachable must not be None.')
    try:
        with Proxy(addr) as proxy:
            proxy._pyroBind()
        return True
    except Pyro4.errors.CommunicationError as e:
        return False

def id(key: URI, hash: Callable = sha1) -> int:
    ''' Returns a numerical identifier obtained from hashing an string key.
    Additional support for Pyro URIs for convenience. '''
    # assert isinstance(key, URI), 'address to id must be provided in URI form.'
    if isinstance(key, URI):
        key = key.host
    hex_hash = hash(key.encode('utf8')).hexdigest()
    return int(hex_hash, base=16)

def in_arc(x: int, l: int, r: int):
    assert l != r, f'Left and right bounds must not be equal for circular comparison.'
    return (l < x <= r) or ((l > r) and (l < x or x <= r))

def unpack(uri: URI):
    assert isinstance(uri, URI), 'Provided `uri` to unpack must be of type `Pyro4.URI`.'
    return uri.object, uri.host, uri.port

def daemon_address(name: str, ip: str = IP, port: int = DAEMON_PORT) -> URI:
    ''' Returns the URI of the daemon. '''
    return URI(f'PYRO:{name}@{ip}:{port}')

def service_address(uri: URI) -> URI:
    name, host, port = unpack(uri)
    return URI(f'PYRO:{name}.service@{host}:{port}')

def split(ls: list, at: int):
    return ls[:at], ls[at:]

def spawn_thread(target: Callable, args: tuple = (), kwargs: dict = {}) -> Thread:
    ''' Spawns a thread from a target function and arguments. '''
    thread = Thread(target=target, args=args, kwargs=kwargs)
    thread.setDaemon(True)
    thread.start()
    return thread

def kill_thread(thread: Thread, logger: logging.Logger = None, timeout=0.1):
    ''' Safely stops a thread from execution, and asserts its dead status. '''
    thread.join(timeout)
    if thread.is_alive() and logger is not None:
        logger.error(f'Error killing thread.')
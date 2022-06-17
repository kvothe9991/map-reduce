from hashlib import sha1
from typing import Callable, Tuple

HASH_BIT_COUNT = 160


def id_from(key: str, hash: Callable = sha1) -> int:
    ''' Returns a numerical identifier obtained from hashing a string key. '''
    hex_hash = hash(key.encode()).hexdigest()
    id = int(hex_hash, base=16)
    return id
from threading import Lock
from typing import Generic, TypeVar

from map_reduce.server.logger import get_logger
logger = get_logger('lock', adapter={'IP': ''})


T = TypeVar('T')
class LockedObject(Generic[T]):
    def __init__(self, obj: T):
        self.obj = obj
        self.lock = Lock()
    
    def __enter__(self):
        if not self.lock.acquire():
            logger.error(f"Couldn't acquire lock for object.")
        return self.obj

    def __exit__(self, *args):
        self.lock.release()
import types
import marshal
from typing import Callable


class Task(Callable):
    ''' Wrapper for a function (map / reduce) that can be serialized easily. '''
    def __init__(self, func: function):
        self._function = func
    
    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)
    
    def serialize(self):
        return marshal.dumps(self._function.__code__)
    
    @classmethod
    def deserialize(cls, bytes_: bytes, name: str = None):
        return cls(types.FunctionType(marshal.loads(bytes_), name=name))
from Pyro4 import Proxy
from Pyro4.errors import CommunicationError 

def alive(proxy: Proxy):
    try:
        proxy._pyroBind()
    except CommunicationError:
        return False
    finally:
        return True
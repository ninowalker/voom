'''
Created on Nov 18, 2012

@author: nino
'''
import threading

#: global active bus
_bus = None

class _TLS(threading.local):
    current_bus = None
_tls = _TLS()


def get_current_bus():
    global _bus
    return getattr(_tls, "current_app", None) or _bus


def set_default_bus(bus):
    global _bus
    _bus = bus
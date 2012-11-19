import threading

__version__ = "0.0.2"

#: global active bus
_bus = None


def get_current_bus():
    return _bus


def set_default_bus(bus):
    global _bus
    _bus = bus
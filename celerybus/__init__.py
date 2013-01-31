import threading

__version__ = "0.9.2"

#: global active bus
_bus = None


def get_current_bus():
    return _bus


def set_default_bus(bus):
    """Set to provide a global bus singleton."""
    global _bus
    _bus = bus

import collections
import inspect
import logging
import types
from itertools import chain

__ALL__ = ['Bus']

LOG = logging.getLogger(__name__)

class Bus(object):
    ALL = "ALL"
    
    verbose = False
    
    @classmethod
    def resetConfig(cls):
        cls._global_handlers = set()
        cls._message_handlers = collections.defaultdict(set)

    @classmethod
    def send(cls, message, fail_on_error=False):
        for callback in chain(cls._global_handlers, cls._message_handlers[type(message)]):
            try:
                if cls.verbose:
                    LOG.debug("invoking %s: %s", callback, message)
                callback(message)
            except Exception:
                LOG.exception("Callback failed: %s. Failed to send message: %s", callback, message)
                if fail_on_error:
                    raise
    
    @classmethod
    def subscribe(cls, message_type, callback):
        if message_type != cls.ALL:
            assert inspect.isclass(message_type), type(message_type)
            cls._message_handlers[message_type].add(callback)
        else:
            cls._global_handlers.add(callback)
        
    @classmethod
    def register(cls, handler):
        receiver_of = getattr(handler, '_receiver_of', None)
        if not receiver_of:
            if hasattr(handler, '__class__'):
                receiver_of = getattr(handler.__class__, '_receiver_of', None)
        #assert hasattr(handler, '_receiver_of')
        assert receiver_of
        for msg_type in receiver_of:
            cls.subscribe(msg_type, handler)
            

Bus.resetConfig()
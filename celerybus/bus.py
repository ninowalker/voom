import collections
import logging
import types
from itertools import chain

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
            assert type(message_type) == types.TypeType, type(message_type)
            cls._message_handlers[message_type].add(callback)
        else:
            cls._global_handlers.add(callback)
        
    @classmethod
    def register(cls, handler):
        assert hasattr(handler, '_receiver_of')
        for msg_type in handler._receiver_of:
            cls.subscribe(msg_type, handler)
        

Bus.resetConfig()
import collections
import inspect
from itertools import chain

class Handler(object):
    _handler_functions = collections.defaultdict(set)
        
    def __call__(self, message):
        exception = None
        try:
            self.pre_dispatch(message)
            self.dispatch(message)
        except Exception, e:
            exception = e
            self.on_failure(message, e)
        finally:
            self.post_dispatch(message, exception)

    def dispatch(self, message):
        for receiver in self._receivers(type(message)):
            receiver(message)
    
    def pre_dispatch(self, message):
        pass
    
    def post_dispatch(self, message, exception):
        pass
    
    @property
    def _receiver_of(self):
        msgs = set()
        for attr in inspect.getmembers(self):
            if hasattr(attr, '_receiver_of'):
                msgs.update(attr._receiver_of)
        return msgs
    
    @property
    def _receivers(self, message_type):
        for attr in inspect.getmembers(self):
            if message_type in getattr(attr, '_receiver_of', set()):
                yield attr

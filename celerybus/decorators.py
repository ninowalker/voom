
from logging import getLogger
import functools

LOG = getLogger('celerybus.bus')


class MessageHandler(object):
    def __init__(self, function, receives=None):
        self.func = function
        self._precondition = None
        self._receiver_of = receives
        functools.update_wrapper(self, self.func)
        
    def __call__(self, *args, **kwargs):
        if self._precondition:
            if self._precondition(*args, **kwargs) == False:
                LOG.debug("precondition not met for %s, skipping", self)
                return None
            LOG.debug("precondition met for %s", self)
        return self.func(*args, **kwargs)

    def __repr__(self):
        return "<MessageHandler %s>" % repr(self.func)
    
    def precondition(self, func):
        self._precondition = func


def receiver(*messages, **kwargs):
    """Decorates a function"""
    wrapper = kwargs.pop('wrapper', MessageHandler)
    assert not kwargs, "Unknown arguments: %s" % kwargs
    return functools.partial(wrapper, receives=messages)

from logging import getLogger
import functools

LOG = getLogger('voom.bus')


class MessageHandlerWrapper(object):
    """A wrapper object for message handling callables. It supports a filter as a
    precondition to address the common situation where one only wants a subset of 
    the messages of the type.
    
    @param function: the message handler 
    @param receives: a list of types this handler handles. 
    """

    def __init__(self, function, receives=None, priority=None):
        self._func = function
        self._filter = None
        self._receiver_of = receives
        self._priority = priority
        functools.update_wrapper(self, self._func)

    def __call__(self, *args, **kwargs):
        if self._filter:
            if not self._filter(*args, **kwargs):
                LOG.debug("filter not met for %s, skipping", self)
                return None
            LOG.debug("filter met for %s", self)
        return self._func(*args, **kwargs)

    def __repr__(self):
        return "<MessageHandlerWrapper %s>" % repr(self._func)

    def filter(self, func):
        """Assign a filter to this handler."""
        self._filter = func


def receiver(*message_types, **kwargs):
    """Provides an easy declaration for what a message handler receives."""
    wrapper = kwargs.pop('wrapper', MessageHandlerWrapper)
    return functools.partial(wrapper, receives=message_types, **kwargs)

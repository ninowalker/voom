from collections import namedtuple

"""A wrapper passed internally by the bus."""
MessageEnvelope = namedtuple("MessageEnvelope", ["body"])

"""A container for details about a message + failed handler"""
InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])


class BusState(object):
    """A thread local state object."""
    def __init__(self):
        self.session = Session()
        self.current_message = None
        self._deferred = []
        self._queued_messages = []

    def consume_messages(self):
        """A destructive iterator for consuming all queued messages."""
        while self._queued_messages:
            yield self._queued_messages.pop()
        
    def is_queue_empty(self):
        """Got messages?"""
        return len(self._queued_messages) == 0
                
    def enqueue(self, message):
        """Enqueue a message during this session."""
        self._queued_messages.append(message)
        
            
class Session(dict):
    """A bag for storing session variables during a Bus.send() call."""
    def __init__(self, **kwargs):
        self.update(kwargs)

        
class SessionKeys(object):

    GATEWAY_EVENT = '_gateway_event'

    RESPONDER = "_responder"

    GATEWAY_EXTRAS = "_gateway_extras"

    GATEWAY = "_gateway"

    CORRELATION_ID = "_correlation_id"

    GATEWAY_HEADERS = "_gateway_headers"
    REPLY_SENDER = "_reply_sender"
    REPLY_TO = "_reply_to"

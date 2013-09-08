from collections import namedtuple

"""A wrapper passed internally by the bus."""
import threading
import heapq

MessageEnvelope = namedtuple("MessageEnvelope", ["body", "context"])

"""A container for details about a message + failed handler"""
InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])

ReplyContext = namedtuple("ReplyContext", ["reply_to", "responder", "thread_channel"])


class TrxState(object):
    """A thread local state object."""

    def __init__(self, trx_proxy):
        self.session = Session()
        self.current_message = None
        self.current_message_context = None
        self._deferred = []
        self._queued_messages = []
        self._indx = 1

        if trx_proxy is not None and trx_proxy.session_future:
            self.session.update(trx_proxy.session_future)

    def consume_messages(self):
        """A destructive iterator for consuming all queued messages."""
        while self._queued_messages:
            yield heapq.heappop(self._queued_messages)[1]

    def is_queue_empty(self):
        """Got messages?"""
        return len(self._queued_messages) == 0

    def enqueue(self, message, priority=None):
        """Enqueue a message during this session."""
        if priority is None:
            indx = self._indx = self._indx + 1
        else:
            indx = priority
        heapq.heappush(self._queued_messages, (indx, message))


class Session(dict):
    """A bag for storing session variables during a Bus.publish() call."""

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


class TrxProxy(threading.local):
    state = None
    session_future = None
    message_future = None


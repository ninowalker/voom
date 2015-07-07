from collections import namedtuple
from contextlib import contextmanager
import heapq
import threading
import time

"""A wrapper passed internally by the bus."""

MessageEnvelope = namedtuple("MessageEnvelope", ["body", "context"])

"""A container for details about a message + failed handler"""
InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])

ReplyContext = namedtuple("ReplyContext", ["reply_to", "responder", "thread_channel"])


class TrxState(object):
    """A thread local state object."""

    def __init__(self):
        self.current_message = None
        self._deferred = []
        self._queued_messages = []
        self._indx = 1
        self._started = None

    def begin(self):
        self._started = time.time()

    def is_running(self):
        return bool(self._started)

    def consume_messages(self):
        """A destructive iterator for consuming all queued messages."""
        while self._queued_messages:
            yield heapq.heappop(self._queued_messages)[1]

    def size(self):
        return len(self._queued_messages)

    def is_queue_empty(self):
        """Got messages?"""
        return not bool(self.size())

    def enqueue(self, message, priority=None):
        """Enqueue a message during this session."""
        if priority is None:
            indx = self._indx = self._indx + 1
        else:
            indx = priority
        heapq.heappush(self._queued_messages, (indx, message))


class ChainedDict(dict):
    def __init__(self, *args, **kwargs):
        self.parent = None
        dict.__init__(self, *args, **kwargs)

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            if self.parent:
                return self.parent[key]
            raise

    def __contains__(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def extend(self):
        d = ChainedDict()
        d.parent = self
        return d


class CactusStack:
    """
    Between the ChainedDict (frames), and this (stack),
    we implement continuation-esque frame management

    See: http://c2.com/cgi/wiki?CactusStack
    """

    def __init__(self):
        self.frame = self.globals = ChainedDict()

    @contextmanager
    def push_frame(self, f=None):
        parent = self.frame
        if f is None:
            f = parent.extend()
        self.frame = f
        try:
            yield f
        finally:
            self.frame = parent


class SessionKeys(object):
    GATEWAY_EVENT = '_gateway_event'

    RESPONDER = "_responder"

    GATEWAY_EXTRAS = "_gateway_extras"

    GATEWAY = "_gateway"

    CORRELATION_ID = "_correlation_id"

    GATEWAY_HEADERS = "_gateway_headers"
    REPLY_SENDER = "_reply_sender"
    REPLY_TO = "_reply_to"


class TrxTLS(threading.local):
    _state = None
    _stack = None

    @property
    def stack(self):
        if not self._stack:
            self._stack = CactusStack()
        return self._stack

    @property
    def state(self):
        s = self._state
        if not s:
            self._state = s = TrxState()
        return s

    @state.deleter
    def state(self):
        self._state = None

    def clear(self):
        del self.state

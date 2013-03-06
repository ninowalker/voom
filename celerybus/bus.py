import collections
import inspect
import logging
import threading
import heapq
import bisect
import sys
import traceback
from contextlib import contextmanager
from celerybus.context import Session, MessageEnvelope,\
    InvocationFailure, BusState
from celerybus.exceptions import AbortProcessing, BusError

LOG = logging.getLogger(__name__)


class DefaultBus(object):
    ALL = object()
    ERRORS = object()
    
    LOW_PRIORITY = 10000
    MEDIUM_PRIORITY = 1000
    HIGH_PRIORITY = 100
    DEFAULT_PRIORITY = MEDIUM_PRIORITY
    
    class _TLS(threading.local):
        state = None
    
    def __init__(self, verbose=False, raise_errors=False):
        self._state = self._TLS()
        self._verbose = verbose
        self._raise_errors = raise_errors
        self.resetConfig()
    
    def resetConfig(self):
        self._global_handlers = []
        self._error_handlers = []
        self._message_handlers = collections.defaultdict(list)
        self._loader = None
        self._loaded = False
        
    @property
    def raise_errors(self):
        return self._raise_errors
    
    @raise_errors.setter
    def raise_errors(self, value):
        self._raise_errors = value

    @property
    def loader(self):
        """A callable that will discover all the handlers for this bus. Defaults to None."""
        return self._loader

    @loader.setter
    def loader(self, value):
        if self._loader == value:
            return
        if self._loader:
            raise AssertionError("Bus loader already initialized with another value: %s" % self._loader)
        self._loader = value
        self._loaded = False
        
    def send(self, body, fail_on_error=False, session_vars=None):            
        message = MessageEnvelope(body)
        if not self._loaded and self._loader:
            LOG.info("running loader...")
            try:
                self._loader()
                self._loaded = True
            except:
                LOG.exception("Failed to run loader!")
                raise
                
        self._send_breadth_first(message, fail_on_error, session_vars)
        
    def defer(self, msg):
        self.state._deferred.append(MessageEnvelope(msg))
    
    def send_error(self, message, source, exception=None, tb=None):
        if exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback)[2:])
        context = traceback.format_stack()[::-1]
        while "/celerybus/" in context[0]:
            context.pop(0)
        failure = InvocationFailure(message.body, exception, tb, context)
        env = MessageEnvelope(failure) 
        self._send(env, False, queue=self._error_handlers)
    
    def _send_breadth_first(self, message, fail_on_error, session_vars):
        # if the queue is not empty, we are in a transaction,
        # so queue it up and it will be processed in the invoking loop.
        root_event = self._state.state is None
        
        if root_event:
            self._state.state = BusState()
        
        if session_vars:
            self.session.update(session_vars)

        self.state.enqueue(message)
        
        if not root_event:
            return

        # this must be absolutely bullet proof
        # and we must leave this function with an 
        # empty queue or we corrupt the bus.
        try:
            for msg in self.state.consume_messages():
                self._state.state.current_message = msg
                try:
                    self._send(msg, fail_on_error)
                except Exception, e:
                    if fail_on_error or self.raise_errors:
                        raise
                    LOG.exception("Bus error handling trap.") # this should not happen
        except Exception, e:
            if fail_on_error or self.raise_errors:
                raise
            LOG.exception("Error handling fail; trapping at _send_breadth_first")
            raise BusError, (message, e), sys.exc_info()[2]
        finally:
            if not self.state.is_queue_empty():
                LOG.error("Exiting send with queued item; something is terminally wrong.")
            self._state.state = None
        
    def _send(self, message, fail_on_error, queue=None):
        if queue == None:
            queue = heapq.merge(self._global_handlers, self._message_handlers[type(message.body)])
        for priority, callback in queue:
            try:
                if self._verbose:
                    LOG.debug("invoking %s (priority=%s): %s", callback, priority, message)
                self.invoke(callback, message)
            except AbortProcessing:
                LOG.info("processing of %s aborted by %s", message, callback)
                self.state._deferred = []
                return
            except Exception, ex:
                LOG.exception("Callback failed: %s. Failed to send message: %s", callback, message)
                if fail_on_error or self.raise_errors:
                    raise
                
                if queue == self._error_handlers:
                    # avoid a circular loop
                    pass
                else:
                    # this message will push down and be
                    # queued for sending, not immediately.
                    try:
                        self.send_error(message, callback, ex)
                    except:
                        # swallow this. Something is really bad.
                        LOG.exception("Error handling failed!")
        
        while self.state._deferred:
            LOG.info("sending queued_message")
            self.send(self.state._deferred.pop(0).body, fail_on_error)
                
    def invoke(self, callback, env):
        """Injection point for doing special things before or after the callback."""
        callback(env.body)
    
    def subscribe(self, message_type, callback, priority=1000):
        LOG.debug("adding subscriber %s for %s", callback, message_type)
        handlers = self._get_handlers(message_type)
        old_item = None
        for p, c in handlers: 
            if c != callback:
                continue
            if p == priority: 
                # already registered
                LOG.debug("callback %s already registered", c)
                return
            old_item = (p, c)
            break
        if old_item:
            handlers.remove(old_item)
            LOG.info("callback %s re-registered with new priority. old=%s, new=%s", c, p, priority)
        bisect.insort(handlers, (priority, callback))
        LOG.debug("Updated handlers: %s", handlers)
        
    def unsubscribe(self, message_type, callback):
        LOG.debug("removing subscriber %s for %s", callback, message_type)
        handlers = self._get_handlers(message_type)
        for priority, cb in handlers:
            if cb == callback:               
                handlers.remove((priority, callback))
                return
        raise ValueError("callback not found")
    
    def _get_handlers(self, message_type):
        if message_type is self.ALL:
            handlers = self._global_handlers
        elif message_type is self.ERRORS:
            handlers = self._error_handlers
        else:
            assert inspect.isclass(message_type), type(message_type)
            handlers = self._message_handlers[message_type]
        return handlers
        
    def register(self, handler, priority=1000):
        receiver_of = getattr(handler, '_receiver_of', None)
        assert receiver_of
        for msg_type in receiver_of:
            self.subscribe(msg_type, handler, priority)
                    
    @property
    def current_message(self):
        return self.state.current_message if self.state else None
    
    @property
    def session(self):
        return self._state.state.session if self.state else None
    
    @property
    def state(self):
        return self._state.state
    
    
            
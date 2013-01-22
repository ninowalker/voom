import collections
import inspect
import logging
import threading
import heapq
import bisect
import sys
import traceback
from contextlib import contextmanager
from celerybus.context import RequestContext, MessageEnvelope,\
    InvocationFailure
from celerybus.exceptions import AbortProcessing

LOG = logging.getLogger(__name__)

class _TLS(threading.local):
    _queued = None
    _context_stack = None
    _current_message_frame = None
    
    @property
    def context_stack(self):
        if self._context_stack is None:
            self._context_stack = []
        return self._context_stack

    @property
    def queued(self):
        if self._queued is None:
            self._queued = []
        return self._queued

    @property
    def current_message_frame(self):
        if self._current_message_frame is None:
            self._current_message_frame = []
        return self._current_message_frame


class DefaultBus(object):
    ALL = object()
    ERRORS = object()
    
    LOW_PRIORITY = 10000
    MEDIUM_PRIORITY = 1000
    HIGH_PRIORITY = 100
    DEFAULT_PRIORITY = MEDIUM_PRIORITY
    
    def __init__(self, verbose=False, raise_errors=None):
        self.state = _TLS()
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
        if self._raise_errors is None:
            # lazy evaluation so that we ensure that celery is properly loaded.
            from celery import conf
            self._raise_errors = conf.ALWAYS_EAGER and conf.EAGER_PROPAGATES_EXCEPTIONS
            LOG.info("defaulted raise_errors to %s (always_eager=%s, propagate=%s)", 
                     self._raise_errors, conf.ALWAYS_EAGER, conf.EAGER_PROPAGATES_EXCEPTIONS)
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
        
    def send(self, body, fail_on_error=False, request_context=None):
        if not request_context:
            parent_context = self.request if self.request else None            
            request_context = RequestContext(parent=parent_context)
            
        message = MessageEnvelope(body, request_context)
        if not self._loaded and self._loader:
            LOG.info("running loader...")
            try:
                self._loader()
                self._loaded = True
            except:
                LOG.exception("Failed to run loader!")
                raise
                
        self._send_breadth_first(message, fail_on_error)
    
    def send_error(self, message, source, exception=None, tb=None):
        if exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback)[2:])
        context = traceback.format_stack()[::-1]
        while "/celerybus/" in context[0]:
            context.pop(0)
        failure = InvocationFailure(message.body, exception, tb, context)
        # TODO copy request?
        env = MessageEnvelope(failure, message.request) 
        self._send(env, False, queue=self._error_handlers)
    
    def _send_breadth_first(self, message, fail_on_error):
        root_event = len(self.state.queued) == 0
        self.state.queued.append(message)
        if not root_event:
            return

        while len(self.state.queued):
            msg = None
            try:
                msg = self.state.queued[0]
                self.state.current_message_frame.insert(0, msg)
                self._send(msg, fail_on_error)
            finally:
                self.state.queued.pop(0)
                if msg:
                    self.state.current_message_frame.pop(0)
    
    def _send(self, message, fail_on_error, queue=None):
        if queue == None:
            queue = heapq.merge(self._global_handlers, self._message_handlers[type(message.body)])
        for priority, callback in queue:
            try:
                if self._verbose:
                    LOG.debug("invoking %s (priority=%s): %s", callback, priority, message)
                with self.use_context(message.request):
                    self.invoke(callback, message)
                message.request.add_header("Processed-By", repr(callback))
            except AbortProcessing:
                LOG.info("processing of %s aborted by %s", message, callback)
                message.request.add_header("Aborted-By", repr(callback))
                return
            except Exception, ex:
                LOG.exception("Callback failed: %s. Failed to send message: %s", callback, message)
                message.request.add_header("Error", "%s - %s" % (repr(callback), ex))
                if fail_on_error or self.raise_errors:
                    raise
                
                if queue != self._error_handlers:
                    # avoid a circular loop
                    self.send_error(message, callback, ex)
        
        with self.use_context(message.request):
            for queued_msg in message.request.queued_messages:
                LOG.info("sending queued_message")
                self.send(queued_msg.body, fail_on_error)
                
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
    def request(self):
        try:
            return self.state.context_stack[0]
        except IndexError:
            return None
        
    @property
    def current_message(self):
        return self.state.current_message_frame[0] if self.state.current_message_frame else None
    
    @contextmanager
    def use_context(self, request_ctx=None):
        """Provides a means setting the current request context for the active thread.
        If request_ctx is not provided/None, the active one is used or a new one is created. 
        """
        if request_ctx is None:
            request_ctx = self.request or RequestContext()
        self.state.context_stack.insert(0, request_ctx)
        try:
            yield
        finally:
            self.state.context_stack.pop(0)

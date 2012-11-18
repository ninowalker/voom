import collections
import inspect
import logging
import threading
import heapq
import bisect
import sys
import traceback
from collections import namedtuple
from contextlib import contextmanager
from celerybus.envelopes import RequestContext, MessageEnvelope,\
    InvocationFailure

__ALL__ = ['Bus']

LOG = logging.getLogger(__name__)


class _Bus(object):
    ALL = "ALL"
    ERRORS = "ERRORS"
    BREADTH_FIRST = "breadth_first"
    DEPTH_FIRST = "depth_first"
    
    LOW_PRIORITY = 10000
    MEDIUM_PRIORITY = 1000
    HIGH_PRIORITY = 100
    DEFAULT_PRIORITY = MEDIUM_PRIORITY
    
    def __init__(self, verbose=False, always_eager_mode=BREADTH_FIRST, mode=BREADTH_FIRST, raise_errors=None, app=None):
        self.verbose = verbose
        self.mode = mode
        self.default_task_kwargs = {}
        self._raise_errors = raise_errors
        self.always_eager_mode = None
        self.breadth_queue = threading.local()
        self.request_local = threading.local()
        self.request_local.stack = []
        self.resetConfig()
        self._loader = None
        self._loaded = False
        
    @property
    def raise_errors(self):
        if self._raise_errors is None:
            from celery import conf
            self._raise_errors = conf.ALWAYS_EAGER and conf.EAGER_PROPAGATES_EXCEPTIONS
            LOG.info("defaulted raise_errors to %s (always_eager=%s)", self._raise_errors, conf.ALWAYS_EAGER)
        return self._raise_errors
    
    @raise_errors.setter
    def raise_errors(self, value):
        self._raise_errors = value

    @property
    def loader(self):
        return self._loader

    @loader.setter
    def loader(self, value):
        if self._loader == value:
            return
        assert not self._loader, "Bus loader already initialized with another value: %s" % self._loader
        self._loader = value
        self._loaded = False
        
    def resetConfig(self):
        self.breadth_queue.msgs = []
        self._global_handlers = []
        self._error_handlers = []
        self._message_handlers = collections.defaultdict(list)
        self._loader = None
        self._loaded = False

    def send(self, body, fail_on_error=False, headers=None):
        message = MessageEnvelope(body, RequestContext(headers))
        if not self._loaded and self._loader:
            LOG.info("running loader...")
            try:
                self._loader()
            except:
                LOG.exception("Failed to run loader!")
                return
            finally:
                self._loaded = True
        if self.always_eager_mode == None:
            from celery import conf
            if conf.ALWAYS_EAGER:
                self.mode = self.BREADTH_FIRST
        if self.mode == self.BREADTH_FIRST:
            self._send_breadth_first(message, fail_on_error)
            return
        self._send(message, fail_on_error)
    
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
        if not hasattr(self.breadth_queue, 'msgs'):
            self.breadth_queue.msgs = []
        root_event = len(self.breadth_queue.msgs) == 0
        self.breadth_queue.msgs.append(message)
        if not root_event:
            return

        while len(self.breadth_queue.msgs):
            self._send(self.breadth_queue.msgs[0], fail_on_error)
            self.breadth_queue.msgs.pop(0)
    
    def _send(self, message, fail_on_error, queue=None):
        if queue == None:
            queue = heapq.merge(self._global_handlers, self._message_handlers[type(message.body)])
        for priority, callback in queue:
            try:
                if self.verbose:
                    LOG.debug("invoking %s (priority=%s): %s", callback, priority, message)
                self.invoke(callback, message)
            except Exception, ex:
                LOG.exception("Callback failed: %s. Failed to send message: %s", callback, message)
                if queue != self._error_handlers:
                    # avoid a circular loop
                    self.send_error(message, callback, ex)
                if fail_on_error or self.raise_errors:
                    raise
                
    def invoke(self, callback, message):
        """Injection point for doing special things before or after the callback."""
        with self.use_context(message.request):
            callback(message.body)
    
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
        if message_type == self.ALL:
            handlers = self._global_handlers
        elif message_type == self.ERRORS:
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
            return self.request_local.stack[0]
        except IndexError:
            return None
    
    @contextmanager
    def use_context(self, request_ctx):
        self.request_local.stack.insert(0, request_ctx)
        try:
            yield
        finally:
            self.request_local.stack.pop(0)

            
Bus = _Bus()
Bus.resetConfig()

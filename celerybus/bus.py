import bisect
import collections
from collections import namedtuple
import heapq
import inspect
import logging
import os
import sys
import threading
import traceback

__ALL__ = ['Bus']

LOG = logging.getLogger(__name__)

InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])

class _Bus(object):
    ALL = "ALL"
    ERRORS = "ERRORS"
    BREADTH_FIRST = "breadth_first"
    DEPTH_FIRST = "depth_first"
    
    LOW_PRIORITY = 10000
    MEDIUM_PRIORITY = 1000
    HIGH_PRIORITY = 100
    DEFAULT_PRIORITY = MEDIUM_PRIORITY
    
    DEFAULT_TASK_KWARGS = None
    
    def __init__(self, verbose=False, always_eager_mode=BREADTH_FIRST, mode=BREADTH_FIRST, raise_errors=None, default_task_kwargs=DEFAULT_TASK_KWARGS):
        self.verbose = verbose
        self.mode = mode

        if raise_errors is not None:
            self.raise_errors = raise_errors
        else:
            from celery import conf
            self.raise_errors = conf.ALWAYS_EAGER and conf.EAGER_PROPAGATES_EXCEPTIONS

        self.always_eager_mode = None
        self.breadth_queue = threading.local()
        self.resetConfig()
        self.default_task_kwargs = default_task_kwargs or {}
    
    def resetConfig(self):
        self.breadth_queue.msgs = []
        self._global_handlers = []
        self._error_handlers = []
        self._message_handlers = collections.defaultdict(list)

    def loadConfig(self):
        assert loader, "The bus must be setup before it can load its config."
        loader.setup_bus(self)

    def send(self, message, fail_on_error=False):
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
        failure = InvocationFailure(message, exception, tb, context)
        self._send(failure, False, queue=self._error_handlers)
    
    def _send_breadth_first(self, message, fail_on_error):
        if not hasattr(self.breadth_queue, 'msgs'):
            self.breadth_queue.msgs = []
        root_event = len(self.breadth_queue.msgs) == 0
        self.breadth_queue.msgs.append(message)
        if not root_event:
            return

        while len(self.breadth_queue.msgs):
            try:
                self._send(self.breadth_queue.msgs[0], fail_on_error)
                self.breadth_queue.msgs.pop(0)
            except:
                try:
                    LOG.exception(u"Failed to process message: %s", message)
                except:
                    LOG.exception("This is bad: can't log exception for message of type %s", type(message))
                if fail_on_error or self.raise_errors:
                    raise
    
    def _send(self, message, fail_on_error, queue=None):
        if queue == None:
            queue = heapq.merge(self._global_handlers, self._message_handlers[type(message)])
        for priority, callback in queue:
            try:
                if self.verbose:
                    LOG.debug("invoking %s (priority=%s): %s", callback, priority, message)
                callback(message)
            except Exception, ex:
                LOG.exception("Callback failed: %s. Failed to send message: %s", callback, message)
                if queue != self._error_handlers:
                    # avoid a circular loop
                    self.send_error(message, callback, ex)
                if fail_on_error or self.raise_errors:
                    raise
    
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
        handlers.remove(callback)
    
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
        if not receiver_of:
            if hasattr(handler, '__class__'):
                receiver_of = getattr(handler.__class__, '_receiver_of', None)
        assert receiver_of
        for msg_type in receiver_of:
            self.subscribe(msg_type, handler, priority)
    
                
Bus = _Bus()
Bus.resetConfig()

loader = None
def setup_bus(Bus):
    global loader
    from celery.utils import get_cls_by_name    
    try:
        loader_cls = get_cls_by_name(os.environ.get('CELERYBUS_LOADER'))
    except (ValueError, ImportError, AttributeError):
        LOG.warning("celerybus config not found, running without a config.")
        return
    loader = loader_cls()
    Bus.loadConfig()
setup_bus(Bus)

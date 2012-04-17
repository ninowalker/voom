import collections
import inspect
import logging
import threading
import heapq
import bisect

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
    
    def __init__(self, verbose=False, always_eager_mode=BREADTH_FIRST, mode=BREADTH_FIRST):
        self.verbose = verbose
        self.mode = mode
        self.always_eager_mode = None
        self.breadth_queue = threading.local()
        self.resetConfig()
    
    def resetConfig(self):
        self.breadth_queue.msgs = []
        self._global_handlers = []
        self._error_handlers = []
        self._message_handlers = collections.defaultdict(list)

    def send(self, message, fail_on_error=False):
        if self.always_eager_mode == None:
            from celery import conf
            if conf.ALWAYS_EAGER:
                self.mode = self.BREADTH_FIRST
        if self.mode == self.BREADTH_FIRST:
            self._send_breadth_first(message, fail_on_error)
            return
        self._send(message, fail_on_error)
        
    
    def _send_breadth_first(self, message, fail_on_error):
        root_event = len(self.breadth_queue.msgs) == 0
        self.breadth_queue.msgs.append(message)
        if not root_event:
            return

        while len(self.breadth_queue.msgs):
            self._send(self.breadth_queue.msgs[0], fail_on_error)
            self.breadth_queue.msgs.pop(0)
        
    
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
                    self._send((message, callback, ex), False, queue=self._error_handlers)
                if fail_on_error:
                    raise
    
    def subscribe(self, message_type, callback, priority=1000):
        handlers = None
        if message_type == self.ALL:
            handlers = self._global_handlers
        elif message_type == self.ERRORS:
            handlers = self._error_handlers
        else:
            assert inspect.isclass(message_type), type(message_type)
            handlers = self._message_handlers[message_type]

        LOG.debug("adding subscriber %s for %s", callback, message_type)
        
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
        #heapq.heapify(handlers)
        #sorted(handlers, key=lambda x: x[0])
        LOG.info("Updated handlers: %s", handlers)
        
    def register(self, handler, priority=1000):
        receiver_of = getattr(handler, '_receiver_of', None)
        if not receiver_of:
            if hasattr(handler, '__class__'):
                receiver_of = getattr(handler.__class__, '_receiver_of', None)
        #assert hasattr(handler, '_receiver_of')
        assert receiver_of
        for msg_type in receiver_of:
            self.subscribe(msg_type, handler, priority)
            
Bus = _Bus()
Bus.resetConfig()
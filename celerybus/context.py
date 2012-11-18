from collections import namedtuple, defaultdict
from celerybus.exceptions import AbortProcessing
import types
import time
import os
import uuid

MessageEnvelope = namedtuple("MessageEnvelope", ["body", "request"])

QueuedMessage = namedtuple("QueuedMessage", ["body"])

InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])

class RequestContext(object):
    def __init__(self, parent=None):
        self._headers = defaultdict(list)
        self.add_header("ID", str(uuid.uuid4()))
        self.add_header("Created", time.time())
        self.add_header("Process", os.getpid())
        self.add_header("Host", os.uname()[1])
        
        if parent and parent.headers:
            for h, val in parent.headers.iteritems():
                if h in self._headers:
                    continue
                if isinstance(val, (list, tuple)):
                    for _v in val:
                        self.add_header(h, _v)
                else:
                    self.add_header(h, val)
        self._queued_messages = []
        self._context = {}
        
    def __repr__(self):
        return "%r" % dict(self.headers)
        
    def __str__(self):
        a = []
        for h, vals in self._headers.iteritems():
            for v in vals:
                a.append(u"%s: %s" % (h, v))
        for k, v in self._context.iteritems():
            a.append(u"Context-%s: %s" % (k, v))
        for m in self._queued_messages:
            a.append(u"Queued: %s" % m)
        return "\n".join(a)
            
    def cancel(self, reason=None):
        """Aborts the processing of the current bus message."""
        raise AbortProcessing(reason or "cancelled")
        
    def add_header(self, key, value):
        """Adds a header; multiple headers of the same type can be added."""
        if type(value) not in (str, unicode, int, float, bool, types.NoneType):
            raise ValueError("Only scalar types are allowed as headers (%s)" % type(value))
        print self._headers, self._headers[key], value
        self._headers[key].append(value)
        
    def set_context(self, key, value):
        """Provides a non-persistent (e.g. not propagated) mechanism for process-local communication of 
        values."""
        self._context[key] = value
        
    @property
    def context(self):
        return self._context

    @property
    def headers(self):
        return self._headers
    
    @property
    def queued_messages(self):
        for qm in self._queued_messages:
            yield qm
        
    def enqueue(self, message, attachments=None):
        self._queued_messages.append(QueuedMessage(message))
        
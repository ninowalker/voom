from collections import namedtuple, defaultdict
from celerybus.exceptions import AbortProcessing
import types
import time
import os
import uuid

MessageEnvelope = namedtuple("MessageEnvelope", ["body", "request"])

QueuedMessage = namedtuple("QueuedMessage", ["body"])

InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])

class BaseContext(object):
    def __init__(self, headers=None):
        self._headers = defaultdict(list)
        self.add_header("id", str(uuid.uuid4()))
        self.add_header("created", time.time())
        self.add_header("process", os.getpid())
        self.add_header("host", os.uname()[1])
        
        for h in headers or {}:
            vals = headers[h]
            if isinstance(vals, (list, tuple)):               
                self._extend_header(h, vals)
            else:
                self.add_header(h, vals)
        
    def __repr__(self):
        return "%r" % dict(self._headers)
    
    def __str__(self):
        return str(unicode(self).encode('utf8'))
        
    def __unicode__(self):
        a = []
        for h, vals in self._headers.iteritems():
            for v in vals:
                a.append(u"%s: %s" % (h, v))
        return u"\n".join(a)
            
    def add_header(self, key, value):
        """Adds a header; multiple headers of the same type can be added."""
        if type(value) not in (str, unicode, int, float, bool, types.NoneType):
            raise ValueError("Only scalar types are allowed as headers (%s)" % type(value))
        self._headers[key.lower()].append(value)
        
    def _extend_header(self, key, values):
        for _v in values:
            self.add_header(key, _v)
        
    @property
    def all_headers(self):
        return self._headers

    def headers(self, key):
        key = key.lower()
        if key not in self._headers:
            return []
        return self._headers[key]
    
    def header(self, key):
        key = key.lower()
        if key in self._headers:
            return self._headers[key][-1]
        return None
    
    def __contains__(self, key):
        key = key.lower()
        return key in self._headers
    

class SessionContext(BaseContext):
    def copy(self):
        return self.__class__(self._headers)


class RequestContext(BaseContext):
    def __init__(self, headers=None, parent=None, inherit_session=True):
        super(RequestContext, self).__init__(headers)
        self.session = None
        self._queued_messages = []
        self._context = {}

        # record the context from which this context was spawned
        if not inherit_session or not parent:
            self.session = SessionContext()
            return
        
        self.session = parent.session.copy()
        if parent.headers("Parent"):
            self._extend_header("Parent", parent.headers("Parent"))
        self.add_header("Parent", parent.header('ID'))
                
    def __repr__(self):
        return "%r (%r)" % (dict(self._headers), dict(self.session.all_headers))
        
    def __unicode__(self):
        a = []
        for k, v in self._context.iteritems():
            a.append(u"Context: %s => %s" % (k, type(v)))
        for m in self._queued_messages:
            a.append(u"Queued: %s" % type(m))
        return u"%s\n%s" % (super(RequestContext, self).__unicode__(), u"\n".join(a)) 
            
    def cancel(self, reason=None):
        """Aborts the processing of the current bus message."""
        raise AbortProcessing(reason or "cancelled")
        
    def set_context(self, key, value):
        """Provides a non-persistent (e.g. not propagated) mechanism for process-local communication of 
        values."""
        self._context[key] = value
        
    @property
    def context(self):
        return self._context

    @property
    def queued_messages(self):
        for qm in self._queued_messages:
            yield qm
        
    def enqueue(self, message):
        self._queued_messages.append(QueuedMessage(message))
        
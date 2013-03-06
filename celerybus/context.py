from collections import namedtuple, defaultdict
from celerybus.exceptions import AbortProcessing
import types
import time
import os
import uuid

MessageEnvelope = namedtuple("MessageEnvelope", ["body"])

QueuedMessage = namedtuple("QueuedMessage", ["body"])

InvocationFailure = namedtuple("InvocationFailure", ["message", "exception", "stack_trace", "invocation_context"])

class Session(dict):
    def __init__(self, **kwargs):
        self.update(kwargs)
        
    def __unicode__(self):
        a = []
        #for m in self._queued_messages:
        #    a.append(u"Queued: %s" % type(m))
        return u"%s\n%s" % (super(Session, self).__str__(), u"\n".join(a)) 
        
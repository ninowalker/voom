'''
Created on Feb 28, 2013

@author: nino
'''
import threading
import urlparse
import re
import sys
from logging import getLogger

LOG = getLogger("voom.transports")

DEFAULT = ":default"

class UnknownSchemeError(ValueError): pass

class TransportRegistry(object):
    """Provides a mapping between address schemes and a given transport."""
    def __init__(self):
        self.transports = {CurrentThreadSender.SCHEME: CurrentThreadSender()}
        
    def register(self, scheme, transport):
        self.transport[scheme] = transport
        
    def get(self, address):
        scheme = urlparse.urlparse(address).scheme
        try:
            return self.transports[scheme]
        except KeyError:
            raise UnknownSchemeError("Unknown protocol scheme in address: %s" % address)


class Sender(object):
    default_encoding = None
    def __call__(self, address, message, mimetype):
        try:
            self._send(address, message, mimetype)
        except TransportError:
            raise
        except Exception, e:
            raise TransportError, (unicode(e), e), sys.exc_info()[2] 


class TransportError(Exception):
    def __init__(self, msg, cause):
        super(TransportError, self).__init__(msg)
        self.cause = cause
        

class CurrentThreadSender(threading.local):
    """Provides a mechanism for collecting messages in the current thread
    for later processing."""
    
    SCHEME = "thread+current"
    ADDRESS = SCHEME + ":"
    default_encoding = None
    _messages = None
        
    def __call__(self, address, message, **kwargs):
        if self._messages is None:
            self._messages = []
        self._messages.append(message)

    @property            
    def messages(self):
        return self._messages or []
            
    def pop_all(self):
        try:
            return self.messages
        finally:
            self._messages = []

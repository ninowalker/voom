'''
Created on Feb 28, 2013

@author: nino
'''
import threading
import urlparse
import re
import sys
from logging import getLogger

LOG = getLogger("voom.channels")


class TransportError(Exception):
    def __init__(self, msg, cause):
        super(TransportError, self).__init__(msg)
        self.cause = cause
        

class CurrentThreadChannel(threading.local):
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

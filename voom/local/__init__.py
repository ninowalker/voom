from logging import getLogger
import re
import sys
import threading
import urlparse

LOG = getLogger("voom.channels")


class CurrentThreadChannel(threading.local):
    """Provides a mechanism for collecting messages in the current thread
    for later processing."""

    SCHEME = "thread+current"
    ADDRESS = SCHEME + ":"
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

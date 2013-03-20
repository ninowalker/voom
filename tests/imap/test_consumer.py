'''
Created on Mar 20, 2013

@author: nino
'''
import unittest
from voom.imap.consumer import IMAPConsumer
from voom.bus import VoomBus
from voom.priorities import BusPriority
from voom.imap.events import IMAPMessageDownloaded
import os
from unittest.case import SkipTest
import warnings


class Test(unittest.TestCase):
    def test_read_from_gmail(self):
        username = os.environ.get('GMAIL_USER')
        password = os.environ.get('GMAIL_PASS')
        
        if not username or not password:
            warnings.warn("Skipping read from gmail; to enable, set GMAIL_PASS and GMAIL_USER in the environment before running the test.")
            raise SkipTest
        
        imap = IMAPConsumer(("imap.gmail.com",993),
                            username,
                            password,
                            VoomBus(),
                            on_complete=self._on_complete)
        bus = imap.bus
        bus.subscribe(IMAPMessageDownloaded, self._print, priority=BusPriority.LOW_PRIORITY**3)
        
        imap.run()
        
    def _print(self, msg):
        print "*" * 80
        print msg.num, msg.mime_message
        
    def _on_complete(self, *args):
        pass
        
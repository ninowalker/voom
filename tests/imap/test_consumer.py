import unittest
from voom.imap.consumer import IMAPConsumer
from voom.bus import VoomBus
from voom.priorities import BusPriority
from voom.imap.events import IMAPMessageDownloaded
import os
from unittest.case import SkipTest
import warnings
from mock import Mock
import imaplib


class Test(unittest.TestCase):
    def test_mock_read(self):
        conn = Mock(spec=imaplib.IMAP4_SSL)
        imap = IMAPConsumer(("imap.gmail.com", 993),
                            "username",
                            'password',
                            VoomBus(),
                            on_complete=self._on_complete,
                            connection_type=Mock(return_value=conn))

        bus = imap.bus
        msgs = []
        bus.subscribe(IMAPMessageDownloaded, msgs.append)
        bus.subscribe(IMAPMessageDownloaded, self._print, priority=BusPriority.LOW_PRIORITY ** 3)

        with open(os.path.join(os.path.dirname(__file__), 'msg.txt')) as f:
            mime_text = f.read()

        conn.search.return_value = ['OK', ['1 2']]
        conn.fetch.return_value = ['OK', [(None, mime_text)]]
        conn.store.return_value = ['OK', None]
        conn.close.side_effect = Exception
        imap.run()

        assert len(msgs) == 2
        m = msgs[0].mime_message
        expected = "Ned Freed <ned@innosoft.com>"
        assert m['To'].strip() == expected, "'%s' '%s'" % (m['To'], expected)

    def test_read_from_gmail(self):
        username = os.environ.get('GMAIL_USER')
        password = os.environ.get('GMAIL_PASS')

        if not username or not password:
            warnings.warn("Skipping read from gmail; to enable, set GMAIL_PASS and GMAIL_USER in the environment before running the test.")
            raise SkipTest

        imap = IMAPConsumer(("imap.gmail.com", 993),
                            username,
                            password,
                            VoomBus(),
                            on_complete=self._on_complete)
        bus = imap.bus
        bus.subscribe(IMAPMessageDownloaded, self._print, priority=BusPriority.LOW_PRIORITY ** 3)

        imap.run()

    def _print(self, msg):
        print "*" * 80
        print msg.num, msg.mime_message

    def _on_complete(self, *args):
        pass

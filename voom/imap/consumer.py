from logging import getLogger
from voom.imap.events import IMAPMessageDownloaded
import email
import imaplib

LOG = getLogger(__name__)


class IMAPConsumer(object):
    def __init__(self, connection_params,
                 username,
                 password,
                 bus,
                 mailbox='INBOX',
                 connection_type=imaplib.IMAP4_SSL,
                 on_complete=None):
        self.connection_params = connection_params
        self.connection_type = connection_type
        self.username = username
        self.password = password
        self.mailbox = str(mailbox)
        self.on_complete = on_complete or self._on_complete
        self.bus = bus
        self.attach(bus)

    def attach(self, bus):
        pass
        #assert isinstance(bus, VoomBus)
        #bus.subscribe(IMAPMessageDownloaded, self._mark_seen, priority=BusPriority.LOW_PRIORITY**3)

    def run(self):
        conn = self.imap_server = self.connection_type(*self.connection_params)
        conn.login(self.username, self.password)
        conn.select(self.mailbox)
        typ, data = conn.search(None, 'UNSEEN')
        try:
            ids = data[0].split()
            LOG.info("%s unread messages found (%s)", len(ids), data)
            for num in ids:
                typ, msg_data = conn.fetch(num, '(RFC822)')
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_string(response_part[1])
                        self.bus.publish(IMAPMessageDownloaded(num, msg, conn))
                self._on_complete(conn, num)
        finally:
            try:
                conn.close()
            except:
                pass
            conn.logout()

    def _on_complete(self, conn, num):
        LOG.debug("marking message read")
        typ, response = conn.store(num, '+FLAGS', r'(\Seen)')

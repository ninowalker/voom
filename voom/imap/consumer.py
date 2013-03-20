'''
Created on Mar 20, 2013

@author: nino
'''
import imaplib
from voom.imap.events import IMAPMessageDownloaded
from logging import getLogger
import email

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



"""
imap_server.login(username, password)
status, response = imap_server.status('INBOX', "(UNSEEN)")
status
response
unreadcount = int(response[0].split()[2].strip(').,]'))
print unreadcount
_ip.magic("paste ")
_ip.magic("paste ")
ids = emails_from("kjkausha@yahoo.com")
imap_server.select('INBOX')
ids = emails_from("kjkausha@yahoo.com")
get_emails(email_ids)
get_emails(ids[0:2])
a = _
a[0]
import email
m email.message_from_string(a[0])
m = email.message_from_string(a[0])
m
m.get_all()
m.keys()
m.values()
m.as_string()
m.epilogue
m._headers
m.defects
a[0][0:100]
m.get_payload()
m.get_payload()
m = email.message_from_string(a[0])
def get_emails(email_ids):
    data = []
    for e_id in email_ids:
        _, response = imap_server.fetch(e_id, '(RFC822)')
        data.append(response[0][1])
    return data

get_emails(email_ids)
a = get_emails(ids[0:2])
a[0]
msg = email.message_from_string(a[0])
msg['subject']  
msg.get_payload()
payloads = msg.get_payload()
payloads = msg.get_payload()
ids = emails_from("gene@livefyre.com")
ids[-1]
msg = email.message_from_string(a[0])
a = get_emails(ids[:2:-1])
ids[:2:-1]
ids[-2:-1]
ids[-3:-1]
a = get_emails(ids[-3:-1])
a = get_emails(ids[-3:-1])
imap_server.select('INBOX')
imap_server.close()
imap_server = imaplib.IMAP4_SSL("imap.gmail.com",993)
imap_server.login(username, password)
imap_server.select('INBOX')
a = get_emails(ids[-3:-1])
a[0]
msg = email.message_from_string(a[0])
msg['subject']
p =msg.get_payload()
p
p[0]
_p = p[0]
_p._headers
_p.get_payload()
_ip.magic("history -n")
_ip.magic("history -n 100")
"""
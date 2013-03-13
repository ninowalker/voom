'''
Created on Mar 10, 2013

@author: nino
'''

from email.mime.multipart import MIMEMultipart
import uuid
import os
from email.mime.application import MIMEApplication
import pickle
import email

class MIMEMessageEncoder(object):
    def __init__(self, encoder=pickle.dumps):
        self.encoder = encoder
    
    def __call__(self, address, *messages):
        msg = MIMEMultipart()
        msg['Subject'] = uuid.uuid4().hex
        msg['From'] = "%s@%s" % (os.getpid(), os.uname()[1]) #### TODO need to configure the reply to address
        msg['To'] = address
        for message in messages:
            attach = MIMEApplication(self.encoder(message))
            msg.attach(attach)
        return msg.as_string()
    
    
class MIMEMessageDecoder(object):
    def __init__(self, decoder=pickle.loads):
        self.decoder = decoder
    
    def __call__(self, str_or_fp):
        if hasattr(str_or_fp, 'read'):
            msg = email.message_from_file(str_or_fp)
        else:
            msg = email.message_from_string(str_or_fp)
        return [self.decoder(attachment.get_payload(decode=True)) for attachment in msg.get_payload()], dict(msg.items())
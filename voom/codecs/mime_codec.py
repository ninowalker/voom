'''
Created on Mar 10, 2013

@author: nino
'''

from email.mime.multipart import MIMEMultipart
import email


class MIMEMessageCodec(object):
    def __init__(self, supported_types_registry):
        self.supported_types_registry = supported_types_registry

    def supported_types(self):
        return (object, type(None))
    
    def mimetypes(self):
        return ["multipart/mixed"]
        
    def encode(self, body, headers={}):
        msg = MIMEMultipart()
        for header in headers:
            msg[header] = headers[header]
        
        if not isinstance(body, list):
            body = [body]

        for message in body:
            msg.attach(self._encode_part(message))
        return msg.as_string()
    
    def _encode_part(self, msg, default_encoding="json"):
        codec = self.supported_types_registry.search(msg)
        if not codec:
            codec = self.supported_types_registry.search(default_encoding)
        return codec.encode_mime_part(msg)
            
    def decode(self, str_or_fp):
        if hasattr(str_or_fp, 'read'):
            msg = email.message_from_file(str_or_fp)
        else:
            msg = email.message_from_string(str_or_fp)
        return dict(msg.items()), [self._decode_part(part) for part in msg.get_payload()]
    
    def _decode_part(self, part):
        codec = self.supported_types_registry.get_mime_codec(part.get_content_type())
        return codec.decode_mime_part(part)

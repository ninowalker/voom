'''
Created on Mar 13, 2013

@author: nino
'''
from email.mime.application import MIMEApplication

try:
    import anyjson
    loads = anyjson.deserialize
    dumps = anyjson.serialize
except ImportError:
    import json
    loads = json.loads
    dumps = json.dumps
    
    
class JSONCodec(object):
    MIME_SUBTYPE = "json"
    
    def supported_types(self):
        return (dict, list, tuple, basestring, int, float, bool, type(None))
    
    def mimetypes(self):
        return ["application/" + self.MIME_SUBTYPE]
    
    def encode_mime_part(self, obj):
        return MIMEApplication(self.encode(obj), self.MIME_SUBTYPE)
    
    def decode_mime_part(self, part):
        return self.decode(part.get_payload(decode=True))
    
    def encode(self, obj):
        return dumps(obj)
        
    def decode(self, input):
        return loads(input)
    
    def decode_message(self, body):
        return {}, [self.decode(body)]
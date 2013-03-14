'''
Created on Mar 13, 2013

@author: nino
'''

import google.protobuf.message
import importlib
from email.mime.application import MIMEApplication


class ProtobufCodec(object):
    # https://groups.google.com/d/msg/protobuf/VAoJ-HtgpAI/mzWkRlIptBsJ
    MIME_SUBTYPE = "vnd.google.protobuf"
    
    def __init__(self, message_type=None):
        self.message_type = message_type
        self.registry = {}

    def supported_types(self):
        return (google.protobuf.message.Message,)
    
    def mimetypes(self):
        return ["application/" + self.MIME_SUBTYPE]
        
    def encode_mime_part(self, obj):
        return MIMEApplication(self.encode(obj), self.MIME_SUBTYPE, proto=obj.DESCRIPTOR.full_name)
    
    def decode_mime_part(self, part):
        payload = part.get_payload(decode=True)
        return self.decode(payload, part.get_param('proto'))
        
    def encode(self, obj, include_type=True):
        s = obj.SerializeToString()
        if not self.message_type or include_type:
            s = u"%s;%s" % (obj.DESCRIPTOR.full_name, s)
        return s
        
    def decode(self, input, message_type=None):
        """Decodes a protobuf message. Message type is derived from:
        1) the input parameter
        2) the instance's message_type attribute
        3) encoded in the input as 'messagetype;protobuf_raw_data'
        """
        pos = input.find(";")
        if pos == -1:
            message_type = message_type or self.message_type
            if not message_type:
                raise Exception("no message type")
            pos = 0
        else:
            message_type = input[0:pos]
            pos += 1
        
        klass = self.get_class(message_type)
        obj = klass()
        obj.ParseFromString(input[pos:])
        return obj
        
    def get_class(self, type):
        """Resolve a python class from the protobuf full_name"""
        if type in self.registry:
            return self.registry[type]
        
        mod_name, cls_name = type.rsplit('.', 1)
        mod_name += '_pb2'

        mod = importlib.import_module(mod_name)
        klass = getattr(mod, cls_name)
        return klass

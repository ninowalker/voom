from email.mime.application import MIMEApplication
from voom.codecs import TypeCodec
import google.protobuf.message


class ProtobufBinaryCodec(TypeCodec):
    # https://groups.google.com/d/msg/protobuf/VAoJ-HtgpAI/mzWkRlIptBsJ
    MIME_SUBTYPE = "vnd.google.protobuf"

    def __init__(self, message_type=None):
        self.message_type = message_type
        self.registry = {}

    def supported_types(self):
        return (google.protobuf.message.Message,)

    def mimetypes(self):
        return ["application/" + self.MIME_SUBTYPE]

    def encode(self, obj, include_type=True):
        s = obj.SerializeToString()
        if include_type:
            s = u"%s;%s" % (obj.DESCRIPTOR.full_name, s)
        return s

    def decode(self, value, message_type=None):
        """Decodes a protobuf message. Message type is derived from:
        1) the input parameter
        2) the instance's message_type attribute
        3) encoded in the input as 'messagetype;protobuf_raw_data'
        """

        if not message_type and ';' in value:
            message_type, value = value.split(';', 1)

        if not message_type:
            raise TypeError("no message type")

        klass = self.get_class(message_type)
        obj = klass()
        obj.ParseFromString(value)
        return obj

    def imp(self, mod_name, _from):
        return __import__(mod_name, globals(), locals(), [_from])

    def get_class(self, type):
        """Resolve a python class from the protobuf full_name"""
        if type in self.registry:
            return self.registry[type]

        mod_name, cls_name = type.rsplit('.', 1)
        mod_name += '_pb2'

        return getattr(self.imp(mod_name, cls_name), cls_name)


class MIMEProtobufBinaryCodec(ProtobufBinaryCodec):
    def encode_part(self, obj):
        return MIMEApplication(self.encode(obj, include_type=False), self.MIME_SUBTYPE, proto=obj.DESCRIPTOR.full_name)

    def decode_part(self, part):
        msg_type = part.get_param('proto')
        return self.decode(part.get_payload(decode=True), msg_type or self.message_type)

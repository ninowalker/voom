from email.mime.application import MIMEApplication
from voom.codecs import TypeCodec, MessageCodec

try:
    import anyjson

    loads = anyjson.deserialize
    dumps = anyjson.serialize
except ImportError:
    import json

    loads = json.loads
    dumps = json.dumps


class JSONCodec(TypeCodec):
    MIME_SUBTYPE = "json"

    def supported_types(self):
        return (dict, list, tuple, basestring, int, float, bool, type(None))

    def mimetypes(self):
        return ["application/" + self.MIME_SUBTYPE]

    def encode(self, obj):
        return dumps(obj)

    def decode(self, input_):
        return loads(input_)


class JSONMessageCodec(MessageCodec):
    supported = [JSONCodec()]

    def mimetypes(self):
        return ["application/json"]

    def encode_message(self, parts, headers):
        return dumps(dict(parts=parts, headers=headers))

    def decode_message(self, str_or_fp):
        if hasattr(str_or_fp, 'read'):
            msg = loads(str_or_fp.read())
        else:
            msg = loads(str_or_fp)
        return msg['headers'], msg['parts']


class MIMEJSONCodec(JSONCodec):
    def encode_part(self, obj):
        return MIMEApplication(self.encode(obj), self.MIME_SUBTYPE)

    def decode_part(self, part):
        return self.decode(part.get_payload(decode=True))

"""
message/vnd.livefyre+json
    Root: [] - a list of elements where the first element is a headers container, followed by a series of parts.
       Headers: {} - a dictionary of values global the the message.
           Key: string header name.
               Value: header value.
       Part: [] - an element where the first element is a headers container, followed by the body.
           Headers: {} - headers for this specific part; generally just encoding/type information.
               Key: string header name.
                   Value: header value.
           Body - an arbitrary object constituting the part, and serialized according to information
                  in the part's headers.
"""
from voom.codecs.json_codec import loads, dumps, JSONCodec, JSONMessageCodec
from voom.codecs.protobuf_codec import ProtobufBinaryCodec
from protobuf_to_dict import protobuf_to_dict, dict_to_protobuf

TRANSFER_ENCODING = 'Content-Transfer-Encoding'


class LivefyreJSONProtobufCodec(ProtobufBinaryCodec):
    MIME_TYPE = "application/vnd.livefyre.protobuf+json"
    SERIAL_FORMAT = "map+name"

    def mimetypes(self):
        return [self.MIME_TYPE]

    def encode_part(self, obj):
        d = {}
        d['Content-Type'] = '%s; proto="%s"; serial="%s"' % (self.MIME_TYPE,
                                                             obj.DESCRIPTOR.full_name,
                                                             self.SERIAL_FORMAT)
        d[TRANSFER_ENCODING] = 'inline'
        return d, protobuf_to_dict(obj)

    def decode_part(self, part):
        headers, content = part[0], part[1]
        content_type, params = self._parse_content_type(headers['Content-Type']) #@UnusedVariable
        if params['serial'] != self.SERIAL_FORMAT:
            raise TypeError("Unable to decode %s, of content_type='%s'" % (params['serial'], part['Content-Type']))
        klass = self.get_class(params['proto'])
        return dict_to_protobuf(klass, content, strict=False)

    def _parse_content_type(self, content_type):
        parts = [c.strip() for c in content_type.split(";")]
        content_type = parts.pop(0)
        params = dict(map(lambda s: s.split("="), parts))
        for k, v in params.iteritems():
            params[k] = v.strip("\"")
        return content_type, params


class InlineJSONCodec(JSONCodec):
    def encode_part(self, obj):
        return {'Content-Type': 'application/json', TRANSFER_ENCODING: 'inline'}, obj

    def decode_part(self, part):
        content = part[1]
        if part[0][TRANSFER_ENCODING] != 'inline':
            return loads(content)
        return content


class LivefyreJSONMessageCodec(JSONMessageCodec):
    TYPE = "message/vnd.livefyre+json"

    supported = [InlineJSONCodec(),
                 LivefyreJSONProtobufCodec()]

    def mimetypes(self):
        return [self.TYPE]

    def encode_message(self, parts, headers):
        headers = headers.copy()
        headers['Content-Type'] = self.TYPE
        _parts = [self._encode_part(part) for part in parts]
        return dumps([headers] + _parts)

    def decode_message(self, str_or_fp):
        if hasattr(str_or_fp, 'read'):
            msg = loads(str_or_fp.read())
        else:
            msg = loads(str_or_fp)
        headers = msg.pop(0)
        return headers, [self._decode_part(self._handle_encoding(part), part[0]['Content-Type']) for part in msg]

    def _handle_encoding(self, part):
        encoding = part[0].get(TRANSFER_ENCODING)
        if encoding and encoding != 'inline':
            headers, part = part
            return headers, part.decode(encoding)
        return part
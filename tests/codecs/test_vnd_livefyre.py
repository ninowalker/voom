from google.protobuf.descriptor_pb2 import FileOptions
from voom.codecs.vnd.livefyre import LivefyreJSONMessageCodec, InlineJSONCodec, \
    LivefyreJSONProtobufCodec
import json
import unittest


def register_types(lfjpc):
    lfjpc.registry['google.protobuf.FileOptions'] = FileOptions


class Test(unittest.TestCase):
    def _pp(self, obj):
        print json.dumps(obj, sort_keys=True,
                         indent=4, separators=(',', ': '))
        return obj

    def test_inline_json(self):
        c = InlineJSONCodec()
        p = c.encode_part(range(10))
        headers, part = p
        assert part == range(10)
        assert headers['Content-Transfer-Encoding'] == 'inline'
        assert headers['Content-Type'] == 'application/json'
        assert c.decode_part(p) == range(10)

        p = p[0], json.dumps(range(10))
        p[0]['Content-Transfer-Encoding'] = 'base64' # doesn't matter, as long as it's not inline
        assert c.decode_part(p) == range(10)

    def test_proto_json(self):
        c = LivefyreJSONProtobufCodec()
        register_types(c)
        f = FileOptions()
        f.java_package = "foo"
        f.java_outer_classname = "bar"

        pjs = c.encode_part(f)
        f2 = c.decode_part(pjs)

        assert f == f2, self._pp(pjs)

    def test_msg_codec(self):
        c = LivefyreJSONMessageCodec()
        pj = c.codecs_registry.get_by_content_type(LivefyreJSONProtobufCodec.MIME_TYPE)
        register_types(pj)

        f = FileOptions()
        f.java_package = "foo"
        f.java_outer_classname = "bar"

        m = c.encode_message([f, range(10)], {'meow': 1})
        headers, (f2, f3) = msg = c.decode_message(m)
        assert f == f2, (f, f2)
        assert headers == {'meow': 1, u'Content-Type': u'message/vnd.livefyre+json'}, headers
        self._pp(json.loads(m))

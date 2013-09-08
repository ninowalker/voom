import codecs


class ContentCodecRegistry(object):
    """A repository of codecs for parsing and unparsing MIME formats."""

    def __init__(self, *codecs):
        self.codecs = codecs
        self.type_to_codec = {}
        for codec in codecs:
            for type in codec.mimetypes():
                self.type_to_codec[type] = codec

    def search(self, obj, accepted=None):
        for c in self.codecs:
            if isinstance(obj, c.supported_types()):
                if not accepted:
                    return c
                return c
        return None

    def get_by_content_type(self, type_):
        type_ = type_.split(";")[0].strip()
        if type_ in self.type_to_codec:
            return self.type_to_codec[type_]

        type_ = type_.split("/", 1)[0] + "/*"
        return self.type_to_codec[type_]

    @property
    def supported(self):
        return self.type_to_codec.keys()


class MessageCodec(object):
    def __init__(self, supported=[]):
        supported = supported + getattr(self, 'supported', [])
        codecs_registry = ContentCodecRegistry(*supported)
        self.codecs_registry = codecs_registry

    def mimetypes(self):
        """Content types handled"""
        abstract  # @UndefinedVariable

    def encode_message(self, parts, headers):
        """Transforms a list of parts into a serialized string with 
        the associated headers.
        
        :param list parts: list of objects to be encoded
        :param dict headers: headers to include in the message
        """
        abstract  # @UndefinedVariable

    def decode_message(self, str_or_fp):
        """Converts a string/file object into a set of headers and decoded parts."""
        abstract  # @UndefinedVariable

    def _encode_part(self, part):
        codec = self.codecs_registry.search(part)
        return codec.encode_part(part)

    def _decode_part(self, part, content_type):
        codec = self.codecs_registry.get_by_content_type(content_type)
        return codec.decode_part(part)


class TypeCodec(object):
    def mimetypes(self):
        """Content types handled"""
        abstract  # @UndefinedVariable

    def encode_part(self, obj):
        abstract  # @UndefinedVariable

    def decode_part(self, part, content_type):
        abstract  # @UndefinedVariable


"""
Provides a simpole extensible way to add additional string codecs to the standard
codecs library.
"""
_STRING_CODECS = {}


def lookup(encoding):
    return _STRING_CODECS.get(encoding)


def register(name, codec_info):
    assert isinstance(codec_info, codecs.CodecInfo), "not an instance of codecs.CodecInfo"
    _STRING_CODECS[name] = codec_info


codecs.register(lookup)

register("gzip", codecs.CodecInfo(codecs.getencoder("zip"),
                                  codecs.getdecoder("zip")))



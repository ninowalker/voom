import codecs
import importlib
from voom.codecs.json_codec import JSONCodec


class ParseError(Exception):
    def __init__(self, message, cause):
        super(ParseError, self).__init__(message)
        self.cause = cause
        
        
class ContentCodecRegistry(object):
    """A repository of codecs for parsing and unparsing MIME formats."""
    def __init__(self, codecs):
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
            
    def get_by_content_type(self, type):
        if type in self.type_to_codec:
            return self.type_to_codec[type]
        
        type = type.split("/", 1)[0] + "/*"
        return self.type_to_codec[type]
    
    @property
    def supported(self):
        return self.type_to_codec.keys()


_STRING_CODECS = {}
        
def lookup(encoding):
    return _STRING_CODECS.get(encoding)

def register(name, codec_info):
    assert isinstance(codec_info, codecs.CodecInfo), "not an instance of codecs.CodecInfo"
    _STRING_CODECS[name] = codec_info

codecs.register(lookup)

register("gzip", codecs.CodecInfo(codecs.getencoder("zip"),
                                  codecs.getdecoder("zip")))



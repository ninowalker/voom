
class EncoderRegistry(object):
    """Provides a mapping between an encoding and the encoder."""
    def __init__(self):
        self.serializers = {None: lambda x: x}
        
    def register(self, name, serializer):
        self.serializers[name] = serializer
        
    def get(self, name):
        return name
        
    def __getitem__(self, name):
        return self.serializers[name]


class ParseError(Exception):
    def __init__(self, message, cause):
        super(ParseError, self).__init__(message)
        self.cause = cause

# celery://queue/function
# rabbitmq://username:password@host/vhost/key

class EncoderRegistry(object):
    """Provides a mapping between address schemes and a given transport."""
    def __init__(self):
        self.serializers = {None: lambda x: x}
        
    def register(self, name, serializer):
        self.serializers[name] = serializer
        
    def get(self, name):
        return name
        
    def __getitem__(self, name):
        return self.serializers[name]


# celery://queue/function
# rabbitmq://username:password@host/vhost/key
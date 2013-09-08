from pika.connection import URLParameters, Parameters
import urllib
import urlparse


class AMQPAddress(object):
    """
    amqp://host/vhost?routing_key=a_queue
    amqp://guest:guest@host:1234/vhost?routing_key=hello&exchange=_an_exchange
    """

    scheme = "amqp"
    params = None
    fragment = None

    def __init__(self, connection_params, **extras):
        assert isinstance(connection_params, Parameters)
        if connection_params.credentials.username:
            self.netloc = "%s:%s@%s" % (connection_params.credentials.username,
                                        connection_params.credentials.password,
                                        connection_params.host)
        else:
            self.netloc = connection_params.host
        if connection_params.port:
            self.netloc = "%s:%s" % (self.netloc, connection_params.port)

        if connection_params.virtual_host == '/':
            self.path = '/%2f'
        else:
            self.path = "/%s" % connection_params.virtual_host

        self.extras = extras

    def get(self, name, default=None):
        return self.extras.get(name, default)

    @classmethod
    def parse(cls, url):
        if not url.startswith("amqp:"):
            raise ValueError(url)
        obj = cls(URLParameters(url))

        if url[0:4] == 'amqp':
            url = 'http' + url[4:]
        parts = urlparse.urlparse(url)
        if parts.query:
            obj.extras.update({k: v for k, v in urlparse.parse_qsl(parts.query)})
        return obj

    def unparse(self, **params_):
        params = {}
        params.update(self.extras)
        params.update(params_)
        return urlparse.urlunparse((self.scheme, self.netloc, self.path,
                                    None, urllib.urlencode(params), None))

    def get_parameters(self):
        return URLParameters(self.unparse())
    
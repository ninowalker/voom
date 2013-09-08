from logging import getLogger
import os
import socket

LOG = getLogger(__name__)


class AMQPSender(object):
    def __init__(self, channel, supported_content_types, from_=None):
        self.channel = channel
        self.from_ = from_ or "%s@%s" % (os.getpid(), socket.getfqdn())
        self.supported_content_types = supported_content_types

    def send(self,
             message,
             properties,
             routing_key,
             exchange):
        LOG.debug("sending %s to routing_key=%s, exchange='%s'", message, routing_key, exchange)
        properties.headers = properties.headers or {}
        properties.headers['From'] = self.from_
        LOG.debug("send properties=%s", properties)

        encoder = self.supported_content_types.get_by_content_type(properties.content_type)
        body = encoder.encode_message(message, {})

        if properties.content_encoding:
            body = body.encode(properties.content_encoding)

        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=body,
                                   properties=properties)

    @classmethod
    def extract_headers(cls, properties):
        headers = {}
        for k, v in properties.__dict__.iteritems():
            if v is not None:
                headers[cls._header(k)] = v
        headers.pop('Headers', None)

        for k, v in (properties.headers or {}).iteritems():
            headers[k] = v

        return headers

    @classmethod
    def _header(cls, s):
        return "-".join(map(str.capitalize, s.split("_")))

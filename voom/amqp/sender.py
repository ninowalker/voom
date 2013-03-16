'''
Created on Mar 15, 2013

@author: nino
'''
from logging import getLogger
import pika
import uuid
import os
import time

LOG = getLogger(__name__)


class AMQPSender(object):
    def __init__(self, channel, supported_content_types, from_=None):
        self.channel = channel
        self.from_ = from_ or "%s@%s" % (os.getpid(), os.uname()[1])
        self.supported_content_types = supported_content_types
                
    def send(self, 
             message,
             properties,
             routing_key,
             exchange):
        LOG.debug("sending %s to routing_key=%s, exchange='%s'", message, routing_key, exchange)
        properties.headers['From'] = self.from_
        LOG.debug("send properties=%s", properties)

        headers = {}
        for k, v in properties.__dict__.iteritems():
            if v is not None:
                headers[self._header(k)] = v
        headers.pop('Headers', None)
        
        for k, v in (properties.headers or {}).iteritems():
            headers[k] = v

        encoder = self.supported_content_types.get_mime_codec(properties.content_type)
        body = encoder.encode(message, headers)
        
        if properties.content_encoding:
            body = body.encode(properties.content_encoding)
                
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=body,
                                   properties=properties)
        LOG.warning("sent %s", body)

    def _header(self, s):
        return "-".join(map(str.capitalize, s.split("_")))

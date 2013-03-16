'''
Created on Mar 11, 2013

@author: nino
'''
from voom.context import SessionKeys
from logging import getLogger
import pika #@UnusedImport
import functools
from collections import namedtuple, defaultdict
from voom.gateway import GatewayShutdownCmd, AMQPConnectionReady, \
    AMQPQueueInitialized, AMQPQueueDescriptor, AMQPChannelReady, \
    AMQPMessageExtras, AMQPDataReceived
from voom.priorities import BusPriority
import socket
import threading
from voom.channels.amqp import AMQPAddress
import os
import uuid
import urlparse
from voom.amqp.config import AMQPInitializer, AMQPConfigSpec, \
    AMQPConsumerDescriptor
import time
from voom.amqp.sender import AMQPSender

LOG = getLogger(__name__)


class AMQPGateway(object):
    def __init__(self,
                 app_name,
                 connection_params,
                 queues,
                 event_bus,
                 supported_content_types,
                 exchanges=None,
                 bindings=None,
                 consumer_params={},
                 accept="multipart/mixed",
                 accept_encoding="gzip"):
        self.bus = event_bus
        self.attach(self.bus)
        self.app_id = app_name
        
        return_queue = AMQPQueueDescriptor("%s@%s" % (app_name, socket.getfqdn()),
                                           declare=True,
                                           durable=False,
                                           exclusive=False,
                                           auto_delete=False)
        
        queues = list(queues)
        queues.append(return_queue)

        consumers = []
        for queue in queues:
            consumers.append(AMQPConsumerDescriptor(self.on_receive,
                                                    queue.queue, **consumer_params))

        # setup the sender                                           
        self.address = AMQPAddress(connection_params,
                                   routing_key=return_queue.queue,
                                   accept=accept,
                                   accept_encoding=accept_encoding,
                                   app_id=app_name)        
        self.sender = AMQPSender(return_queue, self.address, self.bus, supported_content_types)
        self.spec = AMQPConfigSpec(connection_params,
                                   queues=queues,
                                   exchanges=exchanges,
                                   bindings=bindings,
                           consumers=[]
                           )

        
    def run(self):
        LOG.info("Gateway starting...")
        init = AMQPInitializer(self.spec, self.on_complete)
        init.run()
        
    def shutdown(self, *args):
        LOG.warning("Gateway shutdown")
        self.connection.close()
        self.connection.ioloop.stop()
    
    def attach(self, bus):
        bus.subscribe(GatewayShutdownCmd, self.shutdown, BusPriority.LOW_PRIORITY)
        
    def on_complete(self, connection, channel):
        self.sender = AMQPSender(channel, self.supported_content_types)
    
    def reply(self, properties, message, **kwargs):
        headers = properties.headers or {}
        _properties = pika.BasicProperties(delivery_mode=2,
                                           correlation_id=properties.correlation_id or uuid.uuid4().hex,
                                           app_id=self.app_name,
                                           reply_to=self.return_queue.queue,
                                           user_id=None,
                                           timestamp=time.time(),
                                           content_type=properties.content_type,
                                           content_encoding=properties.content_encoding,
                                           headers={'Session-Id': headers.get('Session-Id'),
                                                    'Accept': self.supported_content_types.keys(),
                                                    'Accept-Encoding': self.supported_content_encodings.keys()})
        extra_headers = kwargs.pop('headers', {})
        _properties.headers.update(extra_headers)
        
        for k, v in kwargs.iteritems():
            if hasattr(_properties, k):
                setattr(_properties, k, v)
        
        self.sender.send(message,
                         _properties,
                         routing_key=properties.reply_to,
                         exchange='',
                         accept=properties.content_type,
                         accept_encoding=properties.content_encoding)
    
    def on_receive(self, event):
        properties = event.properties
        codec = self.supported.get_mime_codec(properties.content_type)
        headers, msgs = codec.decode(event.body)
        
        context = {SessionKeys.GATEWAY_HEADERS: headers,
                   SessionKeys.GATEWAY_EXTRAS: event.extras,
                   }
        
        if properties.reply_to:
            parts = urlparse.urlparse(properties.reply_to)
            if parts.scheme:
                context[SessionKeys.REPLY_TO] = parts.reply_to
            else:
                context[SessionKeys.RESPONDER] = functools.partial(self.reply, properties)
        
        self.bus.send(event, context.copy())
        
        for msg in msgs:
            self.bus.send(msg, context.copy())
    
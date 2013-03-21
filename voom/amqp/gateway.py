'''
Created on Mar 11, 2013

@author: nino
'''
from logging import getLogger
import pika 
from voom.gateway import GatewayShutdownCmd, GatewayMessageDecodeError
from voom.priorities import BusPriority
import socket
import os
import uuid
from voom.amqp.config import AMQPInitializer, AMQPConfigSpec, \
    AMQPConsumerDescriptor, AMQPQueueDescriptor
import time
from voom.amqp.sender import AMQPSender
from voom.amqp.events import AMQPGatewayReady, AMQPSenderReady, AMQPDataReceived,\
    AMQPRawDataReceived
import sys
from voom.context import SessionKeys
import urlparse

LOG = getLogger(__name__)


class AMQPGateway(object):
    def __init__(self,
                 app_name,
                 connection_params,
                 queues,
                 event_bus,
                 message_codecs,
                 exchanges=None,
                 bindings=None,
                 consumer_params={}):
        
        self.app_id = app_name
        self.return_queue = AMQPQueueDescriptor("%s.%s@%s" % (app_name, os.getpid(), socket.getfqdn()),
                                                declare=True, durable=False, exclusive=False, auto_delete=True)
        self.bus = event_bus
        self.attach(self.bus)
        self.message_codecs = message_codecs
        # we set these up on_complete
        self.sender = None 
        self.connection = None
        self.channel = None
        
        # define the consumers: one per queue
        queues = list(queues)
        queues.append(self.return_queue)

        consumers = []
        for queue in queues:
            consumers.append(AMQPConsumerDescriptor(queue.queue, 
                                                    self.on_receive,
                                                    **consumer_params))

        #self.address = AMQPAddress(connection_params,
        #                           routing_key=return_queue.queue,
        #                           accept=accept,
        #                           accept_encoding=accept_encoding,
        #                           app_id=app_name)        
        self.spec = AMQPConfigSpec(connection_params,
                                   queues=queues,
                                   exchanges=exchanges,
                                   bindings=bindings,
                                   consumers=consumers
                                   )

        
    def run(self):
        """Starts all of the machinery for consuming and replying to messages sent to
        the work queues. This call will not exit until the gateway is shutdown or an uncaught
        exception propagates.
        """
        LOG.info("AMQPGateway starting...")
        init = AMQPInitializer(self.spec, self._on_complete)
        init.run()
        LOG.warning("Gateway exited")
        
    def shutdown(self, msg=None):
        """Stops consumption and will cause `run()` to exit."""
        LOG.warning("Gateway shutdown")
        self.connection.close()
        self.connection.ioloop.stop()
    
    def attach(self, bus):
        """Binds event handlers to the bus."""
        bus.subscribe(GatewayShutdownCmd, self.shutdown, BusPriority.LOW_PRIORITY)
        
    def reply(self, properties, message, **kwargs):
        """"""
        headers = properties.headers or {}
        _properties = pika.BasicProperties(delivery_mode=2,
                                           correlation_id=properties.correlation_id or uuid.uuid4().hex,
                                           app_id=self.app_id,
                                           reply_to=self.return_queue.queue,
                                           user_id=None,
                                           timestamp=time.time(),
                                           content_type=properties.content_type,
                                           content_encoding=properties.content_encoding,
                                           headers={'Session-Id': headers.get('Session-Id'),
                                                    'Accept': ", ".join(self.message_codecs.supported),
                                                    'Accept-Encoding': ['zip']})
        extra_headers = kwargs.pop('headers', {})
        _properties.headers.update(extra_headers)
        
        for k, v in kwargs.iteritems():
            if hasattr(_properties, k):
                setattr(_properties, k, v)
        
        self.send(message,
                  _properties,
                  routing_key=properties.reply_to,
                  exchange='')
        
    def send(self, message, properties, **kwargs):
        self.sender.send(message, properties, **kwargs)
    
    def on_receive(self, event):
        """Handle data received on a channel."""
        assert isinstance(event, AMQPRawDataReceived)
        properties = event.properties
        assert isinstance(properties, pika.BasicProperties)

        codec = self.message_codecs.get_by_content_type(properties.content_type)
        
        headers = AMQPSender.extract_headers(properties)
        print event.method
        headers['Routing-Key'] = event.method.routing_key
        headers['Exchange'] = event.method.exchange
        
        context = {SessionKeys.GATEWAY_HEADERS: headers,
                   SessionKeys.GATEWAY_EVENT: event}
        
        if properties.reply_to:
            context[SessionKeys.REPLY_TO] = properties.reply_to
            parts = urlparse.urlparse(properties.reply_to)
            if not parts.scheme:
                context[SessionKeys.RESPONDER] = lambda addr, message: self.reply(properties, message)#functools.partial(self.reply, properties)
        
        try:
            body = event.body
            if properties.content_encoding:
                body = body.decode(properties.content_encoding)
            _headers, msgs = codec.decode_message(body)
        except Exception, e:
            LOG.exception("failed to handle message: %s", body)
            self.bus.publish(GatewayMessageDecodeError(event, e, sys.exc_info()[2]), context)
            return 
                
        # send for auditing purposes        
        self.bus.publish(AMQPDataReceived(msgs, headers, body, event), context)

    def _on_complete(self, spec, connection, channel):
        """Called by the `AMQPInitializer` after all queues, exchanges, and bindings are configured."""
        # remember objects:
        self.connection = connection
        self.channel = channel
        # create a sender:
        self.sender = AMQPSender(channel, self.message_codecs, from_=self.return_queue.queue)
        self.bus.publish(AMQPSenderReady(self.sender))
        # let everybody know we're done.        
        self.bus.publish(AMQPGatewayReady(self))
        

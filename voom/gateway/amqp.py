'''
Created on Mar 11, 2013

@author: nino
'''
from voom.context import SessionKeys
from logging import getLogger
import pika #@UnusedImport
import functools
from collections import namedtuple
from voom.gateway import GatewayShutdownCmd, AMQPConnectionReady, \
    AMQPQueueInitialized, AMQPQueueDescriptor, AMQPChannelReady,\
    AMQPMessageExtras
from voom.priorities import BusPriority
import socket
import urllib
import threading
from voom.channels.amqp import AMQPAddress
import os
import uuid

LOG = getLogger(__name__)

AMQPListenerReady = namedtuple("AMQPListenerReady", "listener queue_descriptor")
AMQPListenerShutdown = namedtuple("AMQPListenerShutdown", "listener")
AMQPSenderReady = namedtuple("AMQPSenderReady", "sender queue")


class AMQPGateway(object):
    def __init__(self,
                 app_name,
                 connection_params,
                 work_queues,
                 event_bus,
                 on_receive,
                 supported_content_types,
                 accept="multipart/mixed",
                 accept_encoding="gzip"):
        self.bus = event_bus
        self.connection_params = connection_params

        return_queue = AMQPQueueDescriptor("%s@%s" % (app_name, socket.getfqdn()),
                                           declare=True,
                                           durable=False,
                                           exclusive=False,
                                           auto_delete=False)
        
        self.listener = AMQPQueueListener(work_queues + [return_queue], self.bus, on_receive)

        # setup the sender                                           
        self.address = AMQPAddress(connection_params,
                                    routing_key=return_queue.queue,
                                    accept=accept,
                                    accept_encoding=accept_encoding,
                                    app_id=app_name)        
        self.sender = AMQPSender(return_queue, self.address, self.bus, supported_content_types)
        self.attach(self.bus)
        
    def run(self):
        LOG.info("Gateway starting...")
        self.connection = pika.SelectConnection(self.connection_params, self._opened)
        self.connection.ioloop.start()
        
    def shutdown(self, *args):
        LOG.warning("Gateway shutdown")
        self.connection.close()
        self.connection.ioloop.stop()
    
    def attach(self, bus):
        bus.subscribe(GatewayShutdownCmd, self.shutdown, BusPriority.LOW_PRIORITY)
    
    def _opened(self, connection):
        self.bus.send(AMQPConnectionReady(connection))


class AMQPSender(object):
    def __init__(self, return_queue, reply_to_addr, event_bus,
                 supported_content_types,
                 default_content_type="multipart/mixed",
                 default_encoding="gzip"):
        self.channel = None
        self.connection = None
        self.bus = event_bus
        self.return_queue = return_queue
        self.reply_to_addr = reply_to_addr
        self.supported_content_types = supported_content_types
        self.default_encoding = default_encoding
        self.default_content_type = default_content_type
        self.attach(self.bus)
        
    def attach(self, bus):
        bus.subscribe(AMQPConnectionReady, self.connect)
        bus.subscribe(GatewayShutdownCmd, self.shutdown)
        
    def send(self, address, message, **kwargs):
        LOG.warning("sending %s to %s", message, address)
        properties = pika.BasicProperties(delivery_mode=2,
                                          reply_to=self.reply_to_addr.get('routing_key'))
        extras = self.reply_to_addr.extras.copy()    
        extras.update(kwargs)
        
        for k, v in extras.iteritems():
            if hasattr(properties, k):
                setattr(properties, k, v)
                
        if not properties.correlation_id:
            properties.correlation_id = uuid.uuid4().hex
        
        properties.content_type = properties.content_type or extras['accept']
        properties.content_encoding = properties.content_encoding or extras['accept_encoding']
        
        LOG.warning("properties=%s", properties)
        
        headers = {'From': "%s@%s" % (os.getpid(), os.uname()[1]),
                   'To': properties.reply_to,
                   'Reply-To-FQCS': self.reply_to_addr.unparse()}
        headers.update({_header(k): unicode(v) for k, v in properties.__dict__.iteritems() if v is not None})
        
        encoder = self.supported_content_types.get_mime_codec(properties.content_type)
        body = encoder.encode(message, headers)
                
        self.channel.basic_publish(exchange=address.get('exchange', ''),
                                   routing_key=address.get('routing_key', ''),
                                   body=body,
                                   properties=properties)
        LOG.warning("sent %s", body)

    def connect(self, connected_event):
        """Open the connection, and start putting messages on the bus."""
        LOG.warning("connection opened")
        self.connection = connected_event.connection
        self.connection.channel(self._init_channel)
        
    def shutdown(self, *args):
        self.channel.close()
        
    def _init_channel(self, channel):
        LOG.warning("channel open")
        self.channel = channel        
        callback = functools.partial(self._init_declared, AMQPQueueInitialized(self.return_queue))
        _declare_queue(self.channel, self.return_queue, callback)
        self.bus.send(AMQPChannelReady(self.channel, self.connection, threading.current_thread()))
        
    def _init_declared(self, obj, *args):
        LOG.warning("created %s", obj.descriptor.queue)
        LOG.warning("sender initialized.")
        self.bus.send(AMQPSenderReady(self, obj.descriptor.queue))
        

class AMQPQueueListener(object):
    def __init__(self, queues, event_bus, callback=None):
        self.bus = event_bus
        self.callback = callback or self._default_callback
        self.connection = None
        self._queues = queues
        
        self.attach(self.bus)
        
    def connect(self, connected_event):
        """Open the connection, and start putting messages on the bus."""
        LOG.info("connection opened")
        self.connection = connected_event.connection
        self.connection.channel(self._init_channel)
        
    def stop_consuming(self):
        """Stop consuming messages. The AMQPListener should be disposed after this."""
        self.channel.close()
        self.bus.send(AMQPListenerShutdown(self))
                
    def reject_current(self, requeue=False, **kwargs):
        self.channel.basic_nack(requeue=requeue)

    def shutdown(self, msg=None):
        self.stop_consuming()
        
    def attach(self, bus):
        # we bind to these events to allow reference-less invocation from anywhere
        bus.register(self.shutdown, receiver_of=(GatewayShutdownCmd,))
        bus.register(self.connect, receiver_of=(AMQPConnectionReady,))

    def _init_channel(self, obj):
        LOG.info("channel opened")
        self.channel = obj
        self.channel.basic_qos(prefetch_count=1)        
        
        for desc in self._queues:
            callback = functools.partial(self._init_declared, AMQPQueueInitialized(desc))
            _declare_queue(self.channel, desc, callback)
                

    def _init_declared(self, obj, *args):
        self.channel.basic_consume(self._on_receive, queue=obj.descriptor.queue)
        LOG.info("subscribed to %s", obj.descriptor.queue)
        self.bus.send(obj)
        self.bus.send(AMQPListenerReady(self, obj.descriptor))

    def _on_receive(self, ch, method, properties, body):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)        
        LOG.info('received payload on channel %s %s, %s', method.routing_key, properties, body)
        self.callback(self, body, AMQPMessageExtras(ch, method, properties))
        
    def _default_callback(self, unused, body, extras):
        context = {SessionKeys.GATEWAY_HEADERS: {},
                   SessionKeys.GATEWAY_EXTRAS: extras}

        #try:
        #    headers, messages = self.parser(body)
        #except ParseError:
        #    self.bus.send(AMQPListenerMessageUnparseable(body), context)
        #    return
        #context[SessionKeys.AMQPListener_HEADERS] = headers
        self.bus.send(body, context)
        
def _declare_queue(channel, descriptor, callback):
    if descriptor.declare:
        channel.queue_declare(queue=descriptor.queue,
                                   callback=callback,
                                   **descriptor.declare_params)
    else:
        channel.queue_declare(queue=descriptor.queue, passive=True, callback=callback)


def _header(s):
    return "-".join(map(str.capitalize, s.split("_")))
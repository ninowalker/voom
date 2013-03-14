'''
Created on Mar 11, 2013

@author: nino
'''
from voom.context import SessionKeys
from logging import getLogger
import pika #@UnusedImport
import functools
from collections import namedtuple
from voom.gateway import GatewayShutdownCmd, AMQPConnectionReady,\
    AMQPQueueInitialized
from voom.priorities import BusPriority
import socket
import urllib

LOG = getLogger(__name__)

AMQPListenerReady = namedtuple("AMQPListenerReady", "listener queue")
AMQPListenerShutdown = namedtuple("AMQPListenerShutdown", "listener")
AMQPSenderReady = namedtuple("AMQPSenderReady", "sender queue")


class AMQPGateway(object):
    def __init__(self, app_name, connection_params, work_queue, event_bus, on_receive,
                 accept="multipart/mixed",
                 accept_encoding="gzip"):
        self.bus = event_bus
        self.connection_params = connection_params
        return_queue = "%s@%s" % (app_name, socket.getfqdn())
        reply_to_addr = "amqp:///?%s" % urllib.urlencode(dict(routing_key=return_queue,
                                                              content_type=accept,
                                                              content_encoding=accept_encoding,
                                                              app_id=app_name))
        self.listener = AMQPQueueListener([work_queue, return_queue], self.bus, on_receive)
        self.sender = AMQPSender(return_queue, reply_to_addr, self.bus)
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
                 default_content_type="multipart/mixed", 
                 default_encoding="gzip"):
        self.channel = None
        self.connection = None
        self.bus = event_bus
        self.return_queue = return_queue
        self.reply_to_addr = reply_to_addr
        self.default_encoding = default_encoding
        self.default_content_type = default_content_type
        self.attach(self.bus)
        
    def attach(self, bus):
        bus.subscribe(AMQPConnectionReady, self.connect)
        bus.subscribe(GatewayShutdownCmd, self.shutdown)
        
    def send(self, message, routing_key='', exchange='', properties=None, **kwargs):
        LOG.warning("sending %s", message)
        if not properties:
            properties = pika.BasicProperties(delivery_mode=2,
                                              reply_to = self.return_queue)
        for k, v in kwargs.iteritems():
            if hasattr(properties, k):
                setattr(properties, k, v)
        
        properties.content_type = properties.content_type or self.default_content_type
        properties.content_encoding = properties.content_encoding or self.default_encoding
        
        #properties.content_type, body = codecs.encode((message, properties.content_type, properties.content_encoding)
        body = message
                
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
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
        
        self.channel.queue_declare(queue=self.return_queue,
                           exclusive=False, auto_delete=False,
                           callback=functools.partial(self._init_declared, 
                                                      AMQPQueueInitialized(self.return_queue)))
    def _init_declared(self, obj, *args):
        LOG.warning("created %s", obj.queue)
        LOG.warning("sender initialized.")
        self.bus.send(AMQPSenderReady(self, obj.queue))
        

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
        
        for queue in self._queues:
            self.channel.queue_declare(queue=queue,
                                       exclusive=False, auto_delete=False,
                                       callback=functools.partial(self._init_declared, 
                                                                  AMQPQueueInitialized(queue)))

    def _init_declared(self, obj, *args):
        self.channel.basic_consume(self._on_receive, queue=obj.queue)
        LOG.info("subscribed to %s", obj.queue)
        self._initialized = True
        self.bus.send(AMQPListenerReady(self, obj.queue))

    def _on_receive(self, ch, method, properties, body):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)        
        LOG.info('received payload on channel %s %s, %s', method.routing_key, properties, body)
        self.callback(self, body, dict(ch=ch, method=method, properties=properties))
        
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
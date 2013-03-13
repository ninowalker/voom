'''
Created on Mar 11, 2013

@author: nino
'''
from voom.context import SessionKeys
from logging import getLogger
from voom.gateway import GatewayStarted, GatewayShutdownCmd, \
    GatewayShutdown, GatewayConnectionInitialized
import pika
import functools

LOG = getLogger(__name__)


class AMQPGateway(object):
      
    def __init__(self, queues, event_bus, callback=None, connection_params=None, connection=None):
        self.bus = event_bus
        self.callback = callback or self._default_callback
        self._connection_params = connection_params
        self.connection = connection
        self._queues = queues
        self._initialized = False
        
        # we bind to these events to allow reference-less invocation from anywhere
        self.bus.register(self.shutdown, receiver_of=(GatewayShutdownCmd,))

    def bind(self):
        """Open the connection, and start putting messages on the bus."""
        if not self.connection:
            self.connection = pika.SelectConnection(self._connection_params, self._init_connection)
            self.connection.ioloop.start()
        
    def stop_consuming(self):
        """Stop consuming messages. The gateway should be disposed after this."""
        self.channel.close()
        self.connection.ioloop.stop()
        self.connection.close()
        self.bus.send(GatewayShutdown(self))
                
    def reject_current(self, requeue=False, **kwargs):
        self.channel.basic_nack(requeue=requeue)

    def shutdown(self, msg=None):
        self.stop_consuming()
        
    def _init_connection(self, obj):
        LOG.info("connection opened")
        self.connection = obj
        self.connection.channel(self._init_channel)

    def _init_channel(self, obj, *args):
        LOG.info("channel opened")
        self.channel = obj
        self.channel.basic_qos(prefetch_count=1)        
        
        for queue in self._queues:
            self.channel.queue_declare(queue=queue,
                                       exclusive=False, auto_delete=False,
                                       callback=functools.partial(self._init_declared, GatewayConnectionInitialized(queue)))

    def _init_declared(self, obj, *args):
        self.channel.basic_consume(self._on_receive, queue=obj.queue)
        LOG.info("subscribed to %s", obj.queue)
        self._initialized = True
        self.bus.send(GatewayStarted(self, obj.queue))

    def _on_receive(self, ch, method, properties, body):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)        
        LOG.info('received payload on channel %s %s, %s', method.routing_key, properties, body)
        self.callback(self, body, dict(ch=ch, method=method, properties=properties))
        
        
    def _default_callback(self, unused, body, extras):
        context = {SessionKeys.GATEWAY_HEADERS: {},
                   SessionKeys.GATEWAY: self,
                   SessionKeys.GATEWAY_EXTRAS: extras}

        #try:
        #    headers, messages = self.parser(body)
        #except ParseError:
        #    self.bus.send(GatewayMessageUnparseable(body), context)
        #    return
        #context[SessionKeys.GATEWAY_HEADERS] = headers
        self.bus.send(body, context)
from logging import getLogger
from voom.amqp.config import AMQPInitializer, AMQPConfigSpec, \
    AMQPConsumerDescriptor, AMQPQueueDescriptor, AMQPBindDescriptor
from voom.amqp.events import AMQPGatewayReady, AMQPSenderReady, AMQPDataReceived, \
    AMQPRawDataReceived
from voom.amqp.headers import Headers
from voom.amqp.sender import AMQPSender
from voom.context import SessionKeys
from voom.gateway import GatewayShutdownCmd, GatewayMessageDecodeError
from voom.priorities import BusPriority
import functools
import os
import pika
import socket
import sys
import threading
import time
import urlparse
import uuid

LOG = getLogger(__name__)


class AMQPGateway(object):
    """A Gateway."""

    MY_QUEUE = object()
    """An identifier used as the `queue` value of an `AMQPBindDescriptor`
    that causes it to be mapped to the return queue of an instantiated
    gateway."""

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
        self._ioloop_thread = threading.current_thread()

        # define the consumers: one per queue
        queues = list(queues)
        queues.append(self.return_queue)
        for i, binding in enumerate(bindings or []):
            if binding.queue == self.MY_QUEUE:
                bindings[i] = AMQPBindDescriptor(self.return_queue.queue,
                                                 binding.exchange,
                                                 **binding.bind_params)

        consumers = []
        for queue in queues:
            consumers.append(AMQPConsumerDescriptor(queue.queue,
                                                    self.on_receive,
                                                    **consumer_params))

        # self.address = AMQPAddress(connection_params,
        #                           routing_key=return_queue.queue,
        #                           accept=accept,
        #                           accept_encoding=accept_encoding,
        #                           app_id=app_name)
        self.spec = AMQPConfigSpec(connection_params,
                                   queues=queues,
                                   exchanges=exchanges,
                                   bindings=bindings,
                                   consumers=consumers)

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
        headers = Headers(properties.headers)
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
        """Sends a message using the active sender. Because the sender is 
        running on an IOLoop owned by the connection, we need to queue onto
        the IOLoop if we are in a different thread.
        
        :param message: the message to send. Serialization will be handled by
           the sender.
        :param properties: an instance of `pika.BasicProperties`
        """
        cthread = threading.current_thread()
        if self._ioloop_thread != cthread:
            self.connection.ioloop.add_timeout(0,
                                               functools.partial(self.send, message, properties, sending_thread=cthread,
                                                                 **kwargs))
            LOG.info("send called from thread %r" % cthread)
            return
        sending_thread = kwargs.pop('sending_thread', None)
        if sending_thread:
            LOG.info("sending message queued from thread %r" % sending_thread)
        self.sender.send(message, properties, **kwargs)

    def on_receive(self, event):
        """Handle data received on a channel."""
        assert isinstance(event, AMQPRawDataReceived)
        properties = event.properties
        assert isinstance(properties, pika.BasicProperties)

        codec = self.message_codecs.get_by_content_type(properties.content_type)

        headers = AMQPSender.extract_headers(properties)
        headers['Routing-Key'] = event.method.routing_key
        headers['Exchange'] = event.method.exchange

        context = {SessionKeys.GATEWAY_HEADERS: headers,
                   SessionKeys.GATEWAY_EVENT: event}

        if properties.reply_to:
            context[SessionKeys.REPLY_TO] = properties.reply_to
            parts = urlparse.urlparse(properties.reply_to)
            if not parts.scheme:
                context[SessionKeys.RESPONDER] = lambda addr, message: self.reply(properties,
                                                                                  message) # functools.partial(self.reply, properties)

        try:
            body = event.body
            if properties.content_encoding:
                body = body.decode(properties.content_encoding)
            _headers, msgs = codec.decode_message(body)
        except Exception, e:
            LOG.exception("failed to handle message: %s", body)
            with self.bus.using(context):
                self.bus.publish(GatewayMessageDecodeError(event, e, sys.exc_info()[2]))
            return

        # send for auditing purposes
        with self.bus.using(context):
            self.bus.publish(AMQPDataReceived(msgs, headers, body, event))

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


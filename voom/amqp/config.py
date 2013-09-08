from collections import namedtuple
from logging import getLogger
from voom.amqp.events import AMQPConnectionReady, AMQPChannelReady, \
    AMQPExchangeInitialized, AMQPQueueInitialized, AMQPQueueBound, \
    AMQPConsumerStarted, AMQPRawDataReceived
import functools
import pika

LOG = getLogger(__name__)


class AMQPConfigSpec(namedtuple('AMQPConfigSpec', 'connection_params queues exchanges bindings consumers connection_type')):
    """A data structure that composes descriptors of the things that need to be configured
    before starting work.

    :param connection_params: a parameters object for instantiating the connection.
    :type connection_params: an instance/subtype of `pika.spec.Parameters`
    :param queues: a list of queues to declare
    :type queues: iterable of `AMQPQueueDescriptor` objects or None
    :param exchanges: a list of exchanges to declare
    :type exchanges: iterable of `AMQPExchangeDescriptor` objects or None
    :param bindings: a list of bindings to declare
    :type bindings: iterable of `AMQPBindDescriptor` objects or None
    :param consumers: a list of consumers to start
    :type consumers: iterable of `AMQPConsumerDescriptor` objects or None
    :param type connection_type: a non blocking connection type
    """
    def __new__(cls, connection_params, queues=None, exchanges=None, bindings=None, consumers=None, connection_type=pika.SelectConnection):
        return super(AMQPConfigSpec, cls).__new__(cls, connection_params, queues, exchanges, bindings, consumers, connection_type)


class AMQPQueueDescriptor(namedtuple('AMQPQueueDescriptor', 'queue declare declare_params')):
    """Describes a queue to be checked for existence (if declare=False), or
    created with the given parameters.

    :param queue: the queue to declare
    :type queue: str or unicode
    :param bool declare: declare or (not) check for existence
    :type queue: str or unicode
    :param dict declare_params: arguments passed through to `pika.channel.Channel.queue_declare`

    See `pika.channel.Channel.queue_declare` for valid kwargs.
    """
    def __new__(cls, queue, declare=False, **kwargs):
        return super(AMQPQueueDescriptor, cls).__new__(cls, queue, declare, kwargs)


class AMQPExchangeDescriptor(namedtuple('AMQPExchangeDescriptor', 'declare declare_params')):
    """Describes an exchange to be checked for existence (if declare=False), or
    created with the given parameters.

    :param bool declare: declare or (not) check for existence
    :param dict declare_params: arguments passed through to `pika.channel.Channel.exchange_declare`

    See `pika.channel.Channel.exchange_declare` for valid kwargs.
    """
    def __new__(cls, declare=True, **kwargs):
        return super(AMQPExchangeDescriptor, cls).__new__(cls, declare, kwargs)


class AMQPBindDescriptor(namedtuple('AMQPBindDescriptor', 'queue exchange bind_params')):
    """Describes an exchange to queue binding to be created with the given parameters.

    :param queue: the queue to bind to
    :type queue: str or unicode
    :param exchange: the exchange to bind
    :type exchange: str or unicode
    :param dict declare_params: arguments passed through to `pika.channel.Channel.queue_bind`

    See `pika.channel.Channel.queue_bind` for valid kwargs.
    """
    def __new__(cls, queue=None, exchange=None, **bind_params):
        return super(AMQPBindDescriptor, cls).__new__(cls, queue, exchange, bind_params)


class AMQPConsumerDescriptor(namedtuple('AMQPQueueConsumer', 'queue on_receive consume_params')):
    """Describes a queue to callback relation to start after all other elements have been
    initialized.

    :param queue: the queue to consume from.
    :type queue: str or unicode
    :param method on_receive: function receiving `AMQPDataReceived` events.
    :param dict consume_params: kwargs passed through to `pika.channel.Channel.basic_consume`

    See `pika.channel.Channel.basic_consume` for valid kwargs.
    """
    def __new__(cls, queue, on_receive, **consume_params):
        return super(AMQPConsumerDescriptor, cls).__new__(cls, queue, on_receive, consume_params)


class AMQPInitializer(object):
    def __init__(self, spec, on_complete, on_event=None):
        self.spec = spec
        self.on_event = on_event or self._on_event
        self.on_complete = on_complete
        self.connection = None
        self.channel = None
        self.waiting = set([])

    def run(self):
        LOG.info("AMQP starting...")
        connection = self.spec.connection_type(self.spec.connection_params, self._init_connection)
        connection.ioloop.start()

    def _init_connection(self, connection):
        """Invoked when the connection is opened."""
        self.connection = connection
        self.on_event(AMQPConnectionReady(connection))
        connection.channel(self._init_channel)

    def _init_channel(self, channel):
        """Invoked when a channel is opened."""
        self.channel = channel
        self.on_event(AMQPChannelReady(channel, self.connection))

        # we declare these two first, then bind queues after
        self._declare_exchanges()
        self._declare_queues()
        if not self.waiting:
            self._bind_queues()

    def _declare_exchanges(self):
        """Declares all of the exchanges."""
        channel = self.channel
        for exchange in (self.spec.exchanges or []):
            assert isinstance(exchange, AMQPExchangeDescriptor)
            event = AMQPExchangeInitialized(exchange)
            self.waiting.add(event)
            callback = functools.partial(self._queue_or_exchange_declared, event)
            if exchange.declare:
                channel.exchange_declare(callback=callback, **exchange.declare_params)
            else:
                channel.exchange_declare(callback=callback, passive=True, **exchange.declare_params)

    def _declare_queues(self):
        channel = self.channel
        for descriptor in (self.spec.queues or []):
            event = AMQPQueueInitialized(descriptor)
            self.waiting.add(event)
            callback = functools.partial(self._queue_or_exchange_declared, event)
            if descriptor.declare:
                channel.queue_declare(queue=descriptor.queue,
                                      callback=callback,
                                      **descriptor.declare_params)
            else:
                channel.queue_declare(queue=descriptor.queue, passive=True, callback=callback)

    def _queue_or_exchange_declared(self, event, *args):
        self.on_event(event)
        self.waiting.remove(event)
        if self.waiting:
            return
        self._bind_queues()

    def _bind_queues(self):
        channel = self.channel
        for binding in (self.spec.bindings or []):
            event = AMQPQueueBound(binding)
            self.waiting.add(event)
            callback = functools.partial(self._queue_bound, event)
            channel.queue_bind(callback, binding.queue, binding.exchange, **binding.bind_params)

        if not self.waiting:
            self._complete()

    def _queue_bound(self, event, *args):
        self.on_event(event)
        self.waiting.remove(event)
        if self.waiting:
            return
        self._complete()

    def _start_consumers(self):
        channel = self.channel
        for consumer in (self.spec.consumers or []):
            consumer_tag = channel.basic_consume(functools.partial(self._on_receive, consumer.on_receive),
                                                 consumer.queue,
                                                 **consumer.consume_params)
            self.on_event(AMQPConsumerStarted(consumer, consumer_tag))

    def _on_event(self, event):
        LOG.warning("event %s", event)

    def _on_receive(self, callback, channel, method, properties, body):
        callback(AMQPRawDataReceived(channel, method, properties, body))

    def _complete(self):
        self._start_consumers()
        self.on_complete(self.spec, self.connection, self.channel)

from logging import basicConfig
from pika.channel import Channel
from pika.connection import URLParameters
from tests import no_jython
from voom.amqp.config import AMQPConfigSpec, AMQPInitializer, \
    AMQPExchangeDescriptor, AMQPBindDescriptor, AMQPQueueDescriptor, \
    AMQPConsumerDescriptor
import unittest

basicConfig()


class Test(unittest.TestCase):
    BODY = 'body'

    def setUp(self):
        self.complete = False
        self.spec = None
        self.queue = "config_test"
        self.exchange = "meow"
        self.channel = None
        self.connection = None

    def on_complete(self, spec, connection, channel):
        self.complete = True
        self.connection = connection
        self.channel = channel
        assert isinstance(channel, Channel)
        channel.basic_publish(self.exchange, 'cow.moo', '')
        channel.basic_publish(self.exchange, 'dog.woof', self.BODY)
        print "publishing"

    def _consume(self, event):
        print 'consumed'
        self.body = event.body
        self.channel.close()
        self.connection.close()

    @no_jython
    def test_1(self):
        self.spec = AMQPConfigSpec(URLParameters("amqp://guest:guest@localhost:5672/%2f"),
                                   queues=[
                                       AMQPQueueDescriptor(self.queue, declare=True, exclusive=True, auto_delete=True)],
                                   exchanges=[AMQPExchangeDescriptor(exchange=self.exchange, auto_delete=True,
                                                                     exchange_type="topic")],
                                   bindings=[AMQPBindDescriptor(queue=self.queue, exchange=self.exchange,
                                                                routing_key='dog.*')],
                                   consumers=[AMQPConsumerDescriptor(self.queue, self._consume, no_ack=True)]
        )
        init = AMQPInitializer(self.spec, self.on_complete)
        init.run()
        assert self.complete
        assert self.body == self.BODY

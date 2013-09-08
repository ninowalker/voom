import unittest
from tests import no_jython
from voom.amqp.config import AMQPQueueDescriptor
from voom.amqp.gateway import AMQPGateway
import pika
from voom.bus import VoomBus
from voom.codecs import ContentCodecRegistry
from voom.codecs.json_codec import JSONMessageCodec
from voom.amqp.events import AMQPDataReceived, AMQPGatewayReady
from voom.decorators import receiver
from voom.context import SessionKeys
from voom.gateway import GatewayShutdownCmd
from logging import basicConfig

basicConfig()


class TestRoundtrip(unittest.TestCase):
    @no_jython
    def test_1(self):
        work = AMQPQueueDescriptor("test_round_trip", declare=True, exclusive=False, auto_delete=True)

        g = AMQPGateway(work.queue,
                        pika.ConnectionParameters(host='localhost'),
                        [work],
                        VoomBus(),
                        ContentCodecRegistry(JSONMessageCodec()))

        bus = g.bus
        bus.raise_errors = True
        self.msgs = []

        @receiver(AMQPDataReceived)
        def receives(msg):
            assert isinstance(msg, AMQPDataReceived)
            self.msgs.append(msg)
            if len(self.msgs) == 1:
                properties = pika.BasicProperties(content_type='application/json',
                                                  content_encoding='zip',
                                                  reply_to=g.return_queue.queue)
                g.send([range(0, 100)], properties, exchange='', routing_key=g.return_queue.queue)
                return
            if len(self.msgs) == 2:
                assert bus.session[SessionKeys.RESPONDER]
                #print bus.session.keys()
                bus.reply([msg.messages[0]])
                return

            bus.publish(GatewayShutdownCmd())

        @receiver(AMQPGatewayReady)
        def on_ready(msg):
            properties = pika.BasicProperties(content_type='application/json',
                                              reply_to=g.return_queue.queue)
            g.send([range(0, 10)], properties, exchange='', routing_key=work.queue)

        bus.register(receives)
        bus.register(on_ready)

        g.run()
        assert len(self.msgs) == 3
        msg = self.msgs.pop(0)
        assert isinstance(msg, AMQPDataReceived)
        assert msg.headers['From'] == g.return_queue.queue
        assert msg.headers['Content-Type'] == 'application/json'
        assert msg.headers['Reply-To'] == g.return_queue.queue
        assert msg.headers['Routing-Key'] == work.queue
        assert msg.messages == [range(10)]
        for msg in self.msgs:
            assert isinstance(msg, AMQPDataReceived)
            assert msg.headers['From'] == g.return_queue.queue
            assert msg.headers['Content-Type'] == 'application/json'
            assert msg.headers['Content-Encoding'] == 'zip'
            assert msg.headers['Reply-To'] == g.return_queue.queue
            assert msg.headers['Routing-Key'] == g.return_queue.queue, msg.headers
            assert msg.messages == [range(100)], msg.messages
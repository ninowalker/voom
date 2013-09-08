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
from voom.gateway import GatewayShutdownCmd
from logging import basicConfig, getLogger
import threading
import logging

basicConfig(level=logging.INFO)

LOG = getLogger(__name__)


class TestMultThread(unittest.TestCase):
    @no_jython
    def test_send_other_thread(self):
        self.rlock = threading.RLock()
        self.rlock.acquire()

        def send_on_ready():
            LOG.info("waiting for lock...")
            with self.rlock:
                LOG.info("lock acquired, sending")
                properties = pika.BasicProperties(content_type='application/json',
                                                  reply_to=g.return_queue.queue)
                self.g.send([range(0, 10)], properties, exchange='', routing_key=work.queue)

            LOG.info("lock released")

        t = threading.Thread(target=send_on_ready)
        t.daemon = True
        t.start()

        work = AMQPQueueDescriptor("test_multithread", declare=True, exclusive=False, auto_delete=True)

        g = AMQPGateway(work.queue,
                        pika.ConnectionParameters(host='localhost'),
                        [work],
                        VoomBus(),
                        ContentCodecRegistry(JSONMessageCodec()))

        self.g = g
        bus = g.bus
        bus.raise_errors = True
        self.msgs = []

        @receiver(AMQPDataReceived)
        def receives(msg):
            assert isinstance(msg, AMQPDataReceived)
            self.msgs.append(msg)
            bus.publish(GatewayShutdownCmd())

        @receiver(AMQPGatewayReady)
        def on_ready(msg):
            LOG.info("releasing...")
            self.rlock.release()

        bus.register(receives)
        bus.register(on_ready)

        g.run()
        assert len(self.msgs) == 1
        msg = self.msgs.pop(0)
        assert isinstance(msg, AMQPDataReceived)
        assert msg.headers['From'] == g.return_queue.queue
        assert msg.headers['Content-Type'] == 'application/json'
        assert msg.headers['Reply-To'] == g.return_queue.queue
        assert msg.headers['Routing-Key'] == work.queue
        assert msg.messages == [range(10)]

'''
Created on Mar 11, 2013

@author: nino
'''
import unittest
import pika
from voom.gateway.amqp import AMQPQueueListener, AMQPGateway, AMQPSenderReady,\
    AMQPQueueDescriptor
from voom.bus import DefaultBus, LOG
from voom.priorities import BusPriority
from logging import basicConfig
from voom.gateway import GatewayShutdownCmd, AMQPConnectionReady,\
    AMQPQueueInitialized
from voom.codecs import ContentCodecRegistry
from voom.codecs.json_codec import JSONCodec
from voom.codecs.mime_codec import MIMEMessageCodec
from voom.context import SessionKeys
import urlparse

basicConfig()

connection_params = pika.ConnectionParameters(host='localhost')

class TestListener(unittest.TestCase):
    
    def setUp(self):
        self._stop = False
        self._stopped = False
        self._connection = None
        self.msgs = []
        self.bus = DefaultBus()
        
        def p(x):
            LOG.warning("%s", x)
        
        self.bus.subscribe(DefaultBus.ALL, lambda x: p(x), priority=BusPriority.HIGH_PRIORITY)
        
    def tearDown(self):
        if self._connection:
            self._connection.close()
        
    @property
    def connection(self):
        if not self._connection:
            self._connection = pika.BlockingConnection(connection_params)
        return self._connection
    
    def receive(self, msg):
        self.msgs.append(msg)
        if len(self.msgs) == 3:
            self._stop = True
            
    def stop(self, msg):
        if self._stop is not True:
            return
        # once and only once
        if self._stopped:
            return
        self._stopped = True
        self.bus.send(GatewayShutdownCmd())
        
    def _opened(self, connection):
        self.bus.send(AMQPConnectionReady(connection))
    
    def send_message(self, queue, msg):
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue, passive=True)        
        self.channel.basic_publish(exchange='',
                              routing_key=queue,
                              body=msg,
                              properties=pika.BasicProperties(delivery_mode = 2, # make message persistent
                                                              ))
        print " [x] Sent %r" % (msg,)
    
    def test_1(self):
        work = AMQPQueueDescriptor("test_work411", declare=True, exclusive=False, auto_delete=True)
        rqueue = AMQPQueueDescriptor("test_return411", declare=True, exclusive=False, auto_delete=True)
        
        
        
        def sender(queue_ready):
            desc = queue_ready.descriptor
            if desc == work:
                self.send_message(work.queue, "1")
            else:
                self.send_message(rqueue.queue, "2")
                self.send_message(rqueue.queue, "3")

        self.bus.subscribe(AMQPQueueInitialized, sender)
        self.bus.subscribe(DefaultBus.ALL, self.stop, BusPriority.LOW_PRIORITY)
        self.bus.subscribe(str, self.receive)
        
        g = AMQPQueueListener([work, rqueue], self.bus)
        assert len(self.bus._get_handlers(str)) == 1
        
        connection = pika.SelectConnection(connection_params, self._opened)
        self.bus.subscribe(GatewayShutdownCmd, lambda x: connection.close(), BusPriority.LOW_PRIORITY)
        connection.ioloop.start()
        
        assert len(self.msgs) == 3, self.msgs
        assert sorted(self.msgs) == ["1", "2", "3"], self.msgs
        print "done"
        

class TestGateway(unittest.TestCase):
    def setUp(self):
        self.bus = DefaultBus()
        self.msgs = []
        self.context = None
        self.supported = ContentCodecRegistry([MIMEMessageCodec(ContentCodecRegistry([JSONCodec()]))])
        def p(x):
            LOG.warning("%s", x)
        
        self.bus.subscribe(DefaultBus.ALL, lambda x: p(x), priority=BusPriority.HIGH_PRIORITY)

    def receive(self, data_event):
        properties = data_event.extras.properties
        print data_event.extras
        codec = self.supported.get_mime_codec(properties.content_type)
        headers, msgs = codec.decode(data_event.body)
        
        context = {SessionKeys.GATEWAY_HEADERS: headers,
                   SessionKeys.GATEWAY_EXTRAS: data_event.extras}
        
        if properties.reply_to:
            parts = urlparse.urlparse(properties.reply_to)
            if parts.scheme:
                context[SessionKeys.REPLY_TO] = parts.reply_to
            else:
                context[SessionKeys.RESPONDER] = None
                
        self.context = context
        print self.context
        self.msgs.extend(msgs)
        self.bus.send(GatewayShutdownCmd())
        
    def test_1(self):        
        queue = AMQPQueueDescriptor("gateway_test21", declare=True, exclusive=True, auto_delete=True)
        g = AMQPGateway("test_gateway11", connection_params, [queue], self.bus, self.receive, self.supported)
        self.bus.subscribe(AMQPSenderReady, 
                           lambda x: x.sender.send(g.address, [range(10), range(5)]))
        g.run()
        assert self.msgs == [range(10), range(5)], self.msgs
        
        

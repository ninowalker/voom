'''
Created on Mar 11, 2013

@author: nino
'''
import unittest
from mock import Mock
import pika
from voom.gateway.amqp import AMQPGateway
from voom.bus import DefaultBus, LOG
from voom.priorities import BusPriority
from logging import basicConfig
from voom.gateway import GatewayShutdownCmd

basicConfig()

class TestSimple(unittest.TestCase):
    
    connection_params = pika.ConnectionParameters(host='localhost')
    
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
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
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
    
    def send_message(self, queue, msg):
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue)        
        self.channel.basic_publish(exchange='',
                              routing_key=queue,
                              body=msg,
                              properties=pika.BasicProperties(delivery_mode = 2, # make message persistent
                                                              ))
        print " [x] Sent %r" % (msg,)
    
    def test_1(self):
        work = "test_work2"
        rqueue = "test_return2"
        
        self.send_message(work, "1")
        self.send_message(rqueue, "2")
        self.send_message(rqueue, "3")

        self.bus.subscribe(DefaultBus.ALL, self.stop, BusPriority.LOW_PRIORITY)
        self.bus.subscribe(str, self.receive)
        
        g = AMQPGateway([work, rqueue], 
                        self.bus,
                        connection_params=self.connection_params)
        assert len(self.bus._get_handlers(str)) == 1
        print "start"
        g.bind()
        
        assert len(self.msgs) == 3, self.msgs
        assert sorted(self.msgs) == ["1", "2", "3"]
        print "done"
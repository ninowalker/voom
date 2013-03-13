'''
Created on Mar 11, 2013

@author: nino
'''
import unittest
from voom.channels.amqp import AMQPPublishingAddress


class Test(unittest.TestCase):
    def test_address(self):
        a = "amqp:///vhost/?routing_key=a_queue"
        addr = AMQPPublishingAddress(a)
        assert addr.exchange == ''
        assert addr.routing_key == 'a_queue', addr.__dict__
        assert addr.vhost == 'vhost'
        assert addr.extra_params == {}
        
        a = "amqp:///vhost/?routing_key=hello&exchange=_an_exchange"
        addr = AMQPPublishingAddress(a)
        assert addr.routing_key == 'hello'
        assert addr.exchange == '_an_exchange'
        assert addr.vhost == 'vhost'

        a = "amqp:///vhost/?routing_key=hello&exchange=_an_exchange&extra=1"
        addr = AMQPPublishingAddress(a)
        assert addr.routing_key == 'hello'
        assert addr.exchange == '_an_exchange'
        assert addr.vhost == 'vhost'
        assert addr.extra_params == dict(extra="1")

        a = "amqp:///vhost/?routing_key=hello&exchange=_an_exchange&extra=1&extra=2"
        addr = AMQPPublishingAddress(a)
        assert addr.routing_key == 'hello'
        assert addr.exchange == '_an_exchange'
        assert addr.vhost == 'vhost'
        assert addr.extra_params == dict(extra=["1", "2"])

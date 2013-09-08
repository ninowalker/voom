from pika.connection import ConnectionParameters
from voom.amqp.address import AMQPAddress
import nose.tools
import unittest


class Test(unittest.TestCase):
    def test_address_1(self):
        p = ConnectionParameters(host="foobar", port=1234)

        addr1 = AMQPAddress(p)
        assert addr1.unparse(routing_key='123') == "amqp://guest:guest@foobar:1234/%2f?routing_key=123"
        assert AMQPAddress(addr1.get_parameters()).unparse() == 'amqp://guest:guest@foobar:1234/%2f'

    def test_address_2(self):
        a = "amqp://localhost:5672/vhost/?routing_key=a_queue"
        addr = AMQPAddress.parse(a)
        assert addr.extras == {'routing_key': 'a_queue'}, addr.__dict__
        assert addr.path == '/vhost', addr.__dict__

    def test_address_3(self):
        a = "amqp://localhost:5672/%2f?routing_key=a_queue&meow=1"
        addr = AMQPAddress.parse(a)
        assert addr.extras == {'routing_key': 'a_queue', 'meow': '1'}, addr.__dict__
        assert addr.get('routing_key') == 'a_queue'
        assert addr.path == '/%2f', addr.__dict__

    def test_fail(self):
        nose.tools.assert_raises(ValueError, AMQPAddress.parse, "http://") #@UndefinedVariable

#        a = "amqp:///vhost/?routing_key=hello&exchange=_an_exchange"
#        addr = AMQPAddress(a)
#        assert addr.routing_key == 'hello'
#        assert addr.exchange == '_an_exchange'
#        assert addr.vhost == 'vhost'
#
#        a = "amqp:///vhost/?routing_key=hello&exchange=_an_exchange&extra=1"
#        addr = AMQPAddress(a)
#        assert addr.routing_key == 'hello'
#        assert addr.exchange == '_an_exchange'
#        assert addr.vhost == 'vhost'
#        assert addr.extra_params == dict(extra="1")
#
#        a = "amqp:///vhost/?routing_key=hello&exchange=_an_exchange&extra=1&extra=2"
#        addr = AMQPAddress(a)
#        assert addr.routing_key == 'hello'
#        assert addr.exchange == '_an_exchange'
#        assert addr.vhost == 'vhost'
#        assert addr.extra_params == dict(extra=["1", "2"])

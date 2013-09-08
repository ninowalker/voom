from logging import basicConfig
from mock import Mock
from voom.amqp.sender import AMQPSender
from voom.codecs import ContentCodecRegistry
import pika
import unittest

basicConfig()


class Test(unittest.TestCase):
    def test_1(self):
        sender = AMQPSender(Mock(spec=pika.channel.Channel),
                            Mock(spec=ContentCodecRegistry))
        sender.supported_content_types.get_by_content_type.return_value = encoder = Mock()
        encoder.encode_message.return_value = "cows"
        properties = pika.BasicProperties(content_type='bark',
                                          content_encoding='zip',
                                          headers={'meow': 1})
        sender.send(["meow"], properties, 'to', 'exchange')
        assert sender.supported_content_types.get_by_content_type.call_count == 1
        sender.supported_content_types.get_by_content_type.assert_called_with(properties.content_type)
        assert encoder.encode_message.call_count == 1
        #        expected_headers = {'meow': 1,
        #                            'From': sender.from_,
        #                            'Content-Encoding': 'zip',
        #                            'Content-Type': properties.content_type}
        encoder.encode_message.assert_called_with(['meow'], {}), encoder.encode_message.call_args
        sender.channel.basic_publish.assert_called_with(exchange='exchange',
                                                        routing_key='to',
                                                        body=encoder.encode_message.return_value.encode('zip'),
                                                        properties=properties)

    def test_serialize_props(self):
        properties = pika.BasicProperties(content_type='bark',
                                          content_encoding='zip',
                                          headers={'meow': 1})
        encoded = b"".join(properties.encode())
        new = pika.BasicProperties()
        new.decode(encoded, 0)
        assert new.headers == properties.headers

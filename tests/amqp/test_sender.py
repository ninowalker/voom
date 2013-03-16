'''
Created on Mar 16, 2013

@author: nino
'''
import unittest
from voom.amqp.sender import AMQPSender
from mock import Mock
import pika
from voom.codecs import ContentCodecRegistry
from logging import basicConfig

basicConfig()

class Test(unittest.TestCase):
    def test_1(self):
        sender = AMQPSender(Mock(spec=pika.channel.Channel), 
                            Mock(spec=ContentCodecRegistry))
        sender.supported_content_types.get_mime_codec.return_value = encoder = Mock()
        encoder.encode.return_value = "cows" 
        properties = pika.BasicProperties(content_type='bark',
                                          content_encoding='zip', 
                                          headers={'meow': 1})
        sender.send(["meow"], properties, 'to', 'exchange')
        assert sender.supported_content_types.get_mime_codec.call_count == 1
        sender.supported_content_types.get_mime_codec.assert_called_with(properties.content_type)
        assert encoder.encode.call_count == 1
        expected_headers = {'meow': 1, 
                            'From': sender.from_,
                            'Content-Encoding': 'zip', 
                            'Content-Type': properties.content_type}
        encoder.encode.assert_called_with(['meow'], expected_headers), encoder.encode.call_args
        sender.channel.basic_publish.assert_called_with(exchange='exchange', 
                                                        routing_key='to', 
                                                        body=encoder.encode.return_value.encode('zip'), 
                                                        properties=properties)
        
    def test_serialize_props(self):
        properties = pika.BasicProperties(content_type='bark',
                                          content_encoding='zip', 
                                          headers={'meow': 1})
        encoded = b"".join(properties.encode())
        new = pika.BasicProperties()
        new.decode(encoded, 0)
        assert new.headers == properties.headers
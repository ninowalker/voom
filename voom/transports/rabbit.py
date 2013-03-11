'''
Created on Mar 10, 2013

@author: nino
'''

import pika
from voom.transports import Sender

class RabbitTransport(Sender):
    def __init__(self):
        self.connection = pika.BlockingConnection()
        self.channel = self.connection.channel()
        
    def _send(self, address, message, mimetype):
        channel.basic_publish(exchange='example',
                      routing_key='test',
                      body='Test Message')
        connection.close()

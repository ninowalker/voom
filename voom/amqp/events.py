'''
Created on Mar 15, 2013

@author: nino
'''

class Event(object):
    def __init__(self, *args):
        for k, v in zip(self.FIELDS.split(" "), args):
            setattr(self, k, v)
        
    @classmethod
    def new(cls, name, fields):
        return type(name, (cls,), {'FIELDS': fields})


AMQPQueueBound = Event.new("AMQPQueueBound", "binding")

AMQPConnectionReady = Event.new("AMQPConnectionReady", "connection")

AMQPChannelReady = Event.new("AMQPChannelReady", "channel connection")

AMQPQueueInitialized = Event.new("AMQPQueueInitialized", "descriptor")

AMQPExchangeInitialized = Event.new("AMQPExchangeInitialized", "descriptor")

AMQPConsumerStarted = Event.new("AMQPConsumerStarted", "descriptor consumer_tag")

AMQPDataReceived = Event.new("AMQPDataReceived", "channel method properties body")
from collections import namedtuple

GatewayShutdownCmd = namedtuple("GatewayShutdownCmd", "")
GatewayMessageUnparseable = namedtuple("GatewayMessageUnparseable", "body")

AMQPConnectionReady = namedtuple("AMQPConnectionReady", "connection")
AMQPChannelReady = namedtuple("AMQPChannelReady", "channel connection thread")
AMQPQueueInitialized = namedtuple("AMQPQueueInitialized", "descriptor")

class AMQPQueueDescriptor(namedtuple('AMQPQueueDescriptor', 'queue declare declare_params')):
    def __new__(cls, queue, declare=False, **kwargs):
        return super(AMQPQueueDescriptor, cls).__new__(cls, queue, declare, kwargs)

class AMQPBindDescriptor(namedtuple('AMQPBindDescriptor', 'queue exchange routing_key bind_params')):
    def __new__(cls, queue, exchange, routing_key, **bind_params):
        return super(AMQPBindDescriptor, cls).__new__(cls, queue, exchange, routing_key, bind_params)



AMQPDataReceived = namedtuple("AMQPDataReceived", "body extras listener")
AMQPMessageExtras = namedtuple("AMQPMessageExtras", "channel method properties")
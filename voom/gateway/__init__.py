from collections import namedtuple

GatewayShutdownCmd = namedtuple("GatewayShutdownCmd", "")
GatewayMessageUnparseable = namedtuple("GatewayMessageUnparseable", "body")

AMQPConnectionReady = namedtuple("AMQPConnectionReady", "connection")
AMQPQueueInitialized = namedtuple("AMQPQueueInitialized", "descriptor")


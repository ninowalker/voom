from collections import namedtuple


#
#
#
GatewayStarted = namedtuple("GatewayStarted", "gateway queue")
GatewayShutdown = namedtuple("GatewayShutdown", "gateway")
GatewayConnectionInitialized = namedtuple("GatewayConnectionInitialized", "queue")

GatewayShutdownCmd = namedtuple("GatewayShutdownCmd", "")

GatewayMessageUnparseable = namedtuple("GatewayMessageUnparseable", "body")

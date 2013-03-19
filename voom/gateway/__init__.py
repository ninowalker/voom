from collections import namedtuple
from voom.events.base import Event

GatewayShutdownCmd = namedtuple("GatewayShutdownCmd", "")

GatewayMessageDecodeError = Event.new("GatewayMessageDecodeError", "event exception traceback")

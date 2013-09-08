from voom.events.base import Event

AMQPQueueBound = Event.new("AMQPQueueBound", "binding")

AMQPConnectionReady = Event.new("AMQPConnectionReady", "connection")

AMQPChannelReady = Event.new("AMQPChannelReady", "channel connection")

AMQPQueueInitialized = Event.new("AMQPQueueInitialized", "descriptor")

AMQPExchangeInitialized = Event.new("AMQPExchangeInitialized", "descriptor")

AMQPConsumerStarted = Event.new("AMQPConsumerStarted", "descriptor consumer_tag")

AMQPRawDataReceived = Event.new("AMQPRawDataReceived", "channel method properties body")

AMQPDataReceived = Event.new("AMQPDataReceived", "messages headers decoded_body receive_event")

AMQPGatewayReady = Event.new("AMQPGatewayReady", "gateway")

AMQPSenderReady = Event.new("AMQPSenderReady", "sender")

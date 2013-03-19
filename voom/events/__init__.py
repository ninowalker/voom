from voom.events.base import Event

__ALL__ = ['Event']

MessageForwarded = Event.new("MessageForwarded", "address message")
import collections
import logging


def consumer(*messages):
    def consuming(func):
        func._receiver_of = set(messages)
        return func
    return consuming

class ConsumerMeta(type):
    def __new__(cls, name, bases, attrs):
        msgs = attrs.get('_receiver_of', set())
        receiving = attrs.get('_receivers', collections.defaultdict(list))
        for attr in attrs.values():
            if hasattr(attr, '_receiver_of'):
                msgs.update(attr._receiver_of)
                for m in attr._receiver_of:
                    receiving[m].append(attr)
                    
        attrs['_receiver_of'] = msgs
        attrs['_receivers'] = receiving
        return super(ConsumerMeta, cls).__new__(cls, name, bases, attrs)


class MessageConsumer(object):
    __metaclass__ = ConsumerMeta 
    
    def __call__(self, message):
        exception = None
        try:
            self.pre_dispatch(message)
            self.dispatch(message)
        except Exception, e:
            exception = e
            self.on_failure(message, e)
        finally:
            self.post_dispatch(message, exception)

    def dispatch(self, message):
        for receiver in self._receivers[type(message)]:
            receiver(self, message)
    
    def pre_dispatch(self, message):
        pass
    
    def post_dispatch(self, message, exception):
        pass
    
    def on_failure(self, msg, exception):
        import pdb; pdb.set_trace()
        logging.getLogger(self.__class__.__name__).error("Failed to process %s: %s", msg, exception)

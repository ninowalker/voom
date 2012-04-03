import collections
import logging
from celery.task import Task
from celery.app.task import TaskType
from .decorators import make_async_task

def consumer(*messages):
    def consuming(func):
        func._receiver_of = set(messages)
        return func
    return consuming

def async(cls):
    return make_async_task(cls, cls._receiver_of)

class ConsumerMeta(TaskType):
    def __new__(cls, name, bases, attrs):
        async = attrs.pop('async', True)
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


class MessageConsumer(Task):
    __metaclass__ = ConsumerMeta 

    def run(self, message):
        self(message)
    
    def __call__(self, message):
        exception = None
        try:
            self.pre_dispatch(message)
            self.dispatch(message)
        except Exception, e:
            exception = e
            self.on_dispatch_failure(message, e)
        finally:
            self.post_dispatch(message, exception)

    def dispatch(self, message):
        for receiver in self._receivers[type(message)]:
            receiver(self, message)
    
    def pre_dispatch(self, message):
        pass
    
    def post_dispatch(self, message, exception):
        pass
    
    def on_dispatch_failure(self, msg, exception):
        logging.getLogger(self.__class__.__name__).error("Failed to process %s: %s", msg, exception)

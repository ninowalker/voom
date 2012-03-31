import inspect
from celery.task import task
from celery.registry import tasks
from functools import update_wrapper

def receiver(*messages, **kwargs):
    def receiving(func):
        async = kwargs.pop('async', True)
        if async:
            t = make_async_task(func, **kwargs)
            t._receiver_of = set(messages)
            update_wrapper(t, func)
            return t
        else:
            func._receiver_of = set(messages)
            return func
    return receiving


def handler(**kwargs):
    def handle(cls):
        msgs = set()
        for attr in inspect.getmembers(cls):
            if hasattr(attr, '_receiver_of'):
                msgs.update(attr._receiver_of)
        
        t = task(**kwargs)(cls)
        tasks.register(t)
        t._receiver_of = msgs
        return Callable(t)
    
    
def make_async_task(func, **kwargs):
    t = task(**kwargs)(func)
    tasks.register(t)    
    return Callable(t)


class Callable(object):
    def __init__(self, f):
        self.task = f 
        
    def __call__(self, *args, **kwargs):
        #assert False, (args, kwargs)
        #print "Calling", self.task
        #import pdb; pdb.set_trace()
        r = self.task.delay(*args, **kwargs)
        #import pdb; pdb.set_trace()
        #print "Called ", self.task
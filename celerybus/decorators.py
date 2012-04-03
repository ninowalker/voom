from celery.task import task
from celery.registry import tasks
from functools import update_wrapper

def receiver(*messages, **kwargs):
    def receiving(func):
        async = kwargs.pop('async', True)
        if async:
            t = make_async_task(func, set(messages), **kwargs)
            update_wrapper(t, func)
            return t
        else:
            func._receiver_of = set(messages)
            return func
    return receiving
    
    
def make_async_task(func, messages, **kwargs):
    t = task(**kwargs)(func)
    tasks.register(t)
    c = AsyncCallable(t, messages)
    return c


class AsyncCallable(object):
    def __init__(self, f, receives):
        self.task = f 
        self._receiver_of = receives
        
        
    def __call__(self, *args, **kwargs):
        #assert False, (args, kwargs)
        #print "Calling", self.task
        #import pdb; pdb.set_trace()
        r = self.task.delay(*args, **kwargs)
        #import pdb; pdb.set_trace()
        #print "Called ", self.task
from celery.registry import tasks
from celery.task import task as _task

def make_async_task(func, messages, **kwargs):
    t = _task(**kwargs)(func)
    tasks.register(t)
    c = AsyncCallable(t, messages)
    return c


class AsyncCallable(object):
    def __init__(self, f, receives):
        self.task = f 
        self._receiver_of = receives
        
        
    def __call__(self, *args, **kwargs):
        self.task.delay(*args, **kwargs)
        
from celery.task import task
from celery.registry import tasks

def async(*celery_args, **celery_kwargs):
    def _tasker(func):
        t = task(**celery_kwargs)(func)
        tasks.register(t)
        return t
    
    return _tasker
    

def receiver(*args):
    def receiving(func):
        func._receiver_of = set(args)
        return func
    return receiving

    
    
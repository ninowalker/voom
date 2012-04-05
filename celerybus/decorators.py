from functools import update_wrapper

__ALL__ = ['receiver']

def receiver(*messages, **kwargs):
    def receiving(func):
        async = kwargs.pop('async', True)
        if async:
            from celerybus.async import make_async_task
            t = make_async_task(func, set(messages), **kwargs)
            update_wrapper(t, func)
            return t
        else:
            func._receiver_of = set(messages)
            return func
    return receiving


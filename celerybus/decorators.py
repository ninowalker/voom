
def receiver(*messages, **kwargs):
    """Decorates a function"""
    def receiving(func):
        from celerybus.async import make_async_task
        return make_async_task(func, set(messages), **kwargs)
    return receiving


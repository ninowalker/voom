from celery.registry import tasks

def make_async_task(func, messages, **kwargs):
    from celerybus.bus import Bus
    assert Bus.celery_app != None
    task_kwargs = Bus.default_task_kwargs.copy()
    task_kwargs.update(kwargs)
    t = bus_task(**task_kwargs)(func)
    tasks.register(t)
    c = AsyncCallable(t, messages)
    c.__name__ = "%_async" % func.__name__
    return c


class AsyncCallable(object):
    def __init__(self, f, receives):
        self.task = f 
        self._receiver_of = receives
        
        
    def __call__(self, *args, **kwargs):
        self.task.delay(*args, **kwargs)

def bus_task(*args, **kwargs):
    """Decorator to create a task class out of any callable.

    **Examples**

    .. code-block:: python

        @task
        def refresh_feed(url):
            return Feed.objects.get(url=url).refresh()

    With setting extra options and using retry.

    .. code-block:: python

        @task(max_retries=10)
        def refresh_feed(url):
            try:
                return Feed.objects.get(url=url).refresh()
            except socket.error, exc:
                refresh_feed.retry(exc=exc)

    Calling the resulting task:

            >>> refresh_feed("http://example.com/rss") # Regular
            <Feed: http://example.com/rss>
            >>> refresh_feed.delay("http://example.com/rss") # Async
            <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>
    """
    from .bus import Bus
    assert Bus.celery_app != None
    kwargs.setdefault("accept_magic_kwargs", False)
    return Bus.celery_app.task(*args, **kwargs)
        
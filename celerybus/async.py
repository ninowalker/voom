from celery.registry import tasks

_app = None

def set_app(app):
    global _app
    _app = app

def make_async_task(func, messages, **kwargs):
    assert _app != None
    t = bus_task(**kwargs)(func)
    tasks.register(t)
    c = AsyncCallable(t, messages)
    c.__name__ = "%s_async" % func.__name__
    return c


class AsyncCallable(object):
    def __init__(self, f, receives):
        self.task = f 
        self._receiver_of = receives
        self._precondition = None
        
    def __call__(self, *args, **kwargs):
        if self._precondition and self._precondition(*args, **kwargs) == False:
            LOG.debug("precondition not met for %s, skipping" % self)
            return None
                
        return self.task.delay(*args, **kwargs)

    def __repr__(self):
        return "<async %s>" % repr(self.task)

    def precondition(self, func):
        self._precondition = func


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
    assert _app != None
    kwargs.setdefault("accept_magic_kwargs", False)
    return _app.task(*args, **kwargs)
        

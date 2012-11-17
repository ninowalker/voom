from celery.registry import tasks
from logging import getLogger
from functools import update_wrapper

LOG = getLogger('celerybus.bus')

_app = None

def set_app(app):
    global _app
    _app = app


def _get_bus():
    from celerybus.bus import Bus
    return Bus


def make_async_task(func, messages, **kwargs):
    assert _app != None
    const_args = {'run_async': kwargs.pop('async', True)}
    
    def _wrapped_func(*args, **kwargs):
        with _get_bus().use_context(kwargs.pop('request')):
            return func(*args, **kwargs['kwargs'])
    
    update_wrapper(_wrapped_func, func)
    t = bus_task(**kwargs)(_wrapped_func)
    tasks.register(t)
    c = AsyncCallable(t, messages, **const_args)
    update_wrapper(c, func)
    return c


class AsyncCallable(object):
    def __init__(self, f, receives, run_async=True):
        self.task = f
        self._run_async = run_async
        self._receiver_of = receives
        self._precondition = None
        
    def __call__(self, *args, **kwargs):
        if self._precondition:
            if self._precondition(*args, **kwargs) == False:
                LOG.debug("precondition not met for %s, skipping", self)
                return None
            else:
                LOG.debug("precondition met for %s", self)
        async = kwargs.pop('run_async', self._run_async)
        kwargs = dict(kwargs=kwargs, request=_get_bus().request)
        if async:
            return self.task.delay(*args, **kwargs)
        return self.task(*args, **kwargs)

    def __repr__(self):
        if self._run_async:
            return "<async %s>" % repr(self.task)
        return repr(self.task)

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
        

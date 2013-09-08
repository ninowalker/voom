from voom.decorators import MessageHandlerWrapper, receiver
import nose.tools
import unittest


def foo(_msg):
    return 1


class TestWrapper(unittest.TestCase):

    def test_wraps(self):
        w = MessageHandlerWrapper(foo, [int])
        assert w._receiver_of == [int]
        assert w._filter == None
        assert w(None) == 1

    def test_filter(self):
        w = MessageHandlerWrapper(foo, [int])
        w.filter(lambda x: x > 10)
        assert w(11) == 1
        assert w(1) == None


class TestDecorator(unittest.TestCase):
    def test_default(self):
        w = receiver(int, str)(foo)
        assert w._receiver_of == (int, str)
        assert w._filter == None
        assert w(None) == 1

    def test_custom_wrapper(self):
        def alt(function, **_kwargs):
            return function

        w = receiver(int, str, wrapper=alt)(foo)
        assert w == foo
        assert not hasattr(w, '_receiver_of')

    def test_bad_args(self):
        with nose.tools.assert_raises(TypeError): #@UndefinedVariable
            _w = receiver(int, str, async=True)(foo)

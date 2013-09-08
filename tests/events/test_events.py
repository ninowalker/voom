from voom.events.base import Event
import unittest


class Test(unittest.TestCase):
    def test_1(self):
        klass = Event.new("EV", "a b c")

        i = klass(1, 2, 3)
        print repr(i)

        assert i == i
        assert i != 1
        assert i == klass(1, 2, 3)
        assert i != klass(1, 2, 4)

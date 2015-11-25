import functools
import threading
import time
import unittest

from voom.bus import VoomBus


class TestLoader(unittest.TestCase):
    loaded = None

    def load(self, delay):
        if not self.loaded:
            self.loaded = 0
        self.loaded += 1
        time.sleep(delay)

    def test_load_twice(self):
        self.load(0)
        self.load(0)
        self.assertEqual(self.loaded, 2)

    def test_no_concurrent(self):
        bus = VoomBus()
        bus.loader = functools.partial(self.load, 0)
        self.assertEqual(self.loaded, None)
        bus.publish(0)
        self.assertEqual(self.loaded, 1)
        bus.publish(0)
        self.assertEqual(self.loaded, 1)

    def test_concurrent(self):
        bus = VoomBus()
        bus.loader = functools.partial(self.load, 0.1)
        self.assertEqual(self.loaded, None)

        def spawn():
            t = threading.Thread(target=bus.publish, args=(0,))
            t.start()

        spawn()
        spawn()
        time.sleep(.2)
        self.assertEqual(self.loaded, 1)
        bus.publish(0)
        self.assertEqual(self.loaded, 1)

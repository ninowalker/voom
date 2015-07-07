from voom.bus import VoomBus, BusPriority
from voom.context import TrxState, ChainedDict
from voom.decorators import receiver
from voom.exceptions import AbortProcessing
import unittest


class TestState(unittest.TestCase):
    def test_consume(self):
        s = TrxState()
        i = -1
        for i, _m in enumerate(s.consume_messages()):
            pass
        assert i == -1
        assert s.is_queue_empty()

        s.enqueue(1)
        s.enqueue(1)

        i = -1
        for i, _m in enumerate(s.consume_messages()):
            pass
        assert i == 1
        assert s.is_queue_empty()

        i = -1
        for i, _m in enumerate(s.consume_messages()):
            pass
        assert i == -1
        assert s.is_queue_empty()

    def test_consume_and_add(self):
        s = TrxState()
        s.enqueue(1)
        for i, _m in enumerate(s.consume_messages()):
            if i < 10:
                s.enqueue(1)
        assert i == 10, i

    def test_consume_heap_order(self):
        s = TrxState()
        s.enqueue("b", priority=2)
        s.enqueue("a", priority=1)

        m = ""
        for _m in s.consume_messages():
            m += _m
        assert m == "ab"

        s.enqueue("b", priority=1)
        s.enqueue("a", priority=2)

        m = ""
        for _m in s.consume_messages():
            m += _m
        assert m == "ba"

    def test_consume_heap_auto_order(self):
        s = TrxState()
        s.enqueue("b")
        s.enqueue("a")
        s.enqueue("!", priority=0)

        m = ""
        for _m in s.consume_messages():
            m += _m
        assert m == "!ba"


class TestSession(unittest.TestCase):
    def test1(self):
        s = ChainedDict()
        s[1] = 2
        assert s[1] == 2
        assert 1 in s
        assert s.get(1) == 2
        assert s.get(2) == None


class TestHeaders(unittest.TestCase):
    def setUp(self):
        self.bus = VoomBus()

    def test_arbitrary_headers(self):
        uni = u'\u014b'
        r = ChainedDict(**{'foo': 1, uni: 2})
        assert r['foo'] == 1
        assert r[uni] == 2
        assert uni in r
        assert r.get('bar') == None
        assert 'bar' not in r

    def test_unicode(self):
        uni = u'\u014b'
        r = ChainedDict(**{'foo': 1, uni: 2})
        unicode(r)
        repr(r)
        print r

    def test_abort(self):
        self.bus.resetConfig()
        self.msg = None

        @receiver(str)
        def aborter(msg):
            if msg == "cancel":
                raise AbortProcessing()

        @receiver(str)
        def not_aborter(msg):
            self.msg = msg

        self.bus.raise_errors = True
        self.bus.register(aborter)
        self.bus.register(not_aborter, priority=BusPriority.LOW_PRIORITY)

        self.bus.publish("foo")
        assert self.msg == "foo"
        self.msg = None
        self.bus.publish("cancel")
        assert self.msg == None


class TestContextVars(unittest.TestCase):
    def setUp(self):
        self.bus = VoomBus(raise_errors=True)
        self.context2 = 0

    def do1(self, msg):
        self.msg = msg
        self.context = self.bus.session['c']
        with self.bus.using(dict(c=self.context + 1), local=True):
            with self.bus.using(dict(c=self.context + 2), local=True):
                self.bus.publish("hello")
            self.bus.publish("hello")

    def do2(self, msg):
        self.msg2 = msg
        self.context2 += self.bus.session['c']

    def test_vars(self):
        msg = 1
        self.bus.subscribe(int, self.do1)
        self.bus.subscribe(str, self.do2)
        with self.bus.using(dict(c=100), local=True):
            self.bus.publish(msg)
        assert self.context == 100
        assert self.context2 == 101 + 102

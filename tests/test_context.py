
'''
Created on Nov 18, 2012

@author: nino
'''
import unittest
from voom.exceptions import AbortProcessing
from voom.decorators import receiver
from voom.context import Session, BusState
from voom.bus import VoomBus, BusPriority
from nose.tools import assert_raises #@UnresolvedImport


class TestState(unittest.TestCase):
    def test_consume(self):
        s = BusState()
        i = -1
        for i, m in enumerate(s.consume_messages()):
            pass
        assert i == -1
        assert s.is_queue_empty()

        s.enqueue(1)
        s.enqueue(1)
        
        i = -1
        for i, m in enumerate(s.consume_messages()):
            pass
        assert i == 1
        assert s.is_queue_empty()
        
        i = -1
        for i, m in enumerate(s.consume_messages()):
            pass
        assert i == -1
        assert s.is_queue_empty()

    def test_consume_and_add(self):
        s = BusState()
        s.enqueue(1)
        for i, m in enumerate(s.consume_messages()):
            if i < 10:
                s.enqueue(1)
        assert i == 10, i

        
class TestSession(unittest.TestCase):
    def test1(self):
        s = Session()
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
        r = Session(**{'foo': 1, uni: 2})
        assert r['foo'] == 1
        assert r[uni] == 2
        assert uni in r
        assert r.get('bar') == None
        assert 'bar' not in r
        
    def test_unicode(self):
        uni = u'\u014b'
        r = Session(**{'foo': 1, uni: 2})
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
        
        self.bus.send("foo")
        assert self.msg == "foo"
        self.msg = None
        self.bus.send("cancel")
        assert self.msg == None

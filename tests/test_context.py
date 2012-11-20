
'''
Created on Nov 18, 2012

@author: nino
'''
import unittest
from celerybus.exceptions import AbortProcessing
from celerybus.decorators import receiver
from celerybus.context import RequestContext
from celerybus.bus import DefaultBus
from celerybus import set_default_bus, get_current_bus
from nose.tools import assert_raises


class TestHeaders(unittest.TestCase):
    def setUp(self):
        self.bus = DefaultBus()
        set_default_bus(self.bus)

    def test_header_access(self):
        self.bus.resetConfig()
        
        self.header = None
        
        @receiver(str, async=False)
        def add_header(msg):
            #print "adding", msg
            self.bus.request.add_header('X-Stuff', msg)

        @receiver(str, async=False)
        def read_header(msg):
            print vars(self.bus.request)
            self.header = self.bus.request.header('X-Stuff')

        self.bus.register(add_header, self.bus.HIGH_PRIORITY)
        self.bus.register(read_header)
        
        self.bus.send('s')
        assert self.header == 's'
        
    def test_header_changes(self):
        self.bus.resetConfig()
        self.headers = []
        
        @receiver(str, async=False)
        def add_header2(msg):
            self.bus.request.add_header('X-Stuff', msg)

        @receiver(str, async=False)
        def read_header2(msg):
            self.headers.extend(self.bus.request.headers('X-Stuff'))
            if msg == 'a':
                self.bus.send('b')

        @receiver(str, async=False)
        def read_header3(msg):
            self.headers.extend(self.bus.request.headers('X-Stuff'))
            if msg == 'a':
                self.bus.send('c')

        self.bus.register(add_header2, self.bus.HIGH_PRIORITY)
        self.bus.register(read_header2)
        self.bus.register(read_header3)
        
        self.bus.send('a')
        assert "".join(self.headers) == 'aabbcc', self.headers
        
    def test_session_inheritance(self):
        r = RequestContext()
        r.session.add_header("COWS", "moo")
        r1 = RequestContext(parent=r)
        assert r1.session.headers('COWS') == r.session.headers('COWS')

    def test_arbitrary_headers(self):
        uni = u'\u014b'
        r = RequestContext({'foo': 1, uni: 2})
        assert r.header('FOO') == 1
        assert r.header(uni) == 2
        assert uni in r
        assert r.header('BAR') == None
        assert 'BAR' not in r
        assert_raises(ValueError, r.add_header, "a", object())
        
    def test_unicode(self):
        uni = u'\u014b'
        r = RequestContext({'foo': 1, uni: 2})
        r.set_context(uni, uni)
        r.enqueue(uni)
        unicode(r)
        repr(r)
        print r
        
    def test_parent(self):
        r = RequestContext()
        r1 = RequestContext(parent=r)
        r2 = RequestContext(parent=r1)
        assert len(r2.headers('Parent')) == 2
        
    def test_abort(self):
        self.bus.resetConfig()
        r = RequestContext()
        assert_raises(AbortProcessing, r.cancel)
        self.msg = None
        
        @receiver(str, async=False)
        def aborter(msg):
            assert get_current_bus() == self.bus
            if msg == "cancel":
                get_current_bus().request.cancel()
        
        @receiver(str, async=False)
        def not_aborter(msg):
            self.msg = msg 
        
        self.bus.raise_errors = True
        self.bus.register(aborter)
        self.bus.register(not_aborter, priority=self.bus.LOW_PRIORITY)
        
        self.bus.send("foo")
        assert self.msg == "foo"
        self.msg = None
        self.bus.send("cancel", request_context=r)
        assert self.msg == None
        assert r.header("Aborted-By") != None
        
        
class TestRequestAttributes(unittest.TestCase):
    def setUp(self):
        self.bus = DefaultBus()
        set_default_bus(self.bus)

    def test_context(self):
        r = RequestContext()
        r.set_context("foo", "bar")
        assert r.context['foo'] == 'bar'
        
    def test_queuing(self):
        r = RequestContext()
        r.enqueue("foo")
        queued = list(r.queued_messages)
        assert len(queued) == 1
        assert queued[0].body == "foo"
        
        self.msgs = []
        self.bus.subscribe(self.bus.ALL, lambda x: self.msgs.append(x))
        self.bus.send("s", request_context=r)
        assert self.msgs == ['s', 'foo']



'''
Created on Nov 18, 2012

@author: nino
'''
import unittest
from celerybus.exceptions import AbortProcessing
from celerybus.decorators import receiver
from celerybus.context import Session
from celerybus.bus import DefaultBus
from celerybus import set_default_bus, get_current_bus
from nose.tools import assert_raises #@UnresolvedImport


class TestHeaders(unittest.TestCase):
    def setUp(self):
        self.bus = DefaultBus()
        set_default_bus(self.bus)
        
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
            assert get_current_bus() == self.bus
            if msg == "cancel":
                raise AbortProcessing()
        
        @receiver(str)
        def not_aborter(msg):
            self.msg = msg 
        
        self.bus.raise_errors = True
        self.bus.register(aborter)
        self.bus.register(not_aborter, priority=self.bus.LOW_PRIORITY)
        
        self.bus.send("foo")
        assert self.msg == "foo"
        self.msg = None
        self.bus.send("cancel")
        assert self.msg == None

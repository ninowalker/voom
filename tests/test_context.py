'''
Created on Nov 18, 2012

@author: nino
'''
import unittest
from celerybus.decorators import receiver
from celerybus.context import RequestContext
from celerybus.bus import DefaultBus
from celerybus import set_default_bus

Bus = DefaultBus()
set_default_bus(Bus)

class TestHeaders(unittest.TestCase):
    def test_access(self):
        Bus.resetConfig()
        
        self.header = None
        
        @receiver(str, async=False)
        def add_header(msg):
            #print "adding", msg
            Bus.request.add_header('X-Stuff', msg)

        @receiver(str, async=False)
        def read_header(msg):
            print vars(Bus.request)
            self.header = Bus.request.headers['X-Stuff'][0]

        Bus.register(add_header, Bus.HIGH_PRIORITY)
        Bus.register(read_header)
        
        Bus.send('s')
        assert self.header == 's'
        
    def test2(self):
        Bus.resetConfig()
        self.headers = []
        
        @receiver(str, async=False)
        def add_header2(msg):
            Bus.request.add_header('X-Stuff', msg)

        @receiver(str, async=False)
        def read_header2(msg):
            self.headers.extend(Bus.request.headers['X-Stuff'])
            if msg == 'a':
                Bus.send('b')

        @receiver(str, async=False)
        def read_header3(msg):
            self.headers.extend(Bus.request.headers['X-Stuff'])
            if msg == 'a':
                Bus.send('c')

        Bus.register(add_header2, Bus.HIGH_PRIORITY)
        Bus.register(read_header2)
        Bus.register(read_header3)
        
        Bus.send('a')
        assert "".join(self.headers) == 'aabbcc', self.headers
        
    def test_inheritance(self):
        r = RequestContext()
        r.add_header("COWS", "moo")
        r1 = RequestContext(r)
        assert r1.headers['COWS'] == r.headers['COWS']
        
class TestAttributes(unittest.TestCase):
    def test_context(self):
        r = RequestContext()
        r.set_context("foo", "bar")
        assert r.context['foo'] == 'bar'
        assert 'bar' in str(r)
        
    def test_queuing(self):
        r = RequestContext()
        r.enqueue("foo")
        queued = list(r.queued_messages)
        assert len(queued) == 1
        assert queued[0].body == "foo"
        assert "foo" in str(r)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
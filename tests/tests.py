'''
Created on Mar 30, 2012

@author: nino
'''
from celerybus.async import set_celery_app
from celery import Celery

import unittest
from celerybus.decorators import receiver
from celerybus.bus import DefaultBus
from celerybus import set_default_bus
from nose.tools import assert_raises
from celerybus.context import RequestContext
import sys


class BaseTest(unittest.TestCase):
    def setUp(self):                
        self.bus = DefaultBus(verbose=True)
        set_default_bus(self.bus)


celery = Celery()
celery.config_from_object('tests.celeryconfig', False)
celery.set_current()
set_celery_app(celery)

class TestBasic(BaseTest):
    
    def testDecorators(self):
        self.bus.resetConfig()
        @receiver(str)
        def foo(msg):
            pass
            
        assert str in foo._receiver_of 
        assert len(foo._receiver_of) == 1
        
    def testAsyncDeco(self):
        self.bus.resetConfig()
        self.bus.subscribe(self.bus.ERRORS, lambda x: sys.stdout.write(repr(x)))
        self._adec = None
        this = self

        assert this.bus.current_message == None

        @receiver(str, async=True)
        def foo(msg):
            assert this.bus.current_message.body == msg  
            this._adec = msg
        
        self.bus.register(foo)
        
        msg = "xoxo"
        self.bus.send(msg)
        assert self._adec == msg
        assert this.bus.current_message == None
        
    def testBusSend(self):
        self.bus.resetConfig()
        self.foo_ = None
        self.all_ = None
        self.obj_ = None
        def foo(msg):
            self.foo_ = msg
            
        def glob(msg):
            self.all_ = msg
            
        def obj(msg):
            self.obj_ = msg
        
        self.bus.subscribe(str, foo)
        self.bus.subscribe(object, obj)
        self.bus.subscribe(self.bus.ALL, glob)
        
        msg = "msg"
        self.bus.send(msg)
        assert self.foo_ == msg
        assert self.all_ == msg
        assert self.obj_ == None
        mobj = object()
        self.bus.send(mobj)
        assert self.obj_ == mobj
        assert self.foo_ == msg
        assert self.all_ == mobj

    def testBusRegister(self):
        self.bus.resetConfig()
        self._ack = None
        this = self
        
        @receiver(str, int)
        def foo_async(msg):
            #print "Fail...."
            this._ack = msg
            #return "mooo"

        assert foo_async.task.app.conf.CELERY_ALWAYS_EAGER
            
        self.bus.register(foo_async)
        self.bus.register(foo_async) # handle already registered
        self.bus.send("x", fail_on_error=True)
        assert self._ack == "x"
        self.bus.send(1)
        assert self._ack == 1
        
        self.bus.unsubscribe(str, foo_async)
        
class TestPriority(BaseTest):
    def test1(self):
        msgs = []
        self.bus.resetConfig()
        self.bus.verbose = True
        self.bus.subscribe(str, lambda s: msgs.append(1), priority=self.bus.HIGH_PRIORITY)
        
        self.bus.send("frackle")
        assert msgs == [1], msgs
        msgs = []

        self.bus.subscribe(str, lambda s: msgs.append(3), priority=self.bus.LOW_PRIORITY)
        
        self.bus.send("frackle")
        assert msgs == [1, 3], msgs
        msgs = []

        self.bus.subscribe(str, lambda s: msgs.append(2))
        self.bus.send("frackle")
        assert msgs == [1, 2, 3], msgs
        
        def hi(s):
            return msgs.append(0)
        self.bus.subscribe(str, hi, priority=self.bus.LOW_PRIORITY+1)
        msgs = []
        self.bus.send("frackle")
        assert msgs == [1, 2, 3, 0], msgs
        self.bus.subscribe(str, hi, priority=0)
        msgs = []
        self.bus.send("frackle")
        assert msgs == [0, 1, 2, 3], msgs


class TestErrorQueue(BaseTest):
    def test1(self):
        msgs = []
        self.bus.resetConfig()
        self.bus.verbose = True
        
        class FancyException(Exception): pass
        
        def fail(m):
            raise FancyException(m)
        
        def catch(m):
            msgs.append(m)
            
        self.bus.subscribe(self.bus.ERRORS, catch, 0)
        self.bus.subscribe(str, fail)
        self.bus.send("cows")
        assert len(msgs) == 1
        failure = msgs[0]
        assert isinstance(failure.exception, FancyException)
        assert failure.message == "cows", failure
        assert len(failure.invocation_context)
        # ensure no recursion
        msgs = []
        self.bus.subscribe(self.bus.ERRORS, fail, 0)
        self.bus.send("cows")
        assert len(msgs) == 1
        failure = msgs[0]
        assert isinstance(failure.exception, FancyException)
        

class TestBreadth(BaseTest):
    def test1(self):
        msgs = []
        self.bus.resetConfig()
        self.bus.verbose = True
        
        def parent(s):
            msgs.append("parent")
            self.bus.send(1)
            assert self.bus.current_message.body == "x"
            
        def child1(i):
            msgs.append("c1")
            self.bus.send(1.1)
            assert self.bus.current_message.body == 1

        def child2(i):
            msgs.append("c2")
            assert self.bus.current_message.body == 1
            
        def child3(f):
            msgs.append("c3")
            assert self.bus.current_message.body == 1.1
        
        self.bus.subscribe(str, parent)
        self.bus.subscribe(int, child1, priority=self.bus.HIGH_PRIORITY)
        self.bus.subscribe(int, child2, priority=self.bus.LOW_PRIORITY)
        self.bus.subscribe(float, child3, priority=self.bus.HIGH_PRIORITY)
        self.bus.send("x")
        assert msgs == ["parent", "c1", "c2", "c3"], msgs
        assert self.bus.current_message == None
        

class TestManualAsync(BaseTest):
    def setUp(self):
        from celery import conf
        conf.ALWAYS_EAGER = False
        super(TestManualAsync, self).setUp()

    def tearDown(self):
        from celery import conf
        conf.ALWAYS_EAGER = True
    
    def test1(self):
        """Test manual asynchronous invocation of a default synchronous handler."""
        from celery import conf
        conf.ALWAYS_EAGER = False

        msgs = []
        @receiver(str, async=False)
        def m(msg):
            msgs.append(msg)
        
        # mangle the delay function to ensure 
        # we invoke inband
        m.task.delay = lambda x, **kwargs: msgs.append(x.upper())
        
        self.bus.register(m)
        self.bus.send("a")
        assert msgs == ['a']
        msgs = []
        
        m('a')
        assert msgs == ['a']
        msgs = []
        
        m('a', run_async=True)
        assert msgs == ['A'], msgs
        msgs = []
        
    def test2(self):
        """Test manual synchronous invocation of an async default handler."""

        self.msgs = []
        @receiver(str, async=True)
        def ar(msg):
            self.msgs.append(msg)
        
        # mangle the delay function to ensure 
        # we invoke inband
        ar.task.delay = lambda x, **kwargs: self.msgs.append(x.upper())
        
        self.bus.register(ar)
        self.bus.send("a")
        assert self.msgs == ['A']
        self.msgs = []
        
        ar('a')
        assert self.msgs == ['A']
        self.msgs = []

        ar('a', run_async=False)
        assert self.msgs == ['a'], self.msgs
        
        
class TestPreconditions(BaseTest):    
    def test1(self):
        self.bus.resetConfig()
        self.msgs = []
        @receiver(str, async=False)
        def m2x(msg):
            self.msgs.append(msg)
        
        def pre(s):
            return s == 'cow'
        
        m2x.precondition(pre)
        
        m2x('moo')
        assert not self.msgs
        m2x('cow')
        assert self.msgs == ['cow'], self.msgs
        
    
class TestSettings(BaseTest):
    def test1(self):
        self.bus.raise_errors = self.bus.raise_errors
        self.bus.loader = self.bus.loader
        
        def loader():
            self.x = True
        
        self.bus.loader = loader
        self.bus.send('s')
        assert self.x
        
class TestRaiseErrors(BaseTest):
    def test1(self):
        self.bus.raise_errors = True
        
        @receiver(str, async=False)
        def thrower(m):
            raise ValueError(m)
        self.bus.register(thrower)
        
        assert_raises(ValueError, self.bus.send, "s")
        self.bus.raise_errors = False
        
        self.bus.send("xxx") # no error
        r = RequestContext()
        with assert_raises(ValueError):
            self.bus.send("yyy", fail_on_error=True, request_context=r)
            print r.__dict__
            
    def test_unsubscribe(self):
        with assert_raises(ValueError):
            self.bus.unsubscribe(str, map)
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
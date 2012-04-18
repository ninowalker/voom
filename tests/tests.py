'''
Created on Mar 30, 2012

@author: nino
'''
import os
os.environ['CELERY_CONFIG_MODULE'] = 'tests.celeryconfig'

import unittest
from celerybus import Bus
from celerybus.consumer import MessageConsumer, consumes, AsyncConsumer
from celerybus.decorators import receiver


Bus.verbose = True

class TestBasic(unittest.TestCase):
    
    def testDecorators(self):
        Bus.resetConfig()
        @receiver(str)
        def foo(msg):
            pass
            
        assert str in foo._receiver_of 
        assert len(foo._receiver_of) == 1
        
    def testAsyncDeco(self):
        Bus.resetConfig()
        self._adec = None
        this = self

        @receiver(str, async=True)
        def foo(msg):
            this._adec = msg
        
        Bus.register(foo)
        
        msg = "xoxo"
        Bus.send(msg)
        assert self._adec == msg
        
    def testBusSend(self):
        Bus.resetConfig()
        self.foo_ = None
        self.all_ = None
        self.obj_ = None
        def foo(msg):
            self.foo_ = msg
            
        def glob(msg):
            self.all_ = msg
            
        def obj(msg):
            self.obj_ = msg
        
        Bus.subscribe(str, foo)
        Bus.subscribe(object, obj)
        Bus.subscribe(Bus.ALL, glob)
        
        msg = "msg"
        Bus.send(msg)
        assert self.foo_ == msg
        assert self.all_ == msg
        assert self.obj_ == None
        mobj = object()
        Bus.send(mobj)
        assert self.obj_ == mobj
        assert self.foo_ == msg
        assert self.all_ == mobj

    def testBusRegister(self):
        Bus.resetConfig()
        self._ack = None
        this = self
        
        @receiver(str, int)
        def foo_async(msg):
            #print "Fail...."
            this._ack = msg
            #return "mooo"

        assert foo_async.task.app.conf.CELERY_ALWAYS_EAGER
            
        Bus.register(foo_async)
        Bus.send("x", fail_on_error=True)
        assert self._ack == "x"
        Bus.send(1)
        assert self._ack == 1
        
class TestPriority(unittest.TestCase):
    def test1(self):
        msgs = []
        Bus.resetConfig()
        Bus.verbose = True
        Bus.subscribe(str, lambda s: msgs.append(1), priority=Bus.HIGH_PRIORITY)
        
        Bus.send("frackle")
        assert msgs == [1], msgs
        msgs = []

        Bus.subscribe(str, lambda s: msgs.append(3), priority=Bus.LOW_PRIORITY)
        
        Bus.send("frackle")
        assert msgs == [1, 3], msgs
        msgs = []

        Bus.subscribe(str, lambda s: msgs.append(2))
        Bus.send("frackle")
        assert msgs == [1, 2, 3], msgs
        
        def hi(s):
            return msgs.append(0)
        Bus.subscribe(str, hi, priority=Bus.LOW_PRIORITY+1)
        msgs = []
        Bus.send("frackle")
        assert msgs == [1, 2, 3, 0], msgs
        Bus.subscribe(str, hi, priority=0)
        msgs = []
        Bus.send("frackle")
        assert msgs == [0, 1, 2, 3], msgs


class TestErrorQueue(unittest.TestCase):
    def test1(self):
        msgs = []
        Bus.resetConfig()
        Bus.verbose = True
        
        class FancyException(Exception): pass
        
        def fail(m):
            raise FancyException(m)
        
        def catch(m):
            msgs.append(m)
            
        Bus.subscribe(Bus.ERRORS, catch, 0)
        Bus.subscribe(str, fail)
        Bus.send("cows")
        assert len(msgs) == 1
        msg, callback, ex = msgs[0]
        assert msg == "cows"
        assert callback == fail
        assert "cows" in str(ex)
        assert "fail" in str(ex)
        assert ex[1] == "FancyException"

        # ensure no recursion
        msgs = []
        Bus.subscribe(Bus.ERRORS, fail, 0)
        Bus.send("cows")
        assert len(msgs) == 1
        msg, callback, ex = msgs[0]
        
    def test2(self):                
        Bus.resetConfig()
        msgs = []
                 
        @AsyncConsumer
        class FailConsumer(MessageConsumer):
            """My docs"""            
            @consumes(int)
            def passes(self, v):
                msgs.append(1)
                pass

            @consumes(int)
            def fails(self, msg):
                raise Exception(str(msg))
        
        Bus.register(FailConsumer)
        Bus.subscribe(Bus.ERRORS, lambda m: msgs.append(m))
        Bus.send(1)
        assert len(msgs) == 2
        assert msgs[1][0] == 1
        assert "1" in str(msgs[1][2])


class TestBreadth(unittest.TestCase):
    def test1(self):
        msgs = []
        Bus.resetConfig()
        Bus.verbose = True
        
        def parent(s):
            msgs.append("parent")
            Bus.send(1)
            
        def child1(i):
            msgs.append("c1")
            Bus.send(1.1)

        def child2(i):
            msgs.append("c2")
            
        def child3(f):
            msgs.append("c3")
        
        Bus.subscribe(str, parent)
        Bus.subscribe(int, child1, priority=Bus.HIGH_PRIORITY)
        Bus.subscribe(int, child2, priority=Bus.LOW_PRIORITY)
        Bus.subscribe(float, child3)
        Bus.send("x")
        assert msgs == ["parent", "c1", "c2", "c3"], msgs

class TestConsumers(unittest.TestCase):
    def test1(self):
        Bus.resetConfig()
        self._test1 = False
        this = self
        class AConsumer(MessageConsumer):
            @consumes(int)
            def handleInt(self, msg):
                assert type(msg) == int
                this._test1 = True
                print "got int"
                
        Bus.register(AConsumer())
        Bus.send(1)
        assert this._test1
        
    def test2(self):                
        Bus.resetConfig()
        self._test2 = False
        this = self
         
        @AsyncConsumer
        class BConsumer(MessageConsumer):
            """My docs"""
            max_retries = 2
            serializer = 'json'
            
            @consumes(int)
            def handleInt(self, msg):
                assert type(msg) == int
                this._test2 = True

            @consumes(str)
            def handleStr(self, msg):
                assert type(msg) == str
                this._test2 = msg
        
        assert BConsumer.task.max_retries == 2
        assert BConsumer.task.serializer == 'json'
        
        Bus.register(BConsumer)
        Bus.send(1)
        assert this._test2
        
        Bus.send(str("x"))
        assert this._test2 == "x"
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
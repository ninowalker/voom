'''
Created on Mar 30, 2012

@author: nino
'''
import os
os.environ['CELERY_CONFIG_MODULE'] = 'tests.celeryconfig'

import unittest
from celerybus.bus import Bus
from celerybus.decorators import receiver, async
from celery.registry import tasks


class Test(unittest.TestCase):
    
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

        def foo(msg):
            this._adec = msg
        
        foo = async()(foo)
        assert hasattr(foo, 'delay'), dir(foo)
        
        Bus.subscribe(str, foo)
        
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
        @receiver(str, int)
        def foo(msg):
            self._ack = msg
            
        Bus.register(foo)
        Bus.send("x")
        assert self._ack == "x"
        Bus.send(1)
        assert self._ack == 1
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
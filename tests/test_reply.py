'''
Created on Feb 28, 2013

@author: nino
'''
import unittest
import threading
from celerybus.transports import CurrentThreadSender
from celerybus.bus import DefaultBus
import nose.tools
from celerybus.exceptions import InvalidStateError, InvalidAddressError
from celerybus.decorators import receiver


class TestCurrentThreadSendDelegate(unittest.TestCase):
    def test1(self):        
        d = CurrentThreadSender()
        d(None, [1, 2, 3])
        assert d.messages == [[1, 2, 3]]
        assert d.pop_all() == [[1, 2, 3]]
        assert d.messages == []

    def test2(self):
        d = CurrentThreadSender()
        
        def append(*args):
            d("local", args)
            assert d.messages == [args], (d.messages, args)
            
        assert not d.messages
        for i in [0, 1]:
            t = threading.Thread(target=append, args=(None, [i]))
            t.daemon = True
            t.start()
            t.join()
            
        assert not d.messages

class TestBusReply(unittest.TestCase):
    def setUp(self):
        self.bus = DefaultBus()

    def test_errors(self):
        nose.tools.assert_raises(InvalidAddressError, self.bus.reply, None)
        nose.tools.assert_raises(InvalidStateError, self.bus.set_reply_address, None, None)
        
    def test_reply_1(self):
        @receiver(str)
        def what_is_it(msg):
            self.bus.reply('ponies')
        
        self.bus.register(what_is_it)
        
        with self.bus.use_context() as ctx:
            self.bus.set_reply_address(CurrentThreadSender.ADDRESS, None)            
            self.bus.reply("ponies")
            assert self.bus.thread_replies.messages == ['ponies']
        
    def test_reply_2(self):
        @receiver(str)
        def what_is_it(msg):
            self.bus.reply('ponies')
        
        self.bus.register(what_is_it)
        self.bus.raise_errors = True
        
        with self.bus.use_context() as ctx:
            self.bus.set_reply_address(CurrentThreadSender.ADDRESS, None)
            assert self.bus.request
            self.bus.send("my little", request_context=ctx)
            #self.bus.reply("ponies")
            assert self.bus.thread_replies.messages == ['ponies']
         

'''
Created on Feb 28, 2013

@author: nino
'''
import unittest
import threading
from celerybus.transports import CurrentThreadSender, UnknownSchemeError
from celerybus.bus import DefaultBus
import nose.tools
from celerybus.exceptions import InvalidStateError, InvalidAddressError
from celerybus.decorators import receiver
from mock import patch
from celerybus.context import SessionKeys


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
        with patch('celerybus.bus.DefaultBus.session', {}):
            nose.tools.assert_raises(InvalidAddressError, self.bus.reply, None) #@UndefinedVariable

        with patch('celerybus.bus.DefaultBus.session', {SessionKeys.REPLY_TO: "badaddr"}):
            assert self.bus.session == {SessionKeys.REPLY_TO: "badaddr"}, self.bus.session
            nose.tools.assert_raises(UnknownSchemeError, self.bus.reply, None) #@UndefinedVariable
        
    def test_reply_1(self):
        @receiver(str)
        def what_is_it(msg):
            self.bus.reply('ponies')
        
        self.bus.register(what_is_it)

        self.bus.send("meow", {SessionKeys.REPLY_TO: CurrentThreadSender.ADDRESS})
        assert self.bus.thread_transport.pop_all() == ['ponies']
        
    def test_reply_2(self):
        @receiver(str)
        def what_is_it(msg):
            self.bus.reply('ponies')
        
        self.bus.register(what_is_it)
        self.bus.raise_errors = True
        
        nose.tools.assert_raises(InvalidAddressError, self.bus.send, "my little")
                 

'''
Created on Feb 28, 2013

@author: nino
'''
import unittest
import threading
from voom.channels import CurrentThreadChannel
from voom.bus import DefaultBus
import nose.tools
from voom.exceptions import InvalidStateError, InvalidAddressError
from voom.decorators import receiver
from mock import patch
from voom.context import SessionKeys
from logging import basicConfig
from voom.events import MessageForwarded


basicConfig()

class TestCurrentThreadSendDelegate(unittest.TestCase):
    def test1(self):        
        d = CurrentThreadChannel()
        d(None, [1, 2, 3])
        assert d.messages == [[1, 2, 3]]
        assert d.pop_all() == [[1, 2, 3]]
        assert d.messages == []

    def test2(self):
        d = CurrentThreadChannel()
        
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
        self.forward = None

    def test_errors(self):
        with patch('voom.bus.DefaultBus.session', {}):
            nose.tools.assert_raises(InvalidAddressError, self.bus.reply, None) #@UndefinedVariable

        with patch('voom.bus.DefaultBus.session', {SessionKeys.REPLY_TO: "badaddr"}):
            assert self.bus.session == {SessionKeys.REPLY_TO: "badaddr"}, self.bus.session
            nose.tools.assert_raises(InvalidStateError, self.bus.reply, None) #@UndefinedVariable
        
    def test_reply_1(self):
        @receiver(str)
        def what_is_it(msg):
            self.bus.reply('ponies')
        
        @receiver(MessageForwarded)
        def forward(msg):
            self.forward = msg
        
        self.bus.register(what_is_it)
        self.bus.register(forward)

        self.bus.send("meow", {SessionKeys.REPLY_TO: CurrentThreadChannel.ADDRESS})
        assert self.bus.thread_channel.pop_all() == ['ponies']
        assert self.forward
        assert self.forward.message == "ponies", self.forward
        assert self.forward.address == CurrentThreadChannel.ADDRESS
        
    def test_reply_2(self):
        @receiver(str)
        def what_is_it(msg):
            self.bus.reply('ponies')
        
        self.bus.register(what_is_it)
        self.bus.raise_errors = True
        
        nose.tools.assert_raises(InvalidAddressError, self.bus.send, "my little")

'''
Created on Mar 30, 2012

@author: nino
'''

import sys
import unittest
import nose.tools
import voom.bus

from voom.decorators import receiver
from voom.bus import VoomBus, BusPriority
from voom.exceptions import BusError, AbortProcessing
from nose.tools import assert_raises #@UnresolvedImport
from mock import Mock, patch
from voom.context import BusState


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.bus = VoomBus(verbose=True)


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

        @receiver(str)
        def foo(msg):
            assert this.bus.current_message.body == msg
            this._adec = msg

        self.bus.register(foo)

        msg = "xoxo"
        self.bus.publish(msg)
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
        self.bus.publish(msg)
        assert self.foo_ == msg
        assert self.all_ == msg
        assert self.obj_ == None
        mobj = object()
        self.bus.publish(mobj)
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

        self.bus.register(foo_async)
        self.bus.register(foo_async) # handle already registered
        self.bus.publish("x")
        assert self._ack == "x"
        self.bus.publish(1)
        assert self._ack == 1

        self.bus.unsubscribe(str, foo_async)


class TestPriority(BaseTest):
    def test1(self):
        msgs = []
        self.bus.resetConfig()
        self.bus.verbose = True
        self.bus.subscribe(str, lambda s: msgs.append(1), priority=BusPriority.HIGH_PRIORITY)

        self.bus.publish("frackle")
        assert msgs == [1], msgs
        msgs = []

        self.bus.subscribe(str, lambda s: msgs.append(3), priority=BusPriority.LOW_PRIORITY)

        self.bus.publish("frackle")
        assert msgs == [1, 3], msgs
        msgs = []

        self.bus.subscribe(str, lambda s: msgs.append(2))
        self.bus.publish("frackle")
        assert msgs == [1, 2, 3], msgs

        def hi(s):
            return msgs.append(0)
        self.bus.subscribe(str, hi, priority=BusPriority.LOW_PRIORITY + 1)
        msgs = []
        self.bus.publish("frackle")
        assert msgs == [1, 2, 3, 0], msgs
        self.bus.subscribe(str, hi, priority=0)
        msgs = []
        self.bus.publish("frackle")
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
        self.bus.publish("cows")
        assert len(msgs) == 1
        failure = msgs[0]
        assert isinstance(failure.exception, FancyException)
        assert failure.message == "cows", failure
        assert len(failure.invocation_context)
        # ensure no recursion
        msgs = []
        self.bus.subscribe(self.bus.ERRORS, fail, 0)
        self.bus.publish("cows")
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
            self.bus.publish(1)
            assert self.bus.current_message.body == "x"

        def child1(i):
            msgs.append("c1")
            self.bus.publish(1.1)
            assert self.bus.current_message.body == 1

        def child2(i):
            msgs.append("c2")
            assert self.bus.current_message.body == 1

        def child3(f):
            msgs.append("c3")
            assert self.bus.current_message.body == 1.1

        self.bus.subscribe(str, parent)
        self.bus.subscribe(int, child1, priority=BusPriority.HIGH_PRIORITY)
        self.bus.subscribe(int, child2, priority=BusPriority.LOW_PRIORITY)
        self.bus.subscribe(float, child3, priority=BusPriority.HIGH_PRIORITY)
        self.bus.publish("x")
        assert msgs == ["parent", "c1", "c2", "c3"], msgs
        assert self.bus.current_message == None


class TestPreconditions(BaseTest):
    def test1(self):
        self.bus.resetConfig()
        self.msgs = []
        @receiver(str)
        def m2x(msg):
            self.msgs.append(msg)

        def pre(s):
            return s == 'cow'

        m2x.filter(pre)

        repr(m2x)

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
        self.bus.publish('s')
        assert self.x

class TestRaiseErrors(BaseTest):
    def setUp(self):
        super(TestRaiseErrors, self).setUp()
        self._log = voom.bus.LOG

    def set_log(self, val):
        voom.bus.LOG = val

    def tearDown(self):
        super(TestRaiseErrors, self).tearDown()
        voom.bus.LOG = self._log

    def test_bad_loader(self):
        self.bus.loader = "meow"
        assert_raises(TypeError, self.bus.publish, "s")

    def test1(self):
        self.bus.raise_errors = True

        @receiver(str)
        def thrower(m):
            raise ValueError(m)
        self.bus.register(thrower)

        assert self.bus.raise_errors

        assert_raises(ValueError, self.bus.publish, "s")
        self.bus.raise_errors = False

        self.bus.publish("xxx") # no error

    def test_unsubscribe(self):
        with assert_raises(ValueError):
            self.bus.unsubscribe(str, map)

    def test_bad_publish_error(self):
        @receiver(str)
        def thrower(m):
            raise ValueError(m)

        self.bus.register(thrower)

        with patch.object(self.bus, '_send_error') as ex:
            ex.side_effect = ValueError
            self.bus.publish("x")
            assert type(ex.call_args_list[0].call_list()[0][0][2]) == ValueError
        assert self.bus.session is None

    def test_fatal_exception(self):
        @receiver(str)
        def thrower(m):
            raise ValueError(m)

        self.bus.register(thrower)
        self.bus._send_error = Mock(side_effect=ValueError)

        # this will swallow the error
        self.bus.publish("x")

        # now we cause problems real, hijacking
        # LOG.exception which should never barf.
        with patch.object(self._log, 'exception') as fe: #@UndefinedVariable
            fe.side_effect = TypeError
            with nose.tools.assert_raises(BusError): #@UndefinedVariable
                self.bus.publish("x")

    def test_error_abort(self):
        self.a = 0
        @receiver(str)
        def thrower(m):
            raise ValueError(m)

        @receiver(str)
        def doer(m):
            self.a += 1

        self.bus.register(doer, BusPriority.LOW_PRIORITY)

        self.bus.publish("x")
        assert self.a == 1

        self.bus.register(thrower)
        self.bus._send_error = Mock(side_effect=AbortProcessing)
        self.bus.publish("x")
        assert self.a == 1

class TestSession(unittest.TestCase):
    def setUp(self):
        self.bus = VoomBus()

    def test_1(self):
        session = {}
        @receiver(str)
        def doer1(s):
            self.bus.session[s] = True
            session.update(self.bus.session)

        self.bus.register(doer1)
        self.bus.publish("meow", dict(a=1, b=2))
        assert session == dict(a=1, b=2, meow=True)

        session = {}
        self.bus.publish("meow")
        assert session == dict(meow=True)

        @receiver(str)
        def doer2(s):
            if s == "meow":
                self.bus.publish("grr")
        self.bus.register(doer2)
        session = {}
        self.bus.publish("meow")
        assert session == dict(meow=True, grr=True)


class TestDefer(unittest.TestCase):
    def setUp(self):
        self.bus = VoomBus()

    def test1(self):
        @receiver(str)
        def h1(msg):
            self.msgs.append(msg)
            self.bus.defer(1)

        @receiver(int, str)
        def h2(msg):
            self.msgs.append(msg)

        self.msgs = []
        self.bus.register(h1)
        self.bus.register(h2)
        self.bus.publish("s")
        assert self.msgs == ['s', 's', 1]


class TestWithContext(unittest.TestCase):
    def test1(self):
        bus = VoomBus()
        data = {1: 2}
        with bus.session_data(data):
            assert bus._session_data.data == data, bus._session_data.data
        assert bus._session_data.data is None, bus._session_data.data

    def test2(self):
        bus = VoomBus()
        data1 = {1: 2}
        data2 = {'a': True}

        def func3():
            bus.publish("1")

        def func2():
            with bus.session_data(data2):
                func3()

        def func1():
            with bus.session_data(data1):
                func2()

        session = {}

        bus.subscribe(bus.ALL, lambda _: session.update(bus.session))

        func1()
        data1.update(data2)
        assert session == data1, session


class TestWithTransaction(unittest.TestCase):
    def test_nesting(self):
        bus = VoomBus()
        with bus.transaction() as (nested, state):
            assert not nested
            assert state is not None
            assert isinstance(state, BusState)

            with bus.transaction() as (nested2, state2):
                assert nested2
                assert state2 == state

    def test_send_on_exit(self):
        bus = VoomBus()
        self.msgs = []
        bus.subscribe(bus.ALL, self.msgs.append)

        with bus.transaction() as (nested, state):
            bus.publish(1, dict(a=1))
            assert not self.msgs
            assert isinstance(state, BusState)
            assert not state.is_queue_empty()

        assert self.msgs == [1]
        assert state.is_queue_empty()

    def test_send_on_error(self):
        bus = VoomBus()
        self.msgs = []
        bus.subscribe(bus.ALL, self.msgs.append)

        with nose.tools.assert_raises(ValueError):
            with bus.transaction() as (nested, state):
                bus.publish(1, dict(a=1))
                assert not self.msgs
                int("a")
                bus.publish(1, dict(a=1))

        assert self.msgs == [1]
        assert state.is_queue_empty()
        assert state.session == dict(a=1)

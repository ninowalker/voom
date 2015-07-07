from contextlib import contextmanager
from voom.context import MessageEnvelope, InvocationFailure, \
    SessionKeys, ReplyContext, TrxTLS
from voom.events import MessageForwarded
from voom.exceptions import AbortProcessing, BusError, InvalidAddressError, \
    InvalidStateError
from voom.local import CurrentThreadChannel
from voom.priorities import BusPriority # @UnusedImport
import bisect
import collections
import heapq
import inspect
import logging
import sys
import traceback

LOG = logging.getLogger(__name__)


class VoomBus(object):
    """
    A message dispatching service.
    """

    # : Key used to subscribe to ALL messages
    ALL = object()
    # : Key used to subscribe to ALL failures
    ERRORS = object()

    def __init__(self, verbose=False, raise_errors=False, loader=None):
        self.resetConfig()
        self._verbose = verbose
        self.raise_errors = raise_errors
        self._current_thread_channel = CurrentThreadChannel()
        if loader:
            self.loader = loader

    def resetConfig(self):
        """
        Revert to an uninitialized state. Useful for testing.
        """
        self._tls = TrxTLS()
        self._global_handlers = []
        self._error_handlers = []
        self._message_handlers = collections.defaultdict(list)
        self._loader = None
        self._loaded = False

    @property
    def loader(self):
        """A callable that will discover all the handlers for this bus. Defaults to None."""
        return self._loader

    @loader.setter
    def loader(self, value):
        if self._loader == value:
            return
        if self._loader:
            raise ValueError("Bus loader already initialized with another value: %s" % self._loader)
        self._loader = value
        self._loaded = False

    @property
    def trx(self):
        """
        Returns the active transaction.
        """
        return self._tls.state

    @property
    def message_context(self):
        return self.session

    @property
    def current_message(self):
        return self.trx.current_message

    @property
    def session(self):
        return self._tls.stack.frame

    @property
    def frame(self):
        return self._tls.stack.frame

    @contextmanager
    def using(self, data):
        """Provide a context manager for forwarding data to sessions or messages that will be sent
        or updating the session during a transaction
        """
        with self._tls.stack.push_frame() as f:
            f.update(data)
            yield

    @contextmanager
    def transaction(self):
        """This context manager provides a means for executing a block of code
        which may emit messages while deferring the actual send until execution ends.
        This is necessary, for example for handling persistence of a group of objects
        which emit their own messages.

        >>> bus = VoomBus()
        >>> with bus.transaction() as (nested, state):
        ...     save_and_publish()
        ...     save_and_publish()

        In the example above, both saves would have transpired before
        the first message is published.
        """
        # if we're nested, sub-messages
        if self.trx.is_running():
            yield True, self.trx
            return

        try:
            self.trx.begin()
            yield False, self.trx
        finally:
            self._consume()

    def publish(self, body, priority=None):
        self._load()
        self._send_message(MessageEnvelope(body, self.session), priority)

    def defer(self, msg):
        """Enqueue a message that is sent contingent on the current message
        completing all handlers without aborting."""
        self.trx._deferred.append(MessageEnvelope(msg, self.session))

    def get_reply_context(self):
        """Get a reply context suitable for passing to reply(). Use
        this to reply to a message outside of its Bus session."""
        return ReplyContext(self.session.get(SessionKeys.REPLY_TO),
                            self.session.get(SessionKeys.RESPONDER),
                            self._current_thread_channel)

    def reply(self, message, context=None):
        if not context:
            context = self.get_reply_context()

        reply_to = context.reply_to
        if not reply_to:
            raise InvalidAddressError("no reply responder is configured in the session")

        if reply_to == CurrentThreadChannel.ADDRESS:
            responder = context.thread_channel
        else:
            responder = context.responder

        if not responder:
            raise InvalidStateError("no reply responder is configured in the session")

        self.forward(responder,
                     reply_to,
                     message)

    def forward(self, sender, address, message):
        sender(address, message)
        self.publish(MessageForwarded(address, message))

    @property
    def thread_channel(self):
        return self._current_thread_channel

    def register(self, callback, priority=None, receiver_of=None):
        """Register a function as a handler.
        :param handler: a callable that accepts a single argument.
        :param priority: integer value indicating execution order (desc); if not
           provided, the callback is inspected for a `_priority` value.
        :param receiver_of: list or tuple of types; if not provided the
           callback must have a `_receiver_of` attribute.
        """
        if receiver_of is None:
            receiver_of = getattr(callback, '_receiver_of')
        if priority is None:
            priority = getattr(callback, '_priority', BusPriority.DEFAULT_PRIORITY)
        for msg_type in receiver_of:
            self.subscribe(msg_type, callback, priority)

    def subscribe(self, message_type, callback, priority=None):
        """Subscribe a callback to the given message type."""
        priority = priority if priority is not None else BusPriority.DEFAULT_PRIORITY
        LOG.debug("adding subscriber %s for %s", callback, message_type)
        handlers = self._get_handlers(message_type)
        old_item = None
        for _priority, _callback in handlers:
            if _callback != callback:
                continue
            if _priority == priority:
                # already registered
                LOG.debug("callback %s already registered", _callback)
                return
            old_item = (_priority, _callback)
            break
        if old_item:
            handlers.remove(old_item)
            LOG.info("callback %s re-registered with new priority. old=%s, new=%s",
                     _callback, _priority, priority)
        bisect.insort(handlers, (priority, callback))
        LOG.debug("Updated handlers: %s", handlers)

    def unsubscribe(self, message_type, callback):
        LOG.debug("removing subscriber %s for %s", callback, message_type)
        handlers = self._get_handlers(message_type)
        for priority, cb in handlers:
            if cb == callback:
                handlers.remove((priority, callback))
                return
        raise ValueError("callback not found")

    def invoke(self, callback, message_envelope):
        """Injection point for doing special things before or after the callback."""
        callback(message_envelope.body)

    def _send_message(self, message, priority=None):
        # if the queue is not empty, we are in a transaction,
        # so queue it up and it will be processed in the invoking loop.
        self.trx.enqueue(message, priority)

        if self.trx.is_running():
            return
        self._consume()

    def _consume(self):
        # this must be absolutely bullet proof
        # and we must leave this function with an
        # empty queue or we corrupt the bus.
        trx = self.trx
        trx.begin()
        msg = None
        try:
            for msg in trx.consume_messages():
                self.trx.current_message = msg
                with self._tls.stack.push_frame(msg.context):
                    self._dispatch(msg)
        except Exception, e:
            if self.raise_errors:
                raise
            raise BusError, (msg, e), sys.exc_info()[2] #@IgnorePep8
        finally:
            self.trx.current_message = None
            if not trx.is_queue_empty():
                LOG.error("Exiting send with queued item; something is terminally wrong.")
            self._tls.clear()

    def _dispatch(self, message, queue=None):
        if queue == None:
            queue = list(heapq.merge(self._global_handlers, self._message_handlers[type(message.body)]))
        try:
            for priority, callback in queue:
                try:
                    if self._verbose:
                        LOG.debug("invoking %s (priority=%s): %s", callback, priority, message)
                    self.invoke(callback, message)
                except AbortProcessing:
                    raise
                except Exception, ex:
                    LOG.exception("Callback failed: %s. Failed to send message: %s", callback, message)
                    if self.raise_errors:
                        raise
                    # avoid a circular loop
                    if queue == self._error_handlers:
                        continue
                    try:
                        self._send_error(message, callback, ex)
                    except AbortProcessing:
                        raise
                    except:
                        LOG.exception("Failed to send error. This generally should not happen.")

        except AbortProcessing:
            LOG.info("processing aborted.""")
            self.trx._deferred = []
            return

        while self.trx._deferred:
            LOG.info("sending queued_message")
            self._send_message(self.trx._deferred.pop(0))

    def _send_error(self, message, source, exception=None, tb=None):
        if exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback)[2:])
            # find out where send was called from:
        context = traceback.format_stack()[::-1]
        while "/voom/" in context[0]:
            context.pop(0)

        failure = InvocationFailure(message.body, exception, tb, context[::-1])
        env = MessageEnvelope(failure, message.context)
        self._dispatch(env, queue=self._error_handlers)

    def _get_handlers(self, message_type):
        if message_type is self.ALL:
            handlers = self._global_handlers
        elif message_type is self.ERRORS:
            handlers = self._error_handlers
        else:
            assert inspect.isclass(message_type), type(message_type)
            handlers = self._message_handlers[message_type]
        return handlers

    def _load(self):
        if self._loaded or not self._loader:
            return
        LOG.info("running loader...")
        try:
            self._loader()
            self._loaded = True
        except:
            LOG.exception("Failed to run loader!")
            raise

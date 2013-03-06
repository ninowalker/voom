'''
Created on Nov 17, 2012

@author: nino
'''

class BusError(Exception):
    """Propagates in the extremely rare situation that bus
    execution logics errors out.
    """ 
    def __init__(self, msg, cause):
        super(BusError, self).__init__(msg)
        self.cause = cause

class AbortProcessing(Exception):
    """A throwable that aborts all further handling of the current message,
    and purges any deferred messages. If handlers are organized sequentially to form
    a workflow, then this provides simple flow control.
    """
    pass

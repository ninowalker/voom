import functools
import os
from unittest.case import SkipTest

def no_jython(test):
    @functools.wraps(test)
    def wrapper(*args, **kwargs):
        if os.name != "java":
            return test(*args, **kwargs)
        raise SkipTest
    return wrapper
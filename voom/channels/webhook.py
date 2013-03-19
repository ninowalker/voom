'''
Created on Mar 2, 2013

@author: nino
'''

import warnings
import os


try:
    import requests
except ImportError:
    requests = None
except NameError:
    if os.name == "java":
        warnings.warn('python-requests is currently incompatible with Jython')
    else:
        raise
    requests = None


          
from voom.channels import Sender


class WebhookSender(Sender):
    default_encoding = "json"
    
    def __init__(self, **kwargs):
        self.post_kwargs = kwargs
    
    def _send(self, address, message, mimetype):
        kwargs = dict(mimetype=mimetype)
        kwargs.update(self.post_kwargs)        
        requests.post(address, message, **kwargs)

'''
Created on Mar 10, 2013

@author: nino
'''
import urlparse


class AMQPPublishingAddress(object):
    """
    amqp:///vhost/?routing_key=a_queue
    amqp:///vhost/?routing_key=hello&exchange=_an_exchange
    """

    def __init__(self, url):
        self.url = url
        # hackery because urlparse does silly things
        if url[0:4] == 'amqp':
            url = 'http' + url[4:]
        parts = urlparse.urlparse(url)
        if not parts.query:
            return
        params = urlparse.parse_qs(parts.query)
        for k in params:
            if len(params[k]) == 1:
                params[k] = params[k][0]
        
        self.vhost = parts.path.split("/")[1]
        self.routing_key = params.pop('routing_key')
        self.exchange = params.pop('exchange', '')
        self.extra_params = params

    def __str__(self):
        return self.url
                    
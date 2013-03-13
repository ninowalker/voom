'''
Created on Mar 10, 2013

@author: nino
'''
import unittest
from voom.codecs.mime import MIMEMessageEncoder, MIMEMessageDecoder


class TestMIME(unittest.TestCase):
    def test_1(self):
        addr = "where"
        arr = range(0, 10)
        msg_str = MIMEMessageEncoder()(addr, arr)
        print msg_str
        payloads, headers = MIMEMessageDecoder()(msg_str)
        assert payloads == [arr]
        assert headers.get('To') == addr

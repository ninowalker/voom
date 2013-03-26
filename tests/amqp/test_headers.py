'''
Created on Mar 25, 2013

@author: jonathan
'''
import unittest
from voom.amqp.headers import Headers
from nose.tools import assert_raises


class Test(unittest.TestLoader):
    def test_set_get(self):
        v = Headers()
        v['KeY1'] = 'Value1'
        assert v['key1'] == 'Value1'
        assert v.keys() == ['KeY1']

    def test_constructor(self):
        v = Headers({'key': 'value'})
        assert v['key'] == 'value'
        assert v['KEy'] == 'value'

        v['KeY1'] = 'Value1'
        assert v['key1'] == 'Value1', v['key1']
        assert set(v.keys()) == set(['key', 'KeY1']), v.keys()

    def test_key_error(self):
        v = Headers()
        v['cat'] = 'meow'
        assert_raises(KeyError, v.__getitem__, 'dog')

'''
Created on Mar 25, 2013

@author: jonathan
'''

import collections


class Headers(collections.MutableMapping):
    """
    A dictionary implementation that allows case-insensitive
    lookups while preserving the case of the original key names.
    """
    def __init__(self, initial=None):
        self._dict = {}

        if not initial:
            return

        for k, v in initial.items():
            self[k] = v

    def __contains__(self, key):
        return key.lower() in self._dict

    def __iter__(self):
        for k in self._dict:
            yield self._dict[k][0]

    def __len__(self):
        return len(self._dict)

    def __getitem__(self, key):
        return self._dict[key.lower()][1]

    def __setitem__(self, key, value):
        self._dict[key.lower()] = (key, value)

    def __delitem__(self, key):
        del self._dict[key.lower()]

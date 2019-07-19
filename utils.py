import logging
import pickle
from collections import deque

class CacheManager:
    '''
    Provides a list style cache object with a fixed length.

    :param name: str: Unique identifer for the cache vs other cache objects in use.
    :param maxlen: int: Maximum entries the cache obj will hold before FIFO.

    :method add: Add data to the cache, can be str, int, float or list/tuple.
    :method write: Writes the cache out to a file

    :attr cache: Shows contents of cache. Allows for interaction with embedded deque.

    If the cache exist with different length, the data will be moved to the new cache
    and truncated if required.
    '''

    def __init__(self, name, maxlen=None):
        self.filename = '.' + name + '_cache.pkl'
        self.maxlen = maxlen
        self.cache = None
        self.cache = self._get_pickle(self.filename)
        if not self.cache:
            self.cache = deque(maxlen=self.maxlen)
        else:
            if self.cache.maxlen != self.maxlen:
                self._adjust_cache_size()

    def _adjust_cache_size(self):
        '''
        Change the maximum length of a deque or DictCache object while preserving data.
        '''
        self._new_cache = deque(maxlen=self.maxlen)
        self._new_cache.extend((self.cache))
        self.cache = self._new_cache

    def _get_pickle(self, filename):
        try:
            with open(self.filename, 'rb') as f:
                unpickled_data = pickle.load(f)
        except IOError:
            unpickled_data = None
        return unpickled_data

    def add(self, data):
        if isinstance(data, (list, tuple)):
            self.cache.extend(data)
        else:
            self.cache.append(data)

    def __contains__(self, item):
        if item in self.cache:
            return True
        else:
            return None

    def write(self):
        try:
            with open(self.filename, 'wb') as f:
                pickle.dump(self.cache, f, pickle.HIGHEST_PROTOCOL)
        except:
            logging.error('Error, could not write pickle file')
        #print('Saved cache to disk: {}'.format(self.filename))

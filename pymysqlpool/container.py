# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : container.py
# Date   : 2017-06-16 07-12
# Version: 0.0.1
# Description: pool container, thread-safe


import threading
import logging
from itertools import chain
from collections import deque

__version__ = '0.0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')


class PoolContainer(object):
    def __init__(self, max_pool_size, queue_mode='lifo'):
        self._pool_lock = threading.RLock()
        self._free_items = deque()
        self._busy_items = list()
        self._max_pool_size = max_pool_size
        self._queue_mode = queue_mode

    def __repr__(self):
        return '<{0.__class__.__name__} mode={0._queue_mode}, size=({0._max_pool_size}, ' \
               '{0.pool_size}, {0.free_size})>'.format(self)

    def __iter__(self):
        with self._pool_lock:
            yield from chain(self._busy_items, self._free_items)

    def return_item(self, item):
        with self._pool_lock:
            pass

    def borrow_item(self, wait_timeout):
        with self._pool_lock:
            pass

    @property
    def pool_size(self):
        return 0

    @property
    def free_size(self):
        return 0


if __name__ == '__main__':
    c = PoolContainer(10)
    print(c)

# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: MIT License
# File   : test_container.py
# Date   : 2017-06-16 09-22
# Version: 0.1
# Description: description of this file.

import random
import threading
import logging

import time

from pymysqlpool.pool import *

logging.basicConfig(format='[%(asctime)s][%(name)s][%(module)s.%(lineno)d][%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

container = PoolContainer(10)


def test_add_new_items():
    for _ in range(100):
        try:
            container.add(random.randint(10, 100000))
        except PoolIsFullException:
            print('Full')


def test_get_return():
    test_add_new_items()

    for _ in range(100):
        item = container.get(wait_timeout=10)
        # print(item)
        print((container.pool_size, container.free_size), item)
        container.return_(item)


def worker(id_):
    print('[{}] try to get a free item'.format(id_))
    item = container.get(wait_timeout=60)
    print('[{}] get item: {}'.format(id_, item))
    time.sleep(random.randint(1, 10) / random.randint(10, 20))
    container.return_(item)


def test_with_multi_threads():
    test_add_new_items()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1000)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    test_add_new_items()
    # test_get_return()
    # test_with_multi_threads()

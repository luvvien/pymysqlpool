# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: MIT License
# File   : test_pool.py
# Date   : 2017-06-15 15-05
# Version: 0.1
# Description: description of this file.


import datetime
import logging
import string
import threading

import random

from pymysqlpool import MySQLConnectionPool

logging.basicConfig(format='[%(asctime)s][%(name)s][%(module)s.%(lineno)d][%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

config = {
    'pool_name': 'yunos_new',
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'chri',
    'database': 'yunos_new',
    # 'pool_resize_boundary': 30,
    # 'wait_timeout': 120,
    # 'enable_auto_resize': True,
    # 'max_pool_size': 10
}

conn_pool = MySQLConnectionPool(**config)
conn_pool.connect()

insert_sql = 'INSERT INTO folder (name, icon_url, create_at) VALUES (%s, %s, %s)'
select_sql = 'SELECT * FROM folder ORDER BY id DESC LIMIT 10'
update_sql = ''
delete_sql = ''
truncate_sql = 'TRUNCATE folder'

name_factory = lambda: ''.join(random.sample(string.ascii_letters, random.randint(2, 10)))


def test_insert_one():
    with conn_pool.pool_cursor() as cursor:
        name = name_factory()
        result = cursor.execute_one(insert_sql,
                                    ('folder_{}'.format(name), 'icon_{}.png'.format(name), datetime.datetime.now()))
        print(result)
        # _ = result
        # print(cursor.connection)
        # time.sleep(.1)


def test_insert_many():
    with conn_pool.pool_cursor() as cursor:
        folders = []

        for _ in range(10):
            name = name_factory()
            folders.append(('folder_{}'.format(name), 'icon_{}.png'.format(name), datetime.datetime.now()))
        result = cursor.execute_many(insert_sql, folders)
        print(result)


def test_query():
    with conn_pool.pool_cursor() as cursor:
        for item in sorted(cursor.query(select_sql), key=lambda x: x['id']):
            print(item)
            # _ = item


def test_truncate():
    with conn_pool.pool_cursor() as cursor:
        cursor.execute_one(truncate_sql)


def test_with_multi_threading():
    test_truncate()
    threads = [threading.Thread(target=test_insert_one) for _ in range(1000)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    test_query()


def test_borrow_connections():
    for _ in range(11):
        # with conn_pool.pool_cursor() as c:
        print(conn_pool.pool_cursor().connection)


def test_borrow_return_connections():
    for _ in range(1000):
        with conn_pool.pool_cursor() as cursor:
            print(cursor.connection)


def test_single_thread_insert():
    test_truncate()
    for _ in range(1000):
        test_insert_one()

    test_query()


if __name__ == '__main__':
    import time

    start = time.time()
    # test_insert_many()
    # test_query()
    test_insert_one()
    # test_with_multi_threading()
    # test_single_thread_insert()
    # test_borrow_connections()
    # test_borrow_return_connections()
    print('Time consuming is {}'.format(time.time() - start))

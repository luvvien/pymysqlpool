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
import pandas as pd
import random

from pymysqlpool import ConnectionPool

logging.basicConfig(format='[%(asctime)s][%(name)s][%(module)s.%(lineno)d][%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.ERROR)

config = {
    'pool_name': 'test',
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'chris',
    'database': 'test',
    'pool_resize_boundary': 50,
    'enable_auto_resize': True,
    # 'max_pool_size': 10
}


def conn_pool():
    # pool = MySQLConnectionPool(**config)
    pool = ConnectionPool(**config)
    # pool.connect()
    # print(pool)
    return pool


insert_sql = 'INSERT INTO folder (name, icon_url, create_at) VALUES (%s, %s, %s)'
select_sql = 'SELECT * FROM folder ORDER BY id DESC LIMIT 10'
update_sql = ''
delete_sql = ''
truncate_sql = 'TRUNCATE folder'

name_factory = lambda: ''.join(random.sample(string.ascii_letters, random.randint(2, 10)))


def test_insert_one():
    with conn_pool().connection() as conn:
        name = name_factory()
        result = conn.cursor().execute(insert_sql, ('folder_{}'.format(name),
                                                    'icon_{}.png'.format(name),
                                                    datetime.datetime.now()))
        conn.commit()
        # print(result)
        # _ = result
        # print(cursor.connection)
        # time.sleep(.1)


def test_insert_many():
    with conn_pool().cursor() as cursor:
        folders = []

        for _ in range(10):
            name = name_factory()
            folders.append(('folder_{}'.format(name), 'icon_{}.png'.format(name), datetime.datetime.now()))
        result = cursor.executemany(insert_sql, folders)
        print(result)


def test_query():
    with conn_pool().cursor() as cursor:
        cursor.execute(select_sql)
        for item in sorted(cursor, key=lambda x: x['id']):
            print(item)
            # _ = item


def test_truncate():
    with conn_pool().cursor() as cursor:
        cursor.execute(truncate_sql)


def test_with_multi_threading():
    test_truncate()

    def task(n):
        print('In thread {}'.format(threading.get_ident()))
        for _ in range(n):
            test_insert_one()

    threads = [threading.Thread(target=task, args=(100,)) for _ in range(50)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    test_query()


def test_borrow_return_connections():
    for _ in range(100000):
        with conn_pool().connection() as connection:
            _ = connection


def test_single_thread_insert():
    # with ping: 11s
    # without ping 11s
    test_truncate()
    for _ in range(5000):
        test_insert_one()

    test_query()


def test_query_with_pandas():
    import pandas as pd

    with conn_pool().connection() as conn:
        r = pd.read_sql(select_sql, conn)
        print(r)


if __name__ == '__main__':
    import time

    start = time.time()
    # test_insert_many()
    # test_query()
    # test_insert_one()
    # test_query_with_pandas()
    test_with_multi_threading()
    test_single_thread_insert()
    # test_borrow_return_connections()
    print('Time consuming is {}'.format(time.time() - start))

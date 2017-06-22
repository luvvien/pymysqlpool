# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : test_example.py
# Date   : 2017-06-18 01-23
# Version: 0.0.1
# Description: simple test.


import logging
import string
import threading

import pandas as pd
import random

from pymysqlpool import ConnectionPool

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

logging.basicConfig(format='[%(asctime)s][%(name)s][%(module)s.%(lineno)d][%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)


def connection_pool():
    # Return a connection pool instance
    pool = ConnectionPool(**config)
    # pool.connect()
    return pool


def test_pool_cursor(cursor_obj=None):
    cursor_obj = cursor_obj or connection_pool().cursor()
    with cursor_obj as cursor:
        print('Truncate table user')
        cursor.execute('TRUNCATE user')

        print('Insert one record')
        result = cursor.execute('INSERT INTO user (name, age) VALUES (%s, %s)', ('Jerry', 20))
        print(result, cursor.lastrowid)

        print('Insert multiple records')
        users = [(name, age) for name in ['Jacky', 'Mary', 'Micheal'] for age in range(10, 15)]
        result = cursor.executemany('INSERT INTO user (name, age) VALUES (%s, %s)', users)
        print(result)

        print('View items in table user')
        cursor.execute('SELECT * FROM user')
        for user in cursor:
            print(user)

        print('Update the name of one user in the table')
        cursor.execute('UPDATE user SET name="Chris", age=29 WHERE id = 16')
        cursor.execute('SELECT * FROM user ORDER BY id DESC LIMIT 1')
        print(cursor.fetchone())

        print('Delete the last record')
        cursor.execute('DELETE FROM user WHERE id = 16')


def test_pool_connection():
    with connection_pool().connection(autocommit=True) as conn:
        test_pool_cursor(conn.cursor())


def test_with_pandas():
    with connection_pool().connection() as conn:
        df = pd.read_sql('SELECT * FROM user', conn)
        print(df)


def delete_users():
    with connection_pool().cursor() as cursor:
        cursor.execute('TRUNCATE user')


def add_users(users, conn):
    def execute(c):
        c.cursor().executemany('INSERT INTO user (name, age) VALUES (%s, %s)', users)
        c.commit()

    if conn:
        execute(conn)
        return
    with connection_pool().connection() as conn:
        execute(conn)


def add_user(user, conn=None):
    def execute(c):
        c.cursor().execute('INSERT INTO user (name, age) VALUES (%s, %s)', user)
        c.commit()

    if conn:
        execute(conn)
        return
    with connection_pool().connection() as conn:
        execute(conn)


def list_users():
    with connection_pool().cursor() as cursor:
        cursor.execute('SELECT * FROM user ORDER BY id DESC LIMIT 5')
        print('...')
        for x in sorted(cursor, key=lambda d: d['id']):
            print(x)


def random_user():
    name = "".join(random.sample(string.ascii_lowercase, random.randint(4, 10))).capitalize()
    age = random.randint(10, 40)
    return name, age


def worker(id_, batch_size=1, explicit_conn=True):
    print('[{}] Worker started...'.format(id_))

    def do(conn=None):
        for _ in range(batch_size):
            add_user(random_user(), conn)

    if not explicit_conn:
        do()
        return

    with connection_pool().connection() as c:
        do(c)

    print('[{}] Worker finished...'.format(id_))


def bulk_worker(id_, batch_size=1, explicit_conn=True):
    print('[{}] Bulk worker started...'.format(id_))

    def do(conn=None):
        add_users([random_user() for _ in range(batch_size)], conn)
        time.sleep(3)

    if not explicit_conn:
        do()
        return

    with connection_pool().connection() as c:
        do(c)

    print('[{}] Worker finished...'.format(id_))


def test_with_single_thread(batch_number, batch_size, explicit_conn=False, bulk_insert=False):
    delete_users()
    wk = worker if not bulk_insert else bulk_worker
    for i in range(batch_number):
        wk(i, batch_size, explicit_conn)
    list_users()


def test_with_multi_threads(batch_number=1, batch_size=1000, explicit_conn=False, bulk_insert=False):
    delete_users()

    wk = worker if not bulk_insert else bulk_worker

    threads = []
    for i in range(batch_number):
        t = threading.Thread(target=wk, args=(i, batch_size, explicit_conn))
        threads.append(t)
        t.start()

    [t.join() for t in threads]
    list_users()


if __name__ == '__main__':
    import time

    start = time.perf_counter()
    test_pool_cursor()
    test_pool_connection()

    test_with_pandas()
    test_with_multi_threads(20, 10, True, bulk_insert=True)
    test_with_single_thread(1, 10, True, bulk_insert=True)
    elapsed = time.perf_counter() - start
    print('Elapsed time is: "{}"'.format(elapsed))

# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : test_pool.py
# Date   : 2017-06-15 15-05
# Version: 0.0.1
# Description: description of this file.


import logging
import random
import threading
from time import sleep

from pymysqlpool.pool import MySQLConnectionPool

logging.basicConfig(format='[%(asctime)s][%(name)s][%(module)s.%(lineno)d][%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

config = {
    'pool_name': 'test_pool',
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'chris',
    'database': 'test',
    'wait_connection_timeout': 6,
}

conn_pool = MySQLConnectionPool(**config)
conn_pool.connect()


def test_insert_one():
    with conn_pool.pool_cursor() as cursor:
        sql = 'INSERT INTO user (name, age, school, address, email, phone) ' \
              'VALUES (%(name)s, %(age)s, %(school)s, %(address)s, %(email)s, %(phone)s)'
        print(conn_pool.pool_size, conn_pool.active_connections_size, cursor.connection)
        user = {
            'name': 'Chris',
            'age': random.randint(10, 1000),
            'school': 'Guess what',
            'email': 'chris@mail.com',
            'address': 'where is it',
            'phone': '157518685299'
        }
        # r = cursor.execute_one(sql, user)
        # print(cursor.last_row_id)
        print(cursor.connection)
        sleep(2)
        # print('Now, sleep is over: {}'.format(cursor.connection))


def test_insert_many():
    with conn_pool.pool_cursor() as cursor:
        sql = 'INSERT INTO user (name, age, school, address, email, phone) ' \
              'VALUES (%(name)s, %(age)s, %(school)s, %(address)s, %(email)s, %(phone)s)'
        print(cursor.connection)
        users = [{
                     'name': 'Chris',
                     'age': age,
                     'school': 'Guess what',
                     'email': 'chris@mail.com',
                     'address': 'where is it',
                     'phone': '157518685299'
                 } for age in range(100, 200)]
        r = cursor.execute_many(sql, users)
        print(cursor.last_row_id)
        print(r)


def test_query():
    with conn_pool.pool_cursor() as cursor:
        for item in cursor.query('SELECT * FROM user'):
            # print(item)
            _ = item


def test_with_multi_threading():
    threads = [threading.Thread(target=test_insert_one) for _ in range(20)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()


def test_borrow_connections():
    for _ in range(11):
        # with conn_pool.pool_cursor() as c:
        print(conn_pool.pool_cursor().connection)
        print(conn_pool.pool_size, conn_pool.active_connections_size)


def test_borrow_return_connections():
    for _ in range(1000):
        with conn_pool.pool_cursor() as cursor:
            print(cursor.connection)


if __name__ == '__main__':
    # test_insert_many()
    # test_query()
    # test_insert_one()
    test_with_multi_threading()
    # test_borrow_connections()
    # test_borrow_return_connections()

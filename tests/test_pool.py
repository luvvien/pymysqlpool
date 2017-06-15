# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : test_pool.py
# Date   : 2017-06-15 15-05
# Version: 0.0.1
# Description: description of this file.


import logging
from pymysqlpool.pool import MySQLConnectionPool

logging.basicConfig(level=logging.DEBUG)


# root:pymysql1507@192.168.2.108:3306

def test_connection_pool():
    pool = MySQLConnectionPool('test_db', '192.168.2.108', user='root', password='pymysql1507',
                               port=3306)
    pool.open()

    for _ in range(1):
        conn = pool.borrow_connection()
        pool.return_connection(conn)

    # pool.close()


if __name__ == '__main__':
    test_connection_pool()

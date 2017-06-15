# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : conn.py
# Date   : 2017-06-15 14-09
# Version: 0.0.1
# Description: description of this file.

import logging

__version__ = '0.0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')


class PoolCursor(object):
    def __init__(self, conn_pool):
        self._conn_pool = conn_pool
        self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self._conn_pool.return_connection(self.connection)

    @property
    def connection(self):
        if self._conn is None:
            self._conn = self._conn_pool.borrow_connection()
        return self._conn

    def execute_one(self, sql, args=None):
        try:
            with self.connection.cursor() as cursor:
                logger.debug('[{}][execute_one] sql: "{}"'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args)))
                cursor.execute(sql, args)

            self.connection.commit()
            return cursor.lastrowid
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

    def execute_many(self, sql, args):
        try:
            with self.connection.cursor() as cursor:
                logger.debug('[{}][execute_many] sql: "{}"'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args)))
                cursor.executemany(sql, args)

            self.connection.commit()
            return cursor.lastrowid
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

    def query(self, sql, args=None):
        try:
            with self.connection.cursor() as cursor:
                logger.debug('[{}][query] sql: "{}"'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args)))
                cursor.execute(sql, args)
                yield from cursor

        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

    def transact(self, group_sql_args):
        """
        ((sql1, args1), (sql2, args2))...
        """
        try:
            self.connection.begin()

            with self.connection.cursor() as cursor:
                for sql, args in group_sql_args:
                    logger.debug('[{}][transact] sql: "{}"'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args)))
                    cursor.execute(sql, args)

            self.connection.commit()
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

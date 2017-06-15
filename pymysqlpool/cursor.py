# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : conn.py
# Date   : 2017-06-15 14-09
# Version: 0.0.1
# Description: pool cursor class

import logging

__version__ = '0.0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')


class PoolCursor(object):
    """Cursor class, execute sql expressions here
    """

    def __init__(self, conn_pool, cursor_class):
        self._conn_pool = conn_pool
        self._conn = None
        self._cursor_class = cursor_class
        self._last_row_id = 0

    def __repr__(self):
        return '<PoolCursor object at 0x{:0x}, connection is {}>'.format(id(self), self.connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            logger.error(exc_tb)

        self.close()

    def close(self):
        if self._conn:
            self._conn_pool.return_connection(self.connection)

    @property
    def last_row_id(self):
        return self._last_row_id

    @property
    def connection(self):
        if self._conn is None:
            self._conn = self._conn_pool.borrow_connection(self._conn_pool.wait_connection_timeout)
        return self._conn

    def execute_one(self, sql, args=None):
        try:
            with self.connection.cursor(self._cursor_class) as cursor:
                logger.debug('[{}][execute_one] sql: "{}"'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args)))
                result = cursor.execute(sql, args)

            self.connection.commit()
            self._last_row_id = cursor.lastrowid
            return result
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

    def execute_many(self, sql, args):
        try:
            with self.connection.cursor(self._cursor_class) as cursor:
                logger.debug(
                    '[{}][execute_many] sql: "{}..."'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args[0])))
                result = cursor.executemany(sql, args)

            self.connection.commit()
            self._last_row_id = cursor.lastrowid + result - 1
            return result
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

    def query(self, sql, args=None):
        try:
            with self.connection.cursor(self._cursor_class) as cursor:
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
            logger.info("[{}][transact] start transaction".format(self._conn_pool.pool_name))
            self.connection.begin()

            with self.connection.cursor(self._cursor_class) as cursor:
                for sql, args in group_sql_args:
                    logger.debug(
                        '[{}][transact] sql: "{}"'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args)))
                    cursor.execute(sql, args)

            self.connection.commit()
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()

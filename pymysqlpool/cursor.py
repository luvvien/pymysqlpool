# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: MIT License
# File   : conn.py
# Date   : 2017-06-15 14-09
# Version: 0.1
# Description: pool cursor class

import logging

__version__ = '0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')

__all__ = ['PoolCursor']


class PoolCursor(object):
    """Cursor class, execute sql expressions here.

    Possible usage (not recommended usage on the client side!):
    with PoolCursor(conn_pool, DictCursor) as cursor:
        cursor.execute_one(sql, args)
        cursor.execute_many(sql, args)
        cursor.query(sql, args)
    """

    def __init__(self, conn_pool, cursor_class):
        self._conn_pool = conn_pool
        self._conn = None
        self._cursor_class = cursor_class

    def __repr__(self):
        return '<PoolCursor object at 0x{:0x}, connection is {}>'.format(
            id(self), self.connection)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            logger.error(exc_tb, exc_info=True)
            self.connection.rollback()
        else:
            self.connection.commit()

        self.close()

    def close(self):
        self._conn_pool.return_connection(self.connection)

    @property
    def connection(self):
        if self._conn is None:
            self._conn = self._conn_pool.borrow_connection()

        return self._conn

    def execute_one(self, sql, args=None):
        """
        Execute one sql expression, return a tuple: (affected_results, lastrowid)
        """
        try:
            with self.connection.cursor(self._cursor_class) as cursor:
                logger.debug(
                    '[{}][execute_one] sql: "{}"'.format(
                        self._conn_pool.pool_name,
                        cursor.mogrify(sql, args)))

                result = cursor.execute(sql, args)
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()
        else:
            self.connection.commit()
            return result, cursor.lastrowid

    def execute_many(self, sql, args):
        """
        Execute many, return a tuple: (affected_results, lastrowid)
        """
        try:
            with self.connection.cursor(self._cursor_class) as cursor:
                logger.debug(
                    '[{}][execute_many] sql: "{}..."'.format(self._conn_pool.pool_name, cursor.mogrify(sql, args[0])))
                result = cursor.executemany(sql, args)
        except Exception as err:
            logger.error(err, exc_info=True)
            self.connection.rollback()
        else:
            self.connection.commit()
            return result, result + cursor.lastrowid - 1

    def query(self, sql, args=None):
        """Return a generator for lazy loading"""
        try:
            with self.connection.cursor(self._cursor_class) as cursor:
                logger.debug(
                    '[{}][query] sql: "{}"'.format(
                        self._conn_pool.pool_name,
                        cursor.mogrify(
                            sql,
                            args)))
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
            logger.info(
                "[{}][transact] start transaction".format(
                    self._conn_pool.pool_name))
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

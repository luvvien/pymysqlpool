# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: MIT License
# File   : pool.py
# Date   : 2017-06-15 14-09
# Version: 0.1
# Description: connection pool manager.

import logging
import threading
import contextlib

from pymysql import Connection
from pymysql.cursors import DictCursor, Cursor

from pymysqlpool.cursor import PoolCursor
from pymysqlpool.pool import PoolContainer, PoolIsFullException, PoolIsEmptyException

__version__ = '0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')

__all__ = ['NoFreeConnectionFoundError', 'MySQLConnectionPool']


class NoFreeConnectionFoundError(Exception):
    pass


class PoolBoundaryExceedsError(Exception):
    pass


class MySQLConnectionPool(object):
    """
    A connection pool manager.

    Typical usage are as follows:
    Typical usage:
        >>> pool = MySQLConnectionPool('test_pool', 'localhost', 'username', 'password', 'database', max_pool_size=10)
        >>> with pool.cursor() as cursor:
        >>>     cursor.execute_one('INSERT INTO user (name, password) VALUES (%s, %s)', ('chris', 'password'))
        >>>     cursor.execute_many('INSERT INTO user (name, password) VALUES (%s, %s)', [('chris', 'password'), ('chris', 'password')])
        >>>     print(list(cursor.query('SELECT * FROM user')))
    """

    def __init__(self, pool_name, host=None, user=None, password="", database=None, port=3306,
                 charset='utf8', use_dict_cursor=True, max_pool_size=16,
                 step_size=2, enable_auto_resize=False, auto_resize_scale=1.5,
                 pool_resize_boundary=48,
                 wait_timeout=60, **kwargs):
        """
        Initialize the connection pool.

        :param pool_name: a unique pool_name for this connection pool.
        :param host: host to your database server
        :param user: username to your database server
        :param password: password to access the database server
        :param database: select a default database(optional)
        :param port: port of your database server
        :param charset: default charset is 'utf8'
        :param use_dict_cursor: whether to use a dict cursor instead of a default one
        :param max_pool_size: maximum connection pool size (max pool size can be changed dynamically)
        :param step_size: increase `step_size` connections when call the extend method
        :param enable_auto_resize: if set to True, the max_pool_size will be changed
        :param pool_resize_boundary: !!this is related to the max connections of your mysql server!!
        :param auto_resize_scale: `max_pool_size * auto_resize_scale` is the new max_pool_size.
                                The max_pool_size will be changed dynamically only if `enable_auto_resize` is True.
        :param wait_timeout: wait several seconds each time when we try to get a free connection
        :param kwargs: other keyword arguments to be passed to `pymysql.Connection`
        """
        # config for a database connection
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._port = port
        self._charset = charset
        self._cursor_class = DictCursor if use_dict_cursor else Cursor
        self._other_kwargs = kwargs

        # config for the connection pool
        self._pool_name = pool_name
        self._max_pool_size = max_pool_size if max_pool_size < pool_resize_boundary else pool_resize_boundary
        self._step_size = step_size
        self._enable_auto_resize = enable_auto_resize
        self._pool_resize_boundary = pool_resize_boundary
        if auto_resize_scale < 1:
            raise ValueError(
                "Invalid scale {}, must be bigger than 1".format(auto_resize_scale))

        self._auto_resize_scale = int(round(auto_resize_scale, 0))
        self.wait_timeout = wait_timeout
        self._pool_container = PoolContainer(self._max_pool_size)

        self.__safe_lock = threading.RLock()
        self.__is_killed = False
        self.__is_connected = False

    def __repr__(self):
        return '<MySQLConnectionPool object at 0x{:0x}, ' \
               'name={!r}, size={!r}>'.format(
                   id(self), self.pool_name, (self.pool_size, self.free_size))

    def __enter__(self):
        self.connect()
        return PoolCursor(self, self._cursor_class)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            logger.error(exc_tb, exc_info=True)
            for conn in self:
                conn.rollback()
        else:
            for conn in self:
                conn.commit()

        self.close()

    def __del__(self):
        self.close()

    def __iter__(self):
        """Iterate each connection item"""
        return iter(self._pool_container)

    @property
    def pool_name(self):
        return self._pool_name

    @property
    def pool_size(self):
        return self._pool_container.pool_size

    @property
    def free_size(self):
        return self._pool_container.free_size

    def cursor(self, use_dict_cursor=True):
        cursor_class = DictCursor if use_dict_cursor else self._cursor_class
        return PoolCursor(self, cursor_class)

    @contextlib.contextmanager
    def connection(self):
        conn = self.borrow_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    def connect(self):
        """Connect to this connection pool
        """
        if self.__is_connected:
            return

        logger.info('[{}] Connect to connection pool'.format(self.pool_name))

        test_conn = self._create_connection()
        try:
            test_conn.ping()
        except Exception as err:
            raise err
        else:
            with self.__safe_lock:
                self.__is_connected = True

            self._extend_connection_pool()
        finally:
            test_conn.close()

    def close(self):
        """Close this connection pool"""
        logger.info('[{}] Close connection pool'.format(self.pool_name))
        with self.__safe_lock:
            if self.__is_killed is True:
                return True

        self._free()

        with self.__safe_lock:
            self.__is_killed = True

    def borrow_connection(self, block=False):
        """
        Get a free connection item from current pool
        """
        conn_item = self._borrow(block)
        if conn_item:
            # logger.debug('[{}] borrowed a connection from the connection pool'.format(self.pool_name))
            return conn_item

        if self.pool_size < self._max_pool_size:
            self._extend_connection_pool()
            return self.borrow_connection(True)

        if self._enable_auto_resize is False:
            # raise NoFreeConnectionFoundError('[{}] Cannot find a free connection'.format(self.pool_name))
            return self.borrow_connection(True)

        # Wait until a new free connection is found
        if self.pool_size >= self._pool_resize_boundary:
            return self.borrow_connection(True)

        # Resize the pool automatically
        with self.__safe_lock:
            self._max_pool_size *= self._auto_resize_scale
            self._max_pool_size = self._max_pool_size if self._max_pool_size < self._pool_resize_boundary else \
                self._pool_resize_boundary

            self._pool_container.max_pool_size = self._max_pool_size
            self._extend_connection_pool()
            return self.borrow_connection(True)

    def _borrow(self, block=True):
        try:
            conn_item = self._pool_container.get(block, self.wait_timeout)
        except PoolIsEmptyException:
            return None
        else:
            # check if the connection is alive or not
            conn_item.ping(reconnect=True)
            return conn_item

    def return_connection(self, conn_item):
        """Return a connection to the pool"""
        return self._pool_container.return_(conn_item)

    def _extend_connection_pool(self):
        """
        Extend the connection pool, create several connections here.
        """
        # Create several new connections
        logger.debug('[{}] Extend connection pool, '
                     'current size is {}, '
                     'max size is {}'.format(self.pool_name, (self.pool_size, self._max_pool_size),
                                             self._max_pool_size))
        for i in range(self._step_size):
            try:
                conn_item = self._create_connection()
                conn_item.connect()
            except Exception as err:
                logger.error(err)
                continue

            try:
                self._pool_container.add(conn_item)
            except PoolIsFullException:
                logger.debug(
                    '[{}] Connection pool is full now'.format(self.pool_name))
                if self.pool_size > self._pool_resize_boundary:
                    raise PoolBoundaryExceedsError(
                        'Pool boundary exceeds: {}'.format(self._pool_resize_boundary))
                else:
                    break

    def _free(self):
        """
        Release all the connections in the pool
        """
        for conn_item in self:
            try:
                conn_item.close()
            except Exception as err:
                _ = err

    def _create_connection(self):
        """Create a pymysql connection object
        """
        conn = Connection(self._host,
                          self._user,
                          self._password,
                          self._database,
                          self._port,
                          charset=self._charset,
                          cursorclass=self._cursor_class,
                          **self._other_kwargs)
        return conn

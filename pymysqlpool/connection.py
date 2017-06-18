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

from pymysql.connections import Connection
from pymysql.cursors import DictCursor, Cursor

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
    """

    def __init__(self, pool_name, host=None, user=None, password="", database=None, port=3306,
                 charset='utf8', use_dict_cursor=True, max_pool_size=16,
                 step_size=2, enable_auto_resize=False, auto_resize_scale=1.5,
                 pool_resize_boundary=48,
                 wait_timeout=60, defer_connect_pool=False, **kwargs):
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
        :param defer_connect_pool: don't connect to pool on contruction, wait for explicit call. Default is False.
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

        if not defer_connect_pool:
            self.connect()

    def __repr__(self):
        return '<MySQLConnectionPool object at 0x{:0x}, ' \
               'name={!r}, size={!r}>'.format(id(self), self.pool_name, (self.pool_size, self.free_size))

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

    @contextlib.contextmanager
    def cursor(self, cursor=None):
        """Shortcut to get a cursor object from a free connection.
        It's not that efficient to get cursor object in this way for
        too many times.
        """
        with self.connection() as conn:
            assert isinstance(conn, Connection)
            old_value = conn.get_autocommit()
            conn.autocommit(True)
            cursor = conn.cursor(cursor)

            try:
                yield cursor
            except Exception as err:
                conn.rollback()
                logger.error(err, exc_info=True)
            finally:
                conn.autocommit(old_value)
                cursor.close()

    @contextlib.contextmanager
    def connection(self, autocommit=False):
        conn = self.borrow_connection()
        assert isinstance(conn, Connection)
        old_value = conn.get_autocommit()
        conn.autocommit(autocommit)
        try:
            yield conn
        except Exception as err:
            logger.error(err, exc_info=True)
        finally:
            conn.autocommit(old_value)
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
        connection = self._borrow(block)
        if connection:
            # logger.debug('[{}] borrowed a connection from the connection pool'.format(self.pool_name))
            return connection

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
            connection = self._pool_container.get(block, self.wait_timeout)
        except PoolIsEmptyException:
            return None
        else:
            # check if the connection is alive or not
            connection.ping(reconnect=True)
            return connection
            # return self._create_connection()

    def return_connection(self, connection):
        """Return a connection to the pool"""
        return self._pool_container.return_(connection)
        # connection.close()

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
                connection = self._create_connection()
                # connection.connect()
            except Exception as err:
                logger.error(err)
                continue

            try:
                self._pool_container.add(connection)
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
        for connection in self:
            try:
                connection.close()
            except Exception as err:
                _ = err

    def _create_connection(self):
        """Create a pymysql connection object
        """
        return Connection(host=self._host,
                          user=self._user,
                          password=self._password,
                          database=self._database,
                          port=self._port,
                          charset=self._charset,
                          cursorclass=self._cursor_class,
                          **self._other_kwargs)

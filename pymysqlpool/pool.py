# -*-coding: utf-8-*-
# Author : Christopher Lee
# License: Apache License
# File   : pool.py
# Date   : 2017-06-15 14-09
# Version: 0.0.1
# Description: connection pool manager.

import datetime
import logging
import queue

import threading
import time
from collections import deque
from itertools import chain

from pymysql import Connection
from pymysql.cursors import DictCursor, Cursor

from pymysqlpool.cursor import PoolCursor

__version__ = '0.0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')


class NoFreeConnectionError(Exception):
    pass


class MySQLConnection(object):
    def __init__(self, conn, is_free):
        self.connection = conn
        self.is_free = is_free
        self.last_activation_at = datetime.datetime.now()

    @property
    def last_activation_timestamp(self):
        return int(self.last_activation_at.timestamp())

    def __lt__(self, other):
        return self.last_activation_at < other.last_activation_at

    def __getattr__(self, item):
        # dispatch other operations to pymysql connection
        return getattr(self.connection, item)

    def __repr__(self):
        return '<MySQLConnection at 0x{:0x}, free={}, last_activation_at={}>'.format(id(self), self.is_free,
                                                                                     self.last_activation_at)


class PoolContainer(object):
    pass


class MySQLConnectionPool(object):
    __instance = {}

    def __new__(cls, *args, **kwargs):
        """To create an singleton instance"""
        try:
            pool_name = args[0]
        except IndexError:
            pool_name = kwargs['pool_name']

        if pool_name not in cls.__instance:
            cls.__instance[pool_name] = object.__new__(cls)
        return cls.__instance[pool_name]

    def __init__(self, pool_name, host=None, user=None, password="", database=None, port=3306,
                 charset='utf8', use_dict_cursor=True, max_pool_size=5,
                 incremental_size=2, wait_connection_timeout=60, **kwargs):
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
        :param max_pool_size: maximum connection pool size
        :param incremental_size: increase `incremental_size` connections a time
        :param wait_connection_timeout: wait how many seconds to get a free connection
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
        self._max_pool_size = max_pool_size
        self._incremental_size = incremental_size
        self.wait_connection_timeout = wait_connection_timeout

        # containers for connection pool manager
        self.__free_connections = deque()
        self.__busy_connections = list()

        # single lock is not enough, be aware of the dead lock issue
        self.__pool_lock = threading.RLock()
        self.__kill_lock = threading.RLock()
        self.__is_killed = False

    def __repr__(self):
        return '<MySQLConnectionPool object at 0x{:0x}, ' \
               'name={!r}, size={!r}>'.format(id(self), self.pool_name, self.pool_size)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            logger.error(exc_tb, exc_info=True)

        self.close()

    def __del__(self):
        self.close()

    def __iter__(self):
        """Iterate each connection item"""
        with self.__pool_lock:
            yield from chain(self.__free_connections, self.__busy_connections)

    @property
    def pool_name(self):
        return self._pool_name

    @property
    def pool_size(self):
        with self.__pool_lock:
            return len(self.__free_connections + self.__busy_connections)

    @property
    def free_size(self):
        with self.__pool_lock:
            return len(self.__free_connections)

    def pool_cursor(self, use_dict_cursor=True):
        cursor_class = DictCursor if use_dict_cursor else self._cursor_class
        return PoolCursor(self, cursor_class)

    def connect(self):
        """Connect to this connection pool
        """
        logger.info('[{}] open connection pool'.format(self.pool_name))
        self._add_connections()

    def close(self):
        """Close this connection pool"""
        logger.info('[{}] close connection pool'.format(self.pool_name))
        with self.__kill_lock:
            if self.__is_killed is True:
                return

            self._free()
            self.__is_killed = True

    def borrow_connection(self, wait_timeout=60):
        """
        Get a free connection item from current pool
        """
        logger.debug('[{}] borrow a connection from the connection pool: {}'.format(self.pool_name, self))
        conn_item = self._get_free_connection()

        if conn_item:
            return conn_item

        if self.pool_size < self._max_pool_size:
            logger.debug('[{}] expand connection pool, current size is {}'.format(self.pool_name, self.pool_size))
            self._add_connections()
            return self.borrow_connection(wait_timeout)

        # Otherwise, just wait a moment to get a free one
        logger.debug('[{}] the connection pool is empty, you have to wait at most {} seconds to '
                     'get a free connection'.format(self.pool_name, self.wait_connection_timeout))
        while wait_timeout:
            wait_timeout -= 1
            conn_item = self._get_free_connection()
            if conn_item:
                return conn_item
            time.sleep(1)
        else:
            raise NoFreeConnectionError('[{}] cannot find any free connection now'.format(self.pool_name))

    def _get_free_connection(self):
        # check if we have any free connections
        if self.free_size == 0:
            return None

        with self.__pool_lock:
            conn_item = self.__free_connections.pop()
            self.__busy_connections.append(conn_item)
            conn_item.is_free = False
            conn_item.last_activation_at = datetime.datetime.now()
            logger.debug('[{}] get free connection succeeded, '
                         'current size is {}'.format(self.pool_name, (self.pool_size, self.free_size)))
            return conn_item

    def return_connection(self, conn_item):
        """Return a connection to the pool"""
        if self.pool_size == self._max_pool_size:
            logger.warning('[{}] failed to return connection item {}'.format(self.pool_name, conn_item))
            return False

        logger.debug('[{}] return a connection item: {}'.format(self.pool_name, conn_item))
        with self.__pool_lock:
            self.__busy_connections.remove(conn_item)
            conn_item.is_free = True
            self.__free_connections.appendleft(conn_item)
            logger.debug('[{}] return connection succeeded, '
                         'current size is: {}'.format(self.pool_name, (self.pool_size, self.free_size)))

    def _add_connections(self):
        """
        Initialize the connection pool, create several connections here.
        """
        # Create several new connections
        for i in range(self._incremental_size):
            try:
                conn_item = self._create_connection()
                self._connection_pool.put_nowait(conn_item)
                with self.__safe_lock:
                    self._connection_items.append(conn_item)
            except queue.Full:
                logger.debug('Connection pool is full now')
                break
            else:
                logger.debug('[{}] add a new connection item: {}'.format(self.pool_name, conn_item))
                conn_item.connect()

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
        conn = Connection(self._host,
                          self._user,
                          self._password,
                          self._database,
                          self._port,
                          charset=self._charset,
                          cursorclass=self._cursor_class,
                          **self._other_kwargs)
        return MySQLConnection(conn)

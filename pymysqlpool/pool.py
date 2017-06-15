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

from pymysql import Connection
from pymysql.cursors import DictCursor, Cursor

from pymysqlpool.cursor import PoolCursor

__version__ = '0.0.1'
__author__ = 'Chris'

logger = logging.getLogger('pymysqlpool')


class NoFreeConnectionError(Exception):
    pass


class MySQLConnection(object):
    def __init__(self, conn, is_free=True):
        self.connection = conn
        self.is_free = is_free
        self.last_activation_at = datetime.datetime.now()

    @property
    def last_activation_timestamp(self):
        return int(self.last_activation_at.timestamp())

    def __lt__(self, other):
        return self.last_activation_at < other.last_activation_at

    def __getattr__(self, item):
        return getattr(self.connection, item)

    def __repr__(self):
        return '<Connection item: {}, last_activation_at: {}>'.format(self.connection, self.last_activation_at)


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
                 charset='utf8', use_dict_cursor=True, max_pool_size=10, incremental_size=5, **kwargs):
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
        :param kwargs: other keyword arguments to be passed to `pymysql.Connection`
        """
        self._pool_name = pool_name
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._port = port
        self._charset = charset
        self.cursor_class = DictCursor if use_dict_cursor else Cursor
        self._max_pool_size = max_pool_size
        self._incremental_size = incremental_size
        self._other_kwargs = kwargs
        self._lock = threading.RLock()
        self._connection_items = list()
        self._connection_pool = queue.PriorityQueue(self._max_pool_size)

    def __repr__(self):
        return '<MySQLConnectionPool object at 0x{:0x}, ' \
               'name={!r}, size={!r}>'.format(id(self), self.pool_name, self.pool_size)

    def __enter__(self):
        self.open()
        return self.borrow_connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            logger.error(exc_tb)

        self.close()

    def __del__(self):
        self.close()

    def __iter__(self):
        """Iterate each connection item"""
        return iter(self._connection_items)

    def open(self):
        """Open this connection pool"""
        logger.info('[{}] open connection pool'.format(self.pool_name))
        self._add_connections()

    def close(self):
        """Close this connection pool"""
        logger.info('[{}] close connection pool'.format(self.pool_name))
        self._free()

    def borrow_connection(self, wait_timeout=60):
        """
        Get a free connection item from current pool
        """
        logger.debug('[{}] borrow a connection from the connection pool: {}'.format(self.pool_name, self))
        conn_item = self._get_free_connection()

        if conn_item:
            return conn_item

        if self.pool_size < self._max_pool_size:
            self._add_connections()
            return self.borrow_connection(wait_timeout)

        # Otherwise, just wait a moment to get a free one
        while wait_timeout:
            wait_timeout -= 1
            conn_item = self._get_free_connection()
            if conn_item:
                return conn_item
            time.sleep(1)
        else:
            raise NoFreeConnectionError('[{}] cannot find any free connection now'.format(self.pool_name))

    def _get_free_connection(self):
        try:
            conn_item = self._connection_pool.get_nowait()
        except queue.Empty:
            return None
        else:
            conn_item.last_activation_at = datetime.datetime.now()
            return conn_item

    def return_connection(self, conn_item):
        """Return a connection to the pool"""
        logger.debug('[{}] return a connection item {}: {}'.format(self.pool_name, conn_item, self))
        self._connection_pool.put_nowait(conn_item)

    @property
    def pool_name(self):
        return self._pool_name

    @property
    def pool_size(self):
        return len(self._connection_items)

    def _add_connections(self):
        """
        Initialize the connection pool, create several connections here.
        """
        # Create several connections
        for i in range(self._incremental_size):
            try:
                conn_item = self._create_connection()
                self._connection_pool.put_nowait(conn_item)
                with self._lock:
                    self._connection_items.append(conn_item)
            except queue.Full:
                logger.debug('Connection pool is full now')
                break
            else:
                logger.debug('Add new connection item: {}'.format(conn_item))
                conn_item.connect()

    def _free(self):
        """
        Release all the connections in the pool
        """
        for conn_item in self:
            try:
                conn_item.close()
            except Exception:
                pass

    def _create_connection(self):
        conn = Connection(self._host,
                          self._user,
                          self._password,
                          self._database,
                          self._port,
                          charset=self._charset,
                          cursorclass=self.cursor_class,
                          **self._other_kwargs)
        return MySQLConnection(conn)

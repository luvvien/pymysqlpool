__all__ = ['ConnectionPool']

_instances = {}


def ConnectionPool(*args, **kwargs):
    """Connection pool factory function, singleton instance factory.
    If you want a single connection pool, call this factory function.

    :param args: positional arguments passed to `MySQLConnectionPool`
    :param kwargs: dict arguments passed to `MySQLConnectionPool`
    :return: instance of class`MySQLConnectionPool`
    """
    from .connection import MySQLConnectionPool
    try:
        pool_name = args[0]
    except IndexError:
        pool_name = kwargs['pool_name']

    if pool_name not in _instances:
        _instances[pool_name] = MySQLConnectionPool(*args, **kwargs)
    pool = _instances[pool_name]
    assert isinstance(pool, MySQLConnectionPool)
    return pool

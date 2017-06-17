__all__ = ['create_connection_pool']


_instances = {}


def create_connection_pool(*args, **kwargs):
    """Connection pool factory function, singleton instance factory
    If you want a single connection pool, call this factory function.
    """
    try:
        pool_name = args[0]
    except IndexError:
        pool_name = kwargs['pool_name']

    if pool_name not in _instances:
        from .connection import MySQLConnectionPool
        _instances[pool_name] = MySQLConnectionPool(*args, **kwargs)
    return _instances[pool_name]




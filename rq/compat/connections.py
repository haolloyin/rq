#coding=utf8

from redis import Redis, StrictRedis
from functools import partial


def fix_return_type(func):
    # deliberately no functools.wraps() call here, since the function being
    # wrapped is a partial, which has no module

    # 只是把原来执行函数之后返回 None 的方法装饰成 返回 -1，
    # 用于确保下面 patch_connection() 中的 connection._ttl() 返回 -1，表示永不过期
    def _inner(*args, **kwargs):
        value = func(*args, **kwargs)
        if value is None:
            value = -1
        return value
    return _inner


def patch_connection(connection):
    if not isinstance(connection, StrictRedis):
        raise ValueError('A StrictRedis or Redis connection is required.')

    # Don't patch already patches objects
    PATCHED_METHODS = ['_setex', '_lrem', '_zadd', '_pipeline', '_ttl']
    if all([hasattr(connection, attr) for attr in PATCHED_METHODS]):
        return connection

    if isinstance(connection, Redis):
        # TODO: why？把 Redis 对象的方法重置成 StrictRedis 的
        connection._setex = partial(StrictRedis.setex, connection)
        connection._lrem = partial(StrictRedis.lrem, connection)
        connection._zadd = partial(StrictRedis.zadd, connection)
        connection._pipeline = partial(StrictRedis.pipeline, connection)
        connection._ttl = fix_return_type(partial(StrictRedis.ttl, connection))
        if hasattr(connection, 'pttl'):
            connection._pttl = fix_return_type(partial(StrictRedis.pttl, connection))
    elif isinstance(connection, StrictRedis):
        connection._setex = connection.setex
        connection._lrem = connection.lrem
        connection._zadd = connection.zadd
        connection._pipeline = connection.pipeline
        connection._ttl = connection.ttl
        if hasattr(connection, 'pttl'):
            connection._pttl = connection.pttl
    else:
        raise ValueError('Unanticipated connection type: {}. Please report this.'.format(type(connection)))

    return connection

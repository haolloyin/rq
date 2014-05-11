#coding=utf-8

from contextlib import contextmanager
from redis import StrictRedis
from .local import LocalStack, release_local
from .compat.connections import patch_connection
from .utils import funclog

class NoRedisConnectionException(Exception):
    pass

# contextmanager 用于构造 with 语句的生成器。
# 在这里 yield 没有具体的迭代值，整个生成器为 [None]，在这里的目的主要是使 Connection 支持 with 语法，
# 实现是在 try 之前的 push_connection() 初始化了变量 _connection_stack，最后在 finnaly 进行 assert，
# 这个 _connection_stack 变量可以在 queue.py 中通过本文件定义的函数进行使用。
# 参考：http://www.python.org/dev/peps/pep-0343/
@contextmanager
def Connection(connection=None):
    funclog()
    if connection is None:
        connection = StrictRedis()
    push_connection(connection)
    try:
        yield
    finally:
        popped = pop_connection()
        assert popped == connection, \
            'Unexpected Redis connection was popped off the stack. ' \
            'Check your Redis connection setup.'


def push_connection(redis):
    """Pushes the given connection on the stack."""
    funclog()
    _connection_stack.push(patch_connection(redis))


def pop_connection():
    """Pops the topmost connection from the stack."""
    funclog()
    return _connection_stack.pop()


def use_connection(redis=None):
    """Clears the stack and uses the given connection.  Protects against mixed
    use of use_connection() and stacked connection contexts.
    """
    assert len(_connection_stack) <= 1, \
        'You should not mix Connection contexts with use_connection().'
    release_local(_connection_stack)

    if redis is None:
        redis = StrictRedis()
    push_connection(redis)


def get_current_connection():
    """Returns the current Redis connection (i.e. the topmost on the
    connection stack).
    """
    funclog()
    return _connection_stack.top


def resolve_connection(connection=None):
    """Convenience function to resolve the given or the current connection.
    Raises an exception if it cannot resolve a connection now.

    获取 connection
        如果传入 Redis 对象，则用 partail 附加上 StrictRedis 对应的方法
        如果传入 StrictRedis 对象，不做改变
        如果不传参数（仅在 with Connection() 与语法下可用），由 rq 默认生成
    """
    funclog()
    if connection is not None:
        return patch_connection(connection)

    connection = get_current_connection()
    if connection is None:
        raise NoRedisConnectionException('Could not resolve a Redis connection.')
    return connection


_connection_stack = LocalStack()

__all__ = ['Connection', 'get_current_connection', 'push_connection',
           'pop_connection', 'use_connection']

#coding=utf-8

import logging
from rq import get_current_connection
from rq import Worker


logger = logging.getLogger(__name__)


def cleanup_ghosts():
    """
    RQ versions < 0.3.6 suffered from a race condition where workers, when
    abruptly terminated, did not have a chance to clean up their worker
    registration, leading to reports of ghosted workers in `rqinfo`.  Since
    0.3.6, new worker registrations automatically expire, and the worker will
    make sure to refresh the registrations as long as it's alive.

    This function will clean up any of such legacy ghosted workers.
    """

    # 该函数是为了给 0.3.6 版本以前产生的 ghost worker 设置 ttl，避免这些
    # worker 一直出现在 rqinfo 监视命令中产生多余的信息。
    #
    # 在 compat/connections.py 中的 patch_connection() 为 StrictRedis 对象附加了
    # _ttl() 方法，但返回 -1 时说明当前 key 没有设置 ttl，所以下面给 worker 设置 ttl。
    conn = get_current_connection()
    for worker in Worker.all():
        if conn._ttl(worker.key) == -1:
            ttl = worker.default_worker_ttl
            conn.expire(worker.key, ttl) #
            logger.info('Marked ghosted worker {0} to expire in {1} seconds.'.format(worker.name, ttl))

#coding=utf-8

import sys
import os
import errno
import random
import time
try:
    from procname import setprocname
except ImportError:
    def setprocname(*args, **kwargs):  # noqa
        pass
import socket
import signal
import traceback
import logging
from .queue import Queue, get_failed_queue
from .connections import get_current_connection
from .job import Job, Status
from .utils import make_colorizer, utcnow, utcformat
from .logutils import setup_loghandlers
from .exceptions import NoQueueError, UnpickleError, DequeueTimeout
from .timeouts import death_penalty_after
from .version import VERSION
from rq.compat import text_type, as_text

green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')
blue = make_colorizer('darkblue')

DEFAULT_WORKER_TTL = 420
DEFAULT_RESULT_TTL = 500
logger = logging.getLogger(__name__)


class StopRequested(Exception):
    pass


def iterable(x):
    return hasattr(x, '__iter__')


def compact(l):
    return [x for x in l if x is not None]

# TODO: 这些信号是干嘛的？看 APUE。
_signames = dict((getattr(signal, signame), signame)
                 for signame in dir(signal)
                 if signame.startswith('SIG') and '_' not in signame)


def signal_name(signum):
    # Hackety-hack-hack: is there really no better way to reverse lookup the
    # signal name?  If you read this and know a way: please provide a patch :)

    # TODO: 获取信号名，需要反向查询吗？
    # 通过信号值查询信号名
    try:
        return _signames[signum]
    except KeyError:
        return 'SIG_UNKNOWN'


class Worker(object):
    redis_worker_namespace_prefix = 'rq:worker:'
    redis_workers_keys = 'rq:workers'

    @classmethod
    def all(cls, connection=None):
        """Returns an iterable of all Workers.

        返回所有 worker
        """
        if connection is None:
            connection = get_current_connection()
        reported_working = connection.smembers(cls.redis_workers_keys) # smembers 返回集合 key 中所有成员
        workers = [cls.find_by_key(as_text(key), connection)
                   for key in reported_working]
        return compact(workers)

    @classmethod
    def find_by_key(cls, worker_key, connection=None):
        """Returns a Worker instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup Workers
        by their Redis keys.
        """
        prefix = cls.redis_worker_namespace_prefix
        if not worker_key.startswith(prefix):
            raise ValueError('Not a valid RQ worker key: %s' % (worker_key,))

        if connection is None:
            connection = get_current_connection()
        if not connection.exists(worker_key): # 不存在 worker_key
            connection.srem(cls.redis_workers_keys, worker_key) # srem 移除集合 key 中的一个或多个 member 元素
            return None

        name = worker_key[len(prefix):]
        worker = cls([], name, connection=connection)
        queues = as_text(connection.hget(worker.key, 'queues'))
        worker._state = connection.hget(worker.key, 'state') or '?'
        if queues:
            worker.queues = [Queue(queue, connection=connection)
                             for queue in queues.split(',')] # queues 用逗号分隔多个 queue，见 register_birth()
        return worker

    def __init__(self, queues, name=None,
                 default_result_ttl=DEFAULT_RESULT_TTL, connection=None,
                 exc_handler=None, default_worker_ttl=DEFAULT_WORKER_TTL):  # noqa
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        if isinstance(queues, Queue):
            queues = [queues]
        self._name = name
        self.queues = queues
        self.validate_queues() # 验证所给的 queues 是否可迭代，并且每个元素都是 Queue 实例
        self._exc_handlers = []
        self.default_result_ttl = default_result_ttl
        self.default_worker_ttl = default_worker_ttl
        self._state = 'starting'
        self._is_horse = False
        self._horse_pid = 0
        self._stopped = False
        self.log = logger
        self.failed_queue = get_failed_queue(connection=self.connection) # 每个 worker 保存一个 failed queue

        # By default, push the "move-to-failed-queue" exception handler onto
        # the stack
        self.push_exc_handler(self.move_to_failed_queue)
        if exc_handler is not None:
            self.push_exc_handler(exc_handler)


    def validate_queues(self):  # noqa
        """Sanity check for the given queues.

        验证所给的 queues 是否可迭代，并且每个元素都是 Queue 实例
        """
        if not iterable(self.queues):
            raise ValueError('Argument queues not iterable.')
        for queue in self.queues:
            if not isinstance(queue, Queue):
                raise NoQueueError('Give each worker at least one Queue.')

    def queue_names(self):
        """Returns the queue names of this worker's queues."""
        return map(lambda q: q.name, self.queues)

    def queue_keys(self):
        """Returns the Redis keys representing this worker's queues."""
        return map(lambda q: q.key, self.queues)


    @property  # noqa
    def name(self):
        """Returns the name of the worker, under which it is registered to the
        monitoring system.

        By default, the name of the worker is constructed from the current
        (short) host name and the current PID.

        获取当前 worker 已注册到监控系统的名称，默认为[主机名.pid]
        """
        if self._name is None:
            hostname = socket.gethostname()
            shortname, _, _ = hostname.partition('.')
            self._name = '%s.%s' % (shortname, self.pid)
        return self._name

    @property
    def key(self):
        """Returns the worker's Redis hash key."""
        return self.redis_worker_namespace_prefix + self.name

    @property
    def pid(self):
        """The current process ID."""
        return os.getpid()

    @property
    def horse_pid(self):
        """The horse's process ID.  Only available in the worker.  Will return
        0 in the horse part of the fork.

        # TODO: 木马 pid 在这里是啥意思？后台进程？
        """
        return self._horse_pid

    @property
    def is_horse(self):
        """Returns whether or not this is the worker or the work horse."""
        return self._is_horse

    def procline(self, message):
        """Changes the current procname for the process.

        This can be used to make `ps -ef` output more readable.

        修改当前进程名
        """
        setprocname('rq: %s' % (message,))


    def register_birth(self):  # noqa
        """Registers its own birth.

        worker 启动，注册相关信息
        """
        self.log.debug('Registering birth of worker %s' % (self.name,))
        # redis 中存在当前 worker 并且集合 worker_key 中没有 death 域
        if self.connection.exists(self.key) and \
                not self.connection.hexists(self.key, 'death'):
            raise ValueError('There exists an active worker named \'%s\' '
                             'already.' % (self.name,))
        key = self.key
        queues = ','.join(self.queue_names()) # worker 中的 queue name 用逗号分隔
        with self.connection._pipeline() as p:
            p.delete(key) # 删除已有的 worker_key
            p.hset(key, 'birth', utcformat(utcnow())) # 给 worker_key 哈希表增加 birth、queues 域
            p.hset(key, 'queues', queues)
            p.sadd(self.redis_workers_keys, key) # 注册当前 worker_key 到 workers_keys
            p.expire(key, self.default_worker_ttl)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        self.log.debug('Registering death')
        with self.connection._pipeline() as p:
            # We cannot use self.state = 'dead' here, because that would
            # rollback the pipeline

            # TODO: 上面这么说，可能是因为 pipeline 内强制了 server 缓冲所有响应，
            # 这会导致 set_state() 中的 hset 挂住
            p.srem(self.redis_workers_keys, self.key)
            p.hset(self.key, 'death', utcformat(utcnow()))
            p.expire(self.key, 60)
            p.execute()

    def set_state(self, new_state):
        self._state = new_state
        self.connection.hset(self.key, 'state', new_state)

    def get_state(self):
        return self._state

    state = property(get_state, set_state)

    @property
    def stopped(self):
        return self._stopped

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.

        注册信号 handler
        TODO: 信号相关的看不懂
        """

        def request_force_stop(signum, frame):
            """Terminates the application (cold shutdown).
            """
            self.log.warning('Cold shut down.')

            # Take down the horse with the worker
            if self.horse_pid:
                msg = 'Taking down horse %d with me.' % self.horse_pid
                self.log.debug(msg)
                try:
                    os.kill(self.horse_pid, signal.SIGKILL) # 杀死 horse_pid
                except OSError as e:
                    # ESRCH ("No such process") is fine with us
                    if e.errno != errno.ESRCH:
                        self.log.debug('Horse already down.')
                        raise
            raise SystemExit()

        def request_stop(signum, frame):
            """Stops the current worker loop but waits for child processes to
            end gracefully (warm shutdown).

            停止当前 worker loop，但是会等待子进程弦终止
            """
            self.log.debug('Got signal %s.' % signal_name(signum))

            signal.signal(signal.SIGINT, request_force_stop) # 中断进程
            signal.signal(signal.SIGTERM, request_force_stop) # 软终止

            msg = 'Warm shut down requested.'
            self.log.warning(msg)

            # If shutdown is requested in the middle of a job, wait until
            # finish before shutting down
            if self.state == 'busy':
                self._stopped = True
                self.log.debug('Stopping after current horse is finished. '
                               'Press Ctrl+C again for a cold shutdown.')
            else:
                raise StopRequested()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)


    def work(self, burst=False):  # noqa
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.

        启动 work loop，弹出当前 queue 中所有 job 并开始执行，
        当所有 queue 都为空，等待任意 queue 有 job 入队。
        返回 True 或 False 表示 worker 是否正在执行的。
        """
        setup_loghandlers()
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.log.info('RQ worker started, version %s' % VERSION)
        self.state = 'starting'
        try:
            while True:
                if self.stopped:
                    self.log.info('Stopping on request.')
                    break
                self.state = 'idle'
                qnames = self.queue_names()
                self.procline('Listening on %s' % ','.join(qnames)) # 修改进程名
                self.log.info('')
                self.log.info('*** Listening on %s...' %
                              green(', '.join(qnames)))
                timeout = None if burst else max(1, self.default_worker_ttl - 60)
                try:
                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        break
                except StopRequested:
                    break

                self.state = 'busy'

                job, queue = result
                # Use the public setter here, to immediately update Redis
                job.status = Status.STARTED
                self.log.info('%s: %s (%s)' % (green(queue.name),
                              blue(job.description), job.id))

                self.heartbeat((job.timeout or 180) + 60)
                self.fork_and_perform_job(job)
                self.heartbeat()
                if job.status == Status.FINISHED:
                    queue.enqueue_dependents(job)

                did_perform_work = True
        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work

    def dequeue_job_and_maintain_ttl(self, timeout):
        result = None
        while True:
            # TODO: 这里在取出队列中的 job 时，设置心跳是为啥？
            self.heartbeat()
            try:
                result = Queue.dequeue_any(self.queues, timeout,
                                           connection=self.connection)
                break
            except DequeueTimeout:
                pass

        self.heartbeat()
        return result

    def heartbeat(self, timeout=0):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a "heartbeat"
        to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker will
        die (at least from the monitoring dashboards).

        The effective timeout can never be shorter than default_worker_ttl,
        only larger.

        指定新的 worker 的 timeout 时间，例如延长 worker 的超时时间避免挂掉，即心跳
        """
        timeout = max(timeout, self.default_worker_ttl)
        self.connection.expire(self.key, timeout) # 设置 worker 的超时时间
        self.log.debug('Sent heartbeat to prevent worker timeout. '
                       'Next one should arrive within {0} seconds.'.format(timeout))

    def fork_and_perform_job(self, job):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.

        创建一个后台 worker 处理 job，并确保 horse worker 在超时前开始执行，
        否则以 SIGALRM 信号终止 horse worker
        TODO: 进程控制相关。
        """
        child_pid = os.fork()
        if child_pid == 0:
            self.main_work_horse(job) # 子进程中返回 0，则执行任务
        else:
            self._horse_pid = child_pid # 将在父进程中的返回的子进程 pid 赋值保存
            self.procline('Forked %d at %d' % (child_pid, time.time()))
            while True:
                try:
                    os.waitpid(child_pid, 0)
                    break
                except OSError as e:
                    # In case we encountered an OSError due to EINTR (which is
                    # caused by a SIGINT or SIGTERM signal during
                    # os.waitpid()), we simply ignore it and enter the next
                    # iteration of the loop, waiting for the child to end.  In
                    # any other case, this is some other unexpected OS error,
                    # which we don't want to catch, so we re-raise those ones.
                    if e.errno != errno.EINTR:
                        raise

    def main_work_horse(self, job):
        """This is the entry point of the newly spawned work horse.

        horse worker 的入口
        """
        # After fork()'ing, always assure we are generating random sequences
        # that are different from the worker.
        random.seed()

        # Always ignore Ctrl+C in the work horse, as it might abort the
        # currently running job.
        # The main worker catches the Ctrl+C and requests graceful shutdown
        # after the current work is done.  When cold shutdown is requested, it
        # kills the current job anyway.

        # TODO: 结合前面的 _install_signal_handlers() 一起理解
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        self._is_horse = True
        self.log = logger

        success = self.perform_job(job) # 执行 job

        # os._exit() is the way to exit from childs after a fork(), in
        # constrast to the regular sys.exit()
        os._exit(int(not success)) # TODO: 这里为啥用 not success？

    def perform_job(self, job):
        """Performs the actual work of a job.  Will/should only be called
        inside the work horse's process.

        执行 job，将会而且只能在 horse worker 中调用
        """
        self.procline('Processing %s from %s since %s' % (
            job.func_name,
            job.origin, time.time()))

        with self.connection._pipeline() as pipeline:
            try:
                # TODO:
                with death_penalty_after(job.timeout or Queue.DEFAULT_TIMEOUT):
                    rv = job.perform()

                # Pickle the result in the same try-except block since we need to
                # use the same exc handling when pickling fails
                job._result = rv
                job._status = Status.FINISHED
                job.ended_at = utcnow()

                result_ttl = job.get_ttl(self.default_result_ttl)
                if result_ttl != 0:
                    job.save(pipeline=pipeline)
                job.cleanup(result_ttl, pipeline=pipeline) # 清理 job 信息
                pipeline.execute()

            except Exception:
                # Use the public setter here, to immediately update Redis
                job.status = Status.FAILED
                self.handle_exception(job, *sys.exc_info())
                return False

        if rv is None:
            self.log.info('Job OK')
        else:
            self.log.info('Job OK, result = %s' % (yellow(text_type(rv)),))

        if result_ttl == 0:
            self.log.info('Result discarded immediately.')
        elif result_ttl > 0:
            self.log.info('Result is kept for %d seconds.' % result_ttl)
        else:
            self.log.warning('Result will never expire, clean up result key manually.')

        return True

    def handle_exception(self, job, *exc_info):
        """Walks the exception handler stack to delegate exception handling.

        TODO: 这个从异常中获取有效信息的有意思。
        """
        exc_string = ''.join(traceback.format_exception_only(*exc_info[:2]) +
                             traceback.format_exception(*exc_info))
        self.log.error(exc_string)

        for handler in reversed(self._exc_handlers):
            self.log.debug('Invoking exception handler %s' % (handler,))
            fallthrough = handler(job, *exc_info)

            # Only handlers with explicit return values should disable further
            # exc handling, so interpret a None return value as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break

    def move_to_failed_queue(self, job, *exc_info):
        """Default exception handler: move the job to the failed queue."""
        exc_string = ''.join(traceback.format_exception(*exc_info))
        self.log.warning('Moving job to %s queue.' % self.failed_queue.name)
        self.failed_queue.quarantine(job, exc_info=exc_string)

    def push_exc_handler(self, handler_func):
        """Pushes an exception handler onto the exc handler stack."""
        self._exc_handlers.append(handler_func)

    def pop_exc_handler(self):
        """Pops the latest exception handler off of the exc handler stack."""
        return self._exc_handlers.pop()

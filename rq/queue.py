#coding=utf-8

import uuid

from .connections import resolve_connection
from .job import Job, Status
from .utils import utcnow, funclog

from .exceptions import (DequeueTimeout, InvalidJobOperationError,
                         NoSuchJobError, UnpickleError)
from .compat import total_ordering, string_types, as_text

from redis import WatchError


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""
    return FailedQueue(connection=connection)


def compact(lst):
    return [item for item in lst if item is not None]

# total_ordering 类装饰器，用于给类添加可能缺少的比较排序方法
@total_ordering
class Queue(object):
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues' # 当前 Connection 下所有队列的 set，见 enqueue_job() 第一行

    @classmethod
    def all(cls, connection=None):
        """Returns an iterable of all Queues.

        以 list 返回所有 queue 对象
        """
        connection = resolve_connection(connection)

        def to_queue(queue_key):
            return cls.from_queue_key(as_text(queue_key),
                                      connection=connection)
        # connection.smembers() 返回 set key 对应的所有成员
        # to_queue() 将指定 key 的对象初始化成 Queue 对象
        return [to_queue(rq_key) for rq_key in connection.smembers(cls.redis_queues_keys) if rq_key]

    @classmethod
    def from_queue_key(cls, queue_key, connection=None):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.

        根据指定的 key 初始化 Queue 对象并返回。
        """
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix): # 判断前缀
            raise ValueError('Not a valid RQ queue key: %s' % (queue_key,))
        name = queue_key[len(prefix):]
        return cls(name, connection=connection)

    def __init__(self, name='default', default_timeout=None, connection=None,
                 async=True):
        funclog()
        self.connection = resolve_connection(connection) # 取得 Connection 对象
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '%s%s' % (prefix, name) # 每个 Queue 对象的 key 前缀都是固定的
        self._default_timeout = default_timeout
        self._async = async

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    def empty(self):
        """Removes all messages on the queue.

        取消当前 queue.connection 中的所有 job
        """
        job_list = self.get_jobs()
        self.connection.delete(self.key)
        for job in job_list:
            job.cancel()

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    def fetch_job(self, job_id):
        try:
            return Job.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            self.remove(job_id)

    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue.

        指定偏移量、长度获取（多个）job_id
        """
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length # -1 时相当于取整个 list
        return [as_text(job_id) for job_id in
                self.connection.lrange(self.key, start, end)]

    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue.

        获取指定区间内的（多个）job
        """
        job_ids = self.get_job_ids(offset, length)
        return compact([self.fetch_job(job_id) for job_id in job_ids]) # compact 函数仅用于剔除 None 对象

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.get_job_ids()

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        return self.get_jobs()

    @property
    def count(self):
        """Returns a count of all messages in the queue.

        llen 返回 queue 中 key 的长度
        """
        return self.connection.llen(self.key)

    def remove(self, job_or_id):
        """Removes Job from queue, accepts either a Job instance or ID.

        lrem(key, count, value) 根据 count 的值，移除 queue 中与 value 相等的（job_id 对应的）job
        """
        job_id = job_or_id.id if isinstance(job_or_id, Job) else job_or_id
        return self.connection._lrem(self.key, 0, job_id)

    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guarantueeing FIFO semantics.

        按照 FIFO 删除所有死掉的或已完成的 job
        """
        COMPACT_QUEUE = 'rq:queue:_compact:{0}'.format(uuid.uuid4())

        self.connection.rename(self.key, COMPACT_QUEUE) # 对当前连接改名
        while True:
            job_id = as_text(self.connection.lpop(COMPACT_QUEUE))
            if job_id is None:
                break
            # 如果 job 存在，重新放到正常队列中，否则丢弃不处理
            if Job.exists(job_id, self.connection):
                self.connection.rpush(self.key, job_id)


    def push_job_id(self, job_id):  # noqa
        """Pushes a job ID on the corresponding Redis queue."""
        self.connection.rpush(self.key, job_id)


    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, description=None, depends_on=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.

        给传入的函数创建 job 并入队，注意 kwargs 可同时传入 rq 需要的参数
        """
        timeout = timeout or self._default_timeout

        # TODO: job with dependency shouldn't have "queued" as status
        # TODO: 有依赖的 job 不应该用 ‘queued’ 状态
        job = Job.create(func, args, kwargs, connection=self.connection,
                         result_ttl=result_ttl, status=Status.QUEUED,
                         description=description, depends_on=depends_on, timeout=timeout)

        # If job depends on an unfinished job, register itself on it's
        # parent's dependents instead of enqueueing it.
        # If WatchError is raised in the process, that means something else is
        # modifying the dependency. In this case we simply retry

        # 如果 job 有未完成的依赖，注册到所依赖 job 的下属去，而不是入队
        # 如果抛出 WatchError 异常，说明依赖正被修改，这里只是简单地重试
        if depends_on is not None:
            # pipeline 用于一次性给 redis 发送多条命令，常用于批处理
            # 注：这些命令会在 redis 的 multi 和 exec 下按事务执行，保证了先后顺序
            # 并且每个命令发生异常不会暂停本次 pipeline，而是捕获后作为结果返回
            # 另，用 with 语法默认会 reset() 把连接返回连接池，如果不是用 with，必须手动调用 reset()
            # 参考：https://github.com/andymccurdy/redis-py#pipelines
            with self.connection.pipeline() as pipe:
                while True:
                    try:
                        # 监视依赖的任务，如果在事务执行之前这个 key 被其他命令所改动，那么事务将被打断
                        pipe.watch(depends_on.key)
                        if depends_on.status != Status.FINISHED:
                            job.register_dependency() # 把当前 job_id 加入到以父依赖为 key 的 set 中
                            job.save()
                            return job
                        break
                    except WatchError:
                        continue

        return self.enqueue_job(job)

    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues
        it.

        Expects the function to call, along with the arguments and keyword
        arguments.

        The function argument `f` may be any of the following:

        * A reference to a function
        * A reference to an object's instance method
        * A string, representing the location of a function (must be
          meaningful to the import context of the workers)

        f 参数只能是：函数、对象的实例方法、字符串（必须在 worker 能访问到的上下文中）
        这里只是解析参数，最终调用 enqueue_call()
        """
        # 如果 f 是字符串或 __main__ 里面的函数，抛出异常
        if not isinstance(f, string_types) and f.__module__ == '__main__':
            raise ValueError('Functions from the __main__ module cannot be processed '
                             'by workers.')

        # Detect explicit invocations, i.e. of the form:
        #     q.enqueue(foo, args=(1, 2), kwargs={'a': 1}, timeout=30)
        timeout = None
        description = None
        result_ttl = None
        depends_on = None
        # TODO 不明白为啥要检查 kwargs
        if 'args' in kwargs or 'kwargs' in kwargs or 'depends_on' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs.'  # noqa
            timeout = kwargs.pop('timeout', None)
            description = kwargs.pop('description', None)
            args = kwargs.pop('args', None)
            result_ttl = kwargs.pop('result_ttl', None)
            depends_on = kwargs.pop('depends_on', None)
            kwargs = kwargs.pop('kwargs', None)

        return self.enqueue_call(func=f, args=args, kwargs=kwargs,
                                 timeout=timeout, result_ttl=result_ttl,
                                 description=description, depends_on=depends_on)

    def enqueue_job(self, job, set_meta_data=True):
        """Enqueues a job for delayed execution.

        If the `set_meta_data` argument is `True` (default), it will update
        the properties `origin` and `enqueued_at`.

        If Queue is instantiated with async=False, job is executed immediately.

        将 job 入队等待执行，如果 async=False（即不是异步），则 job 会被立即执行而不通过 worker
        """
        # Add Queue key set
        # 将当前 queue_name 加入 set 中，反正 set 会自动去重
        self.connection.sadd(self.redis_queues_keys, self.key)

        if set_meta_data:
            job.origin = self.name
            job.enqueued_at = utcnow()

        if job.timeout is None:
            job.timeout = self.DEFAULT_TIMEOUT
        job.save()

        if self._async:
            self.push_job_id(job.id)
        else:
            job.perform() # 执行
            job.save()
        return job

    def enqueue_dependents(self, job):
        """Enqueues all jobs in the given job's dependents set and clears it.

        将给定的 job 的所有父依赖入队，每次从 set 中随机取一个
        """
        # TODO: can probably be pipelined
        while True:
            job_id = as_text(self.connection.spop(job.dependents_key))
            if job_id is None:
                break
            dependent = Job.fetch(job_id, connection=self.connection)
            self.enqueue_job(dependent)

    def pop_job_id(self):
        """Pops a given job ID from this Redis queue.

        TODO: 上面的原注释好像有误？
        从当前队列中去弹出一个 job，而不是 job_id
        """
        return as_text(self.connection.lpop(self.key))

    @classmethod
    def lpop(cls, queue_keys, timeout, connection=None):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key, whereas BLPOP
        accepts multiple.  So if we want the non-blocking LPOP, we need to
        iterate over all queues, do individual LPOPs, and return the result.

        Until Redis receives a specific method for this, we'll have to wrap it
        this way.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block

        Queue 类的工具函数，给定 queue_key 弹出值
        通过 timeout 参数的值来封装 lpop 和 blpop：
            timeout > 0     则用阻塞的 blpop
            timeout = None  则用 lpop 遍历弹出
            timeout = 0     这里不支持无限时长的阻塞
        """
        connection = resolve_connection(connection)
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. Please pick a timeout value > 0.')
            result = connection.blpop(queue_keys, timeout) # 阻塞式弹出
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:  # non-blocking variant
            # 非阻塞，需要遍历所有队列（queue_key），从中一个个弹出
            for queue_key in queue_keys:
                blob = connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def dequeue(self):
        """Dequeues the front-most job from this queue.

        Returns a Job instance, which can be executed or inspected.

        将最前端的 job 出队，返回的 Job 实例可用于执行或检查
        """
        job_id = self.pop_job_id()
        if job_id is None:
            return None
        try:
            job = Job.fetch(job_id, connection=self.connection)
        except NoSuchJobError as e:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking itself recursively
            return self.dequeue() # 递归出队
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting

            # 附加一点 job 信息抛出异常
            e.job_id = job_id
            e.queue = self
            raise e
        return job

    @classmethod
    def dequeue_any(cls, queues, timeout, connection=None):
        """Class method returning the Job instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.

        类方法，按序从指定的队列中弹出最前端的值
        """
        queue_keys = [q.key for q in queues]
        result = cls.lpop(queue_keys, timeout, connection=connection) # 调用类方法 lpop
        if result is None:
            return None
        queue_key, job_id = map(as_text, result) # 对返回的 queue_key 和 job_id 分别调用 as_text
        queue = cls.from_queue_key(queue_key, connection=connection)
        try:
            job = Job.fetch(job_id, connection=connection)
        except NoSuchJobError:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking the same function recursively
            return cls.dequeue_any(queues, timeout, connection=connection) # 递归调用，直到有 job
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.job_id = job_id
            e.queue = queue
            raise e
        return job, queue


    # Total ordering defition (the rest of the required Python methods are
    # auto-generated by the @total_ordering decorator)
    def __eq__(self, other):  # noqa
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):  # noqa
        return 'Queue(%r)' % (self.name,)

    def __str__(self):
        return '<Queue \'%s\'>' % (self.name,)


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__(Status.FAILED, connection=connection)

    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue).

        This is different from normal job enqueueing, since certain meta data
        must not be overridden (e.g. `origin` or `enqueued_at`) and other meta
        data must be inserted (`ended_at` and `exc_info`).

        将所给的 job 保存，主要是 job 的一些执行信息
        """
        job.ended_at = utcnow()
        job.exc_info = exc_info
        return self.enqueue_job(job, set_meta_data=False)

    def requeue(self, job_id):
        """Requeues the job with the given job ID.

        将指定的 job 重新入队
        """
        try:
            job = Job.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            self.remove(job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if self.remove(job) == 0:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs.')

        job.status = Status.QUEUED
        job.exc_info = None
        q = Queue(job.origin, connection=self.connection) # 用原来的 queue_key 重新入队
        q.enqueue_job(job)

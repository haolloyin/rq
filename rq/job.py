#coding=utf8

import inspect
from uuid import uuid4
try:
    from cPickle import loads, dumps, UnpicklingError
except ImportError:  # noqa
    from pickle import loads, dumps, UnpicklingError  # noqa
from .local import LocalStack
from .connections import resolve_connection
from .exceptions import UnpickleError, NoSuchJobError
from .utils import import_attribute, utcnow, utcformat, utcparse
from rq.compat import text_type, decode_redis_hash, as_text


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)
    return type(name, (), values)

Status = enum('Status',
              QUEUED='queued', FINISHED='finished', FAILED='failed',
              STARTED='started')

# Sentinel value to mark that some of our lazily evaluated properties have not
# yet been evaluated.
# 哨兵，标示一些惰性求值的属性还没被执行
UNEVALUATED = object()


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)

    反序列化
    """
    try:
        obj = loads(pickled_string)
    except (Exception, UnpicklingError) as e:
        raise UnpickleError('Could not unpickle.', pickled_string, e)
    return obj


def cancel_job(job_id, connection=None):
    """Cancels the job with the given job ID, preventing execution.  Discards
    any job info (i.e. it can't be requeued later).

    取消 job，丢弃所有 job 信息，例如事后不能被重新入队
    """
    Job(job_id, connection=connection).cancel()


def requeue_job(job_id, connection=None):
    """Requeues the job with the given job ID.  The job ID should refer to
    a failed job (i.e. it should be on the failed queue).  If no such (failed)
    job exists, a NoSuchJobError is raised.

    将失败的 job 重新入队，如果 job 不存在，抛出 NoSuchJobError
    """
    from .queue import get_failed_queue
    fq = get_failed_queue(connection=connection)
    fq.requeue(job_id)


def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.

    获取当前正在执行的 job
    """
    job_id = _job_stack.top
    if job_id is None:
        return None
    return Job.fetch(job_id, connection=connection)


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.

    用 redis 的哈希表保存 job_id，key 为 'rq:job:' + job_id.encode('utf-8')
    同理用 'rq:job:%s:dependents' % (job_id,) 保存 job 对应的父依赖
    """

    # Job construction
    @classmethod
    def create(cls, func, args=None, kwargs=None, connection=None,
               result_ttl=None, status=None, description=None, depends_on=None, timeout=None):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.

        类方法，按所给参数创建 Job 实例，并附加一些属性
        TODO: 为啥不直接在 __init__() 中实现，避免传入过多参数？因为 job = Job(...) 更直观，
            但用户不会直接 Job()，主要由 queue.queue_call() 调用，或者直接用 job 装饰器，
            这也许是 Job.create(...) 内调用 Job() 存在的意义。
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list.'.format(args))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict.'.format(kwargs))

        job = cls(connection=connection) # 这里只是先调用 __init__()

        # Set the core job tuple properties
        # 给 job 对象附加属性
        # TODO: 好像这里加上判断 func 是否 callable 或 str 会好点
        job._instance = None
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = '%s.%s' % (func.__module__, func.__name__) # 函数要加上模块名
        else:  # we expect a string
            job._func_name = func
        job._args = args
        job._kwargs = kwargs

        # Extra meta data
        job.description = description or job.get_call_string() # job 的描述信息，即调用时的参数列表
        job.result_ttl = result_ttl
        job.timeout = timeout
        job._status = status

        # dependency could be job instance or id
        if depends_on is not None:
            job._dependency_id = depends_on.id if isinstance(depends_on, Job) else depends_on
        return job

    def _get_status(self):
        self._status = as_text(self.connection.hget(self.key, 'status'))
        return self._status

    def _set_status(self, status):
        self._status = status
        self.connection.hset(self.key, 'status', self._status) # 给当前 job 设置 status 域

    status = property(_get_status, _set_status)

    @property
    def is_finished(self):
        return self.status == Status.FINISHED

    @property
    def is_queued(self):
        return self.status == Status.QUEUED

    @property
    def is_failed(self):
        return self.status == Status.FAILED

    @property
    def is_started(self):
        return self.status == Status.STARTED

    @property
    def dependency(self):
        """Returns a job's dependency. To avoid repeated Redis fetches, we cache
        job.dependency as job._dependency.

        job 的依赖，第一次访问从 redis 获取，后面把这个依赖缓存在 _dependency 属性下
        """
        if self._dependency_id is None:
            return None
        if hasattr(self, '_dependency'):
            return self._dependency
        job = Job.fetch(self._dependency_id, connection=self.connection)
        job.refresh() # 根据自身 id 从 redis 获取 job 的属性重新构造
        self._dependency = job
        return job

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        if self.instance:
            return getattr(self.instance, func_name) # 如果是类实例方法，直接返回之

        return import_attribute(self.func_name) # 如果是函数，返回指定路径下模块的属性

    def _unpickle_data(self):
        self._func_name, self._instance, self._args, self._kwargs = unpickle(self.data) # 反序列化

    @property
    def data(self):
        """序列化后的二进制文件"""
        if self._data is UNEVALUATED:
            if self._func_name is UNEVALUATED:
                raise ValueError('Cannot build the job data.')

            if self._instance is UNEVALUATED:
                self._instance = None

            if self._args is UNEVALUATED:
                self._args = ()

            if self._kwargs is UNEVALUATED:
                self._kwargs = {}

            job_tuple = self._func_name, self._instance, self._args, self._kwargs
            self._data = dumps(job_tuple) # 序列化
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED

    @property
    def func_name(self):
        if self._func_name is UNEVALUATED:
            self._unpickle_data()
        return self._func_name

    @func_name.setter
    def func_name(self, value):
        self._func_name = value
        self._data = UNEVALUATED

    @property
    def instance(self):
        if self._instance is UNEVALUATED:
            self._unpickle_data()
        return self._instance

    @instance.setter
    def instance(self, value):
        self._instance = value
        self._data = UNEVALUATED

    @property
    def args(self):
        if self._args is UNEVALUATED:
            self._unpickle_data()
        return self._args

    @args.setter
    def args(self, value):
        self._args = value
        self._data = UNEVALUATED

    @property
    def kwargs(self):
        if self._kwargs is UNEVALUATED:
            self._unpickle_data()
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = value
        self._data = UNEVALUATED

    @classmethod
    def exists(cls, job_id, connection=None):
        """Returns whether a job hash exists for the given job ID.

        指定 job_id 对应的 job 是否已存在
        """
        conn = resolve_connection(connection)
        return conn.exists(cls.key_for(job_id))

    @classmethod
    def fetch(cls, id, connection=None):
        """Fetches a persisted job from its corresponding Redis key and
        instantiates it.

        用对应的 key 从 redis 中取出 Job 实例并初始化
        """
        job = cls(id, connection=connection)
        job.refresh()
        return job

    def __init__(self, id=None, connection=None):
        """构造函数基本没啥用"""
        self.connection = resolve_connection(connection)
        self._id = id
        self.created_at = utcnow()
        self._data = UNEVALUATED
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self._status = None
        self._dependency_id = None
        self.meta = {}

    # Data access
    def get_id(self):  # noqa
        """The job ID for this job instance. Generates an ID lazily the
        first time the ID is requested.

        当前 job 的 id，仅在第一次被请求时惰性求出
        """
        if self._id is None:
            self._id = text_type(uuid4())
        return self._id

    def set_id(self, value):
        """Sets a job ID for the given job."""
        self._id = value

    id = property(get_id, set_id) # 设置 id 属性

    @classmethod
    def key_for(cls, job_id):
        """The Redis key that is used to store job hash under.

        获取指定 job 的 key
        """
        return b'rq:job:' + job_id.encode('utf-8')

    @classmethod
    def dependents_key_for(cls, job_id):
        """The Redis key that is used to store job hash under.

        获取指定 job 所依赖 job 的 key
        """
        return 'rq:job:%s:dependents' % (job_id,)

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return self.key_for(self.id)

    @property
    def dependents_key(self):
        """The Redis key that is used to store job hash under."""
        return self.dependents_key_for(self.id)

    @property
    def result(self):
        """Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be
        None.  But when the job has been executed, and had a return value or
        exception, this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        ReadOnlyJob object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_
        been executed when its return value is None, since return values
        written back to Redis will expire after a given amount of time (500
        seconds by default).
        """
        if self._result is None:
            rv = self.connection.hget(self.key, 'result')
            if rv is not None:
                # cache the result
                self._result = loads(rv)
        return self._result

    """Backwards-compatibility accessor property `return_value`."""
    return_value = result

    # Persistence
    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.

        用 redis 中对应保存的值替换当前 job 的属性，有点类似于重新构造
        """
        key = self.key
        obj = decode_redis_hash(self.connection.hgetall(key)) # hgetall 返回哈希表 key 中，所有的域和值
        if len(obj) == 0:
            raise NoSuchJobError('No such job: %s' % (key,))

        def to_date(date_str):
            if date_str is None:
                return
            else:
                return utcparse(as_text(date_str))

        try:
            self.data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        self.created_at = to_date(as_text(obj.get('created_at')))
        self.origin = as_text(obj.get('origin'))
        self.description = as_text(obj.get('description'))
        self.enqueued_at = to_date(as_text(obj.get('enqueued_at')))
        self.ended_at = to_date(as_text(obj.get('ended_at')))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = obj.get('exc_info')
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None  # noqa
        self._status = as_text(obj.get('status') if obj.get('status') else None)
        self._dependency_id = as_text(obj.get('dependency_id', None))
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}

    def dump(self):
        """Returns a serialization of the current job instance

        序列化当前 job，函数二进制、结果二进制分别用 obj['data']、obj['result'] 访问，其他信息是字符串
        """
        obj = {}
        obj['created_at'] = utcformat(self.created_at or utcnow())
        obj['data'] = self.data # 函数是二进制

        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = utcformat(self.enqueued_at)
        if self.ended_at is not None:
            obj['ended_at'] = utcformat(self.ended_at)
        if self._result is not None:
            obj['result'] = dumps(self._result) # 结果也是二进制
        if self.exc_info is not None:
            obj['exc_info'] = self.exc_info
        if self.timeout is not None:
            obj['timeout'] = self.timeout
        if self.result_ttl is not None:
            obj['result_ttl'] = self.result_ttl
        if self._status is not None:
            obj['status'] = self._status
        if self._dependency_id is not None:
            obj['dependency_id'] = self._dependency_id
        if self.meta:
            obj['meta'] = dumps(self.meta) # 元信息也是二进制

        return obj

    def save(self, pipeline=None):
        """Persists the current job instance to its corresponding Redis key.

        将序列化后的 job 保存到 redis 中
        """
        key = self.key
        connection = pipeline if pipeline is not None else self.connection

        connection.hmset(key, self.dump())

    def cancel(self):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.  Technically, this call is (currently) the same as just
        deleting the job hash.

        撤销指定的 job，即使 job 正在执行。
        TODO: 只是删除 redis 中对应的 key 及其父依赖，这样后续用户也就取不到 job 执行结果，
        实际上 job 的代码应该还是在执行，消耗 CPU 的。
        """
        pipeline = self.connection.pipeline()
        self.delete(pipeline=pipeline)
        pipeline.delete(self.dependents_key)
        pipeline.execute()

    def delete(self, pipeline=None):
        """Deletes the job hash from Redis."""
        connection = pipeline if pipeline is not None else self.connection
        connection.delete(self.key)

    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments."""
        _job_stack.push(self.id)
        try:
            self._result = self.func(*self.args, **self.kwargs)
        finally:
            assert self.id == _job_stack.pop() # 断言栈顶和当前 job_id
        return self._result

    def get_ttl(self, default_ttl=None):
        """Returns ttl for a job that determines how long a job and its result
        will be persisted. In the future, this method will also be responsible
        for determining ttl for repeated jobs.
        """
        return default_ttl if self.result_ttl is None else self.result_ttl

    # Representation
    def get_call_string(self):  # noqa
        """Returns a string representation of the call, formatted as a regular
        Python function invocation statement.

        返回调用函数时的签名表示，例如 foo(1, 2, None, a='a', b={'A': 'a'})
        """
        if self.func_name is None:
            return None

        arg_list = [repr(arg) for arg in self.args]
        arg_list += ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
        args = ', '.join(arg_list)
        return '%s(%s)' % (self.func_name, args)

    def cleanup(self, ttl=None, pipeline=None):
        """Prepare job for eventual deletion (if needed). This method is usually
        called after successful execution. How long we persist the job and its
        result depends on the value of result_ttl:
        - If result_ttl is 0, cleanup the job immediately.
        - If it's a positive number, set the job to expire in X seconds.
        - If result_ttl is negative, don't set an expiry to it (persist
          forever)

        设置 ttl 让 job 及其 result 过期，为完全删除 job 做准备。
        """
        if ttl == 0:
            self.cancel()
        elif ttl > 0:
            connection = pipeline if pipeline is not None else self.connection
            connection.expire(self.key, ttl)

    def register_dependency(self):
        """Jobs may have dependencies. Jobs are enqueued only if the job they
        depend on is successfully performed. We record this relation as
        a reverse dependency (a Redis set), with a key that looks something
        like:

            rq:job:job_id:dependents = {'job_id_1', 'job_id_2'}

        This method adds the current job in its dependency's dependents set.

        把当前 job_id 加入到以父依赖为 key 的 set 中
        """
        # TODO: This can probably be pipelined
        self.connection.sadd(Job.dependents_key_for(self._dependency_id), self.id)

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.description)

    # Job equality
    def __eq__(self, other):  # noqa
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

_job_stack = LocalStack()

注：只是 0.4.0 版本源代码阅读的记录。

在 `rq/utils.py` 底部加了一个用于打印函数调用日志的函数。

```python
def funclog():
    f0 = sys._getframe(1) # funclog 的调用者
    f1 = sys._getframe(2) # funclog 的调用者的上一层

    formatstr = '{0:-<40}{1:->5}   {2:<40}\r{3:-<40}{4:->5}   {5:<20}\n'
    print formatstr.format(f1.f_code.co_filename, f1.f_lineno, f1.f_code.co_name, \
                           f0.f_code.co_filename, f0.f_lineno, f0.f_code.co_name)
```


### Redis 中的结构

- rq:job:`[job_id]`，散列表
- rq:job:`[job_id]`:dependents，集合
- rq:queue:`[queue_name]`，列表
- rq:queues:`[queues]`，集合
- rq:queue:_compact:`uuid4`，列表
- rq:worker:`[worker_id]`，散列表
- rq:workers:`[worker_ids]`，集合


### setup.py 中添加命令

例如安装 rq 时用 `python setup.py install` 会在 Python bin 目录下增加两个命令 `rqworker` 和 `rqinfo`。
正如下面 entry_points 所表明的，这两个命令默认从 `rq/scripts/rqworker.py` 中的 `mian` 函数开始执行。
参考：[http://guide.python-distribute.org/creation.html#entry-points](http://guide.python-distribute.org/creation.html#entry-points)

```python
setup(
    name='rq',
    version=get_version(),
    url='https://github.com/nvie/rq/',
    license='BSD',
    author='Vincent Driessen',
    author_email='vincent@3rdcloud.com',
	...
    entry_points='''\
    [console_scripts]
    rqworker = rq.scripts.rqworker:main
    rqinfo = rq.scripts.rqinfo:main
    ''',
	...
)
```

========== 以下为官方 README =============


[![Build status](https://secure.travis-ci.org/nvie/rq.png?branch=master)](https://secure.travis-ci.org/nvie/rq)

RQ (_Redis Queue_) is a simple Python library for queueing jobs and processing
them in the background with workers.  It is backed by Redis and it is designed
to have a low barrier to entry.  It should be integrated in your web stack
easily.


## Getting started

First, run a Redis server, of course:

```console
$ redis-server
```

To put jobs on queues, you don't have to do anything special, just define
your typically lengthy or blocking function:

```python
import requests

def count_words_at_url(url):
    """Just an example function that's called async."""
    resp = requests.get(url)
    return len(resp.text.split())
```

You do use the excellent [requests][r] package, don't you?

Then, create a RQ queue:

```python
from rq import Queue, use_connection
use_connection()
q = Queue()
```

And enqueue the function call:

```python
from my_module import count_words_at_url
result = q.enqueue(count_words_at_url, 'http://nvie.com')
```

For a more complete example, refer to the [docs][d].  But this is the essence.


### The worker

To start executing enqueued function calls in the background, start a worker
from your project's directory:

```console
$ rqworker
*** Listening for work on default
Got count_words_at_url('http://nvie.com') from default
Job result = 818
*** Listening for work on default
```

That's about it.


## Installation

Simply use the following command to install the latest released version:

    pip install rq

If you want the cutting edge version (that may well be broken), use this:

    pip install -e git+git@github.com:nvie/rq.git@master#egg=rq


## Project history

This project has been inspired by the good parts of [Celery][1], [Resque][2]
and [this snippet][3], and has been created as a lightweight alternative to the
heaviness of Celery or other AMQP-based queueing implementations.

[r]: http://python-requests.org
[d]: http://nvie.github.com/rq/docs/
[m]: http://pypi.python.org/pypi/mailer
[p]: http://docs.python.org/library/pickle.html
[1]: http://www.celeryproject.org/
[2]: https://github.com/defunkt/resque
[3]: http://flask.pocoo.org/snippets/73/

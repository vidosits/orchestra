# Orchestra

Orchestra is:
- a job scheduler
- using [Celery](https://docs.celeryq.dev/) (which is a distributed task queue) under the hood for running tasks/jobs
- using [scheduler](https://digon.io/hyd/project/scheduler/t/master/readme.html) under the hood which is a simple in-process python scheduler library with asyncio, threading and timezone support.

## Dependencies
- python 3.11
- celery > 5.3.6
- sqlalchemy > 2.0.25
- psycopg2-binary
- scheduler > 0.8.5
- pytz > 2023.3.post1

## Getting started

### Schedule definition

```yaml
- name: sample-tasks # this has no bearing on how orchestra works whatsoever, you may name your blocks in any way
  module: tasks # this is the python module where celery will look for the task
  schedules: # a list of schedules
    - name: "short_task_every_1_second" # name of the task that shows up in the logs, *has to be unique*
      task: short_task # the function in the module decorated by `orchestra.task`
      enabled: false # if it's not enabled it will be ignored
      schedule: 
        timing: "every 00:00:01" # see the examples for a list of understood expressions
        timezone: Europe/Budapest # name of a time zone from the tz database
      tags: # tags are list of string, they may be used to group together tasks
        - cpu
        - fast
```

### Timing expressions

A list of examples of understood expressions:

* every 00:00:01
* every 48:00:00
* every 31 minutes and 12 seconds
* every Monday at 03:15:00
* next Tuesday at 13:11:00
* once, Wednesday at 01:15:00
* once on Thursday, 00:31:41
* every day at 09:15:14
* every minute at 15 seconds
* every hour at 05:00
* every day on 9:03
* every day at 9:15
* every day at 13 hours
* once in 13:11:52
* 11:23:00
* 9:13
* 2023-04-01 09:13
* 2023-04-01 09:13:11
* next 23:00:00
* once in 5 hours 2 minutes and 15 seconds
* once in 1 hour and 10 minutes
* once in 2 minutes and 10 seconds
* once in 17 seconds

### Creating an instance
`tasks.py`
```python
import os
from time import sleep

from orchestra import Orchestra

orchestra = Orchestra(
    broker=os.getenv("ORCHESTRA_CELERY_BROKER_CONN_STRING", "sqla+sqlite:///log.db"),
    backend_conn_str=os.getenv('ORCHESTRA_CELERY_BACKEND_DB_CONN_STRING', 'sqlite:///log.db'),
    broker_connection_retry_on_startup=True,
)


def fibo(n: int):
    if n <= 1:
        return n
    else:
        return fibo(n - 1) + fibo(n - 2)


@orchestra.task
def short_task() -> int:
    return fibo(10)


@orchestra.task
def long_task() -> int:
    sleep(10000)
    return fibo(33)
```

`main.py`
```python
import asyncio
import os
import yaml
from orchestra import Orchestra

orchestra = Orchestra(
    broker=os.getenv("ORCHESTRA_CELERY_BROKER_CONN_STRING", "sqla+sqlite:///log.db"),
    backend_conn_str=os.getenv('ORCHESTRA_CELERY_BACKEND_DB_CONN_STRING', 'sqlite:///log.db'),
    broker_connection_retry_on_startup=True,
)


async def main():
    schedule_definitions = yaml.safe_load(
        open(os.getenv("ORCHESTRA_TASK_SCHEDULE", "schedule.yaml"), "rt")
    )
    await orchestra.create_schedule(schedule_definitions)
    await orchestra.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### Adding jobs programmatically

```python
import asyncio
import os
import yaml
import datetime
from orchestra import Orchestra

orchestra = Orchestra(
    broker=os.getenv("ORCHESTRA_CELERY_BROKER_CONN_STRING", "sqla+sqlite:///log.db"),
    backend_conn_str=os.getenv('ORCHESTRA_CELERY_BACKEND_DB_CONN_STRING', 'sqlite:///log.db'),
    broker_connection_retry_on_startup=True,
)


async def main():
    schedule_definitions = yaml.safe_load(
        open(os.getenv("ORCHESTRA_TASK_SCHEDULE", "schedule.yaml"), "rt")
    )
    
    custom_schedule: Schedule = ...
    await orchestra.create_schedule(schedule_definitions)
    orchestra.scheduler.cyclic(datetime.timedelta(seconds=10), orchestra.create_task_from_schedule(module_name, task_name, custom_schedule), alias="sample task")
    await orchestra.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### Using the API

Orchestra comes with a built-in API accessible by setting `api_server=True` on the `Orchestra` instance.
You may optionally provide an `api_address` parameter to control which `host:port` gets bound by `uvicorn`.

```python
import asyncio
import os
import yaml
from orchestra import Orchestra

orchestra = Orchestra(
    broker=os.getenv("ORCHESTRA_CELERY_BROKER_CONN_STRING", "sqla+sqlite:///log.db"),
    backend_conn_str=os.getenv('ORCHESTRA_CELERY_BACKEND_DB_CONN_STRING', 'sqlite:///log.db'),
    enable_api=True,
    api_address="0.0.0.0:5000",
    broker_connection_retry_on_startup=True,
)


async def main():
    schedule_definitions = yaml.safe_load(
        open(os.getenv("ORCHESTRA_TASK_SCHEDULE", "schedule.yaml"), "rt")
    )
    await orchestra.create_schedule(schedule_definitions)
    await orchestra.run()


if __name__ == "__main__":
    asyncio.run(main())
```
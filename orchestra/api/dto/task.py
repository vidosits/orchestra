import json
import datetime
from typing import Any

from pydantic import BaseModel

from orchestra.models import TimingAwareTask, Status


class TaskDTO(BaseModel):
    id: int
    task_id: str
    status: Status
    result: Any
    date_done: datetime.datetime
    traceback: str | None
    args: list
    kwargs: dict
    worker: str
    retries: int
    queue: str
    date_created: datetime.datetime
    date_started: datetime.datetime

    @classmethod
    def map(cls, task: TimingAwareTask) -> "TaskDTO":
        return TaskDTO(id=task.id,
                       task_id=task.task_id,
                       status=Status[task.status.lower()],
                       result=task.result,
                       date_done=task.date_done,
                       traceback=task.traceback,
                       args=json.loads(task.args.decode("utf-8")),
                       kwargs=json.loads(task.kwargs.decode("utf-8")),
                       worker=task.worker,
                       retries=task.retries,
                       queue=task.queue,
                       date_created=task.date_created,
                       date_started=task.date_started)


import datetime
from pydantic import BaseModel

from orchestra.models import Status, Run


class RunDTO(BaseModel):
    id: int
    job_name: str
    status: Status
    module: str
    task: str
    task_id: str
    schedule: str
    timezone: str | None
    triggered_date: datetime.datetime

    @classmethod
    def map(cls, run: Run) -> "RunDTO":
        status = Status[run.task_object.status.lower()] if run.task_object is not None else Status.pending
        return RunDTO(id=run.id,
                      job_name=run.job,
                      status=status,
                      module=run.module,
                      task=run.task,
                      task_id=run.task_id,
                      schedule=run.schedule.lower(),
                      timezone=run.timezone,
                      triggered_date=run.triggered_date)

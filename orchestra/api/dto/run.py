import datetime
from pydantic import BaseModel

from orchestra.models import StatusEnum, Run


class RunDTO(BaseModel):
    id: int
    job_name: str
    status: StatusEnum
    module: str
    task: str
    task_id: str
    schedule: str
    timezone: str | None
    triggered_date: datetime.datetime

    @classmethod
    def map(cls, run: Run) -> "RunDTO":
        status = StatusEnum[run.task_object.status.lower()] if run.task_object is not None else StatusEnum.pending
        return RunDTO(id=run.id,
                      job_name=run.job,
                      status=status,
                      module=run.module,
                      task=run.task,
                      task_id=run.task_id,
                      schedule=run.schedule.lower(),
                      timezone=run.timezone,
                      triggered_date=run.triggered_date)

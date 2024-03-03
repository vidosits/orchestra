import datetime

from pydantic import BaseModel

from orchestra.models import Log


class RunDTO(BaseModel):
    id: int
    job_name: str
    module: str
    task: str
    schedule: str
    timezone: str | None
    triggered_date: datetime.datetime

    @classmethod
    def map(cls, run: Log) -> "RunDTO":
        return RunDTO(id=run.id,
                      job_name=run.job,
                      module=run.module,
                      task=run.task,
                      schedule=run.schedule.lower(),
                      timezone=run.timezone,
                      triggered_date=run.triggered_date)

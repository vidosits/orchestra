import datetime

from pydantic import BaseModel
from scheduler.asyncio.job import Job


class JobDTO(BaseModel):
    schedule: str
    job_name: str | None
    task: str
    start: datetime.datetime | None
    due_at: datetime.datetime
    tzinfo: str | None
    due_in: float
    attempts: int
    max_attempts: str | int
    tags: set[str] | None
    is_paused: bool

    @classmethod
    def map(cls, job: Job) -> "JobDTO":
        return JobDTO(schedule=job._BaseJob__type.name.lower(),
                      job_name=job.alias,
                      start=job.start,
                      due_at=job.datetime,
                      tzinfo=str(job.tzinfo),
                      due_in=job.timedelta(datetime.datetime.now(job.tzinfo)).total_seconds(),
                      task=job.handle.__name__,
                      attempts=job.attempts,
                      max_attempts=job.max_attempts,
                      tags=job.tags,
                      is_paused=job.is_paused)  # is_paused is dynamically added to the native Job object

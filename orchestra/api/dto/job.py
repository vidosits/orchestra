import datetime

from celery.backends.database import session_cleanup
from pydantic import BaseModel
from sqlalchemy import select, func

from orchestra.api.dto import ScheduleDefinitionDTO, RunDTO
from orchestra.core import instance
from orchestra.job import StatefulJob
from orchestra.models import Run, TimingAwareTask


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
    definition: ScheduleDefinitionDTO
    last_run: RunDTO | None
    runs: dict[str, int]

    @classmethod
    def map(cls, stateful_job: StatefulJob) -> "JobDTO":
        session = instance.backend.ResultSession()
        with session_cleanup(session):
            last_run_object = next(session.scalars(select(Run).where(Run.job == stateful_job.job.alias).order_by(Run.triggered_date.desc()).limit(1)), None)
            last_run = RunDTO.map(last_run_object) if last_run_object else None

            states = session.execute(select(TimingAwareTask.status, func.count()).select_from(Run).join(TimingAwareTask, Run.task_id == TimingAwareTask.task_id, isouter=True).where(
                Run.job == stateful_job.job.alias).group_by(TimingAwareTask.status))

            run_counts = {}
            for state, count in states:
                if state is None:
                    run_counts["pending"] = count
                else:
                    run_counts[state.lower()] = count

        return JobDTO(schedule=stateful_job.job._BaseJob__type.name.lower(),
                      job_name=stateful_job.job.alias,
                      start=stateful_job.job.start,
                      due_at=stateful_job.job.datetime,
                      tzinfo=str(stateful_job.job.tzinfo),
                      due_in=stateful_job.job.timedelta(datetime.datetime.now(stateful_job.job.tzinfo)).total_seconds(),
                      task=stateful_job.job.handle.__name__,
                      attempts=stateful_job.job.attempts,
                      max_attempts=stateful_job.job.max_attempts,
                      tags=stateful_job.job.tags,
                      is_paused=stateful_job.is_paused,
                      definition=ScheduleDefinitionDTO.map(stateful_job.job.definition),  # definition is dynamically attached
                      last_run=last_run,
                      runs=run_counts)

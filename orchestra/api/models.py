import datetime

from pydantic import BaseModel


class Job(BaseModel):
    job_type: str
    handle: str
    max_attempts: str | int
    start: datetime.datetime | None
    tzinfo: str | None
    tags: set[str] | None
    alias: str | None

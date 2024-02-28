from fastapi import APIRouter

from orchestra.api.models import Job
from orchestra.core import instance

router = APIRouter()


@router.get("/jobs/", tags=["jobs"])
async def get_all_jobs() -> list[Job]:
    return list(map(lambda job: Job(job_type=job._BaseJob__type.name.lower(),
                                    handle=job.handle.__name__,
                                    max_attempts=job.max_attempts,
                                    tzinfo=str(job.tzinfo),
                                    tags=job.tags,
                                    start=job.start,
                                    alias=job.alias), instance.scheduler.jobs))


@router.get("/users/me", tags=["users"])
async def read_user_me():
    return {"username": "fakecurrentuser"}


@router.get("/users/{username}", tags=["users"])
async def read_user(username: str):
    return {"username": username}

from typing import Annotated

from fastapi import APIRouter, Body, Path, Query

from orchestra.api.dto import JobDTO, ScheduleDefinitionDTO
from orchestra.core import instance

router = APIRouter()


@router.get("/jobs/", tags=["jobs"])
async def get_jobs(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                   any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False) -> list[JobDTO]:
    return list(map(lambda job: JobDTO.map(job), instance.get_jobs(set(tags.split(",")) or {}, any_tag)))


@router.post("/jobs/", tags=["jobs"])
async def schedule_new_job(module: Annotated[str, Body()], schedule_definition: ScheduleDefinitionDTO):
    module_definition = [
        {"module": module,
         "schedules": [
             {
                 "name": schedule_definition.name,
                 "task": schedule_definition.task,
                 "enabled": True,
                 "schedule": {
                     "timing": schedule_definition.timing,
                     "timezone": schedule_definition.timezone or "utc"
                 }
             }
         ]}]
    instance.add_schedules(module_definition, attempt_resume=schedule_definition.resume or False)


@router.post("/jobs/trigger/{job_name}", tags=["jobs"])
async def trigger_job_by_name(job_name: Annotated[str, Path(description="The name of the job to trigger")]):
    await instance.trigger_job(job_name)


@router.post("/jobs/trigger/", tags=["jobs"])
async def trigger_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                               any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False) -> list[JobDTO]:
    return list(map(lambda job: JobDTO.map(job), await instance.trigger_jobs_with_tags(set(tags.split(",")) or {}, any_tag)))


@router.delete("/jobs/{job_name}", tags=["jobs"])
async def delete_scheduled_job(job_name: Annotated[str, Path(description="The name of the job to delete")]):
    instance.delete_job(job_name)


@router.delete("/jobs/", tags=["jobs"])
async def delete_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                              any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False) -> list[JobDTO]:
    return list(map(lambda job: JobDTO.map(job), instance.delete_jobs_with_tags(set(tags.split(",")) or {}, any_tag)))

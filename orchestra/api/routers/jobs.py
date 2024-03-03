from typing import Annotated

from fastapi import APIRouter, Body, Path, Query

from orchestra.api.dto import JobDTO, ScheduleDefinitionDTO
from orchestra.core import instance

router = APIRouter()


@router.get("/jobs/", tags=["jobs"])
async def get_jobs(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                   any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False,
                   include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when searching for a match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.get_jobs(tags, any_tag, include_paused=include_paused)))


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
                                               any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = True,
                                               include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when searching for a match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), await instance.trigger_jobs_with_tags(tags, any_tag, include_paused)))


@router.delete("/jobs/{job_name}", tags=["jobs"])
async def delete_scheduled_job(job_name: Annotated[str, Path(description="The name of the job to delete")]):
    return JobDTO.map(instance.delete_job(job_name))


@router.delete("/jobs/", tags=["jobs"])
async def delete_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                              any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False,
                                              include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when searching for a match")] = False) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.delete_jobs_with_tags(tags, any_tag, include_paused)))


@router.put("/jobs/{job_name}/pause", tags=["jobs"])
async def pause_job(job_name: Annotated[str, Path(description="The name of the job to update")]):
    instance.pause_job(job_name)


@router.post("/jobs/pause/", tags=["jobs"])
async def pause_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                             any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.pause_jobs_with_tags(tags, any_tag)))


@router.put("/jobs/{job_name}/resume", tags=["jobs"])
async def resume_job(job_name: Annotated[str, Path(description="The name of the job to update")]):
    instance.resume_job(job_name)


@router.post("/jobs/resume/", tags=["jobs"])
async def resume_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                              any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.resume_jobs_with_tags(tags, any_tag)))

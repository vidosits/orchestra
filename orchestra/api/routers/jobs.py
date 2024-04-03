from typing import Annotated

from fastapi import APIRouter, Body, Path, Query, Response, status

from orchestra.api.dto import JobDTO, ScheduleDefinitionDTO, RunDTO
from orchestra.core import instance
from orchestra.models import Run

router = APIRouter(prefix="/api")


@router.get("/jobs", tags=["jobs"])
async def get_jobs(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                   any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False,
                   include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when searching for a match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.get_jobs(tags, any_tag, include_paused=include_paused)))


@router.post("/jobs", tags=["jobs"], status_code=201)
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


@router.delete("/jobs", tags=["jobs"])
async def delete_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                              any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False,
                                              include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when searching for a match")] = False) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.delete_jobs_with_tags(tags, any_tag, include_paused)))


@router.post("/jobs/trigger", tags=["jobs"])
async def trigger_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                               any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = True,
                                               include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when searching for a match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), await instance.trigger_jobs_with_tags(tags, any_tag, include_paused)))


@router.post("/jobs/pause", tags=["jobs"])
async def pause_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                             any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.pause_jobs_with_tags(tags, any_tag)))


@router.post("/jobs/resume", tags=["jobs"])
async def resume_scheduled_jobs_matching_tags(tags: Annotated[str | None, Query(description="Comma separated tags to filter scheduled jobs")] = None,
                                              any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = True) -> list[JobDTO]:
    tags: set[str] = set(tags.split(",")) if tags else set()
    return list(map(lambda job: JobDTO.map(job), instance.resume_jobs_with_tags(tags, any_tag)))


@router.get("/jobs/{job_name}", tags=["jobs"], status_code=status.HTTP_200_OK)
async def get_job_by_name(job_name: Annotated[str, Path(description="The name of the job to for which to fetch runs")], response: Response) -> JobDTO | str:
    if instance.job_exists(job_name):
        return JobDTO.map(instance.get_job_by_name(job_name, include_paused=True))
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.get("/jobs/{job_name}/runs", tags=["jobs"], status_code=status.HTTP_200_OK)
async def get_runs_of_job(job_name: Annotated[str, Path(description="The name of the job to for which to fetch runs")], response: Response, page_size: int = 50, page: int = 1) -> list[RunDTO] | str:
    if instance.job_exists(job_name):
        return list(map(lambda run: RunDTO.map(run), instance.get_runs_of_a_job(job_name, page_size, page)))
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.get("/jobs/{job_name}/runs/{run_id}", tags=["jobs"], status_code=status.HTTP_200_OK)
async def get_run_of_job_by_id(job_name: Annotated[str, Path(description="The name of the job to for which to fetch runs")],
                               run_id: Annotated[int, Path(description="The id of the run to fetch")], response: Response) -> RunDTO | str:
    if instance.job_exists(job_name):
        run: Run | None = instance.get_run_of_a_job_by_id(job_name, run_id)
        if run is not None:
            return RunDTO.map(run)
        else:
            response.status_code = status.HTTP_404_NOT_FOUND
            return f"Run with run id {run_id} for job {job_name} does not exist"
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.post("/jobs/{job_name}/trigger", tags=["jobs"])
async def trigger_job_by_name(job_name: Annotated[str, Path(description="The name of the job to trigger")], response: Response) -> None | str:
    if instance.job_exists(job_name):
        response.status_code = status.HTTP_204_NO_CONTENT
        await instance.trigger_job(job_name)
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.put("/jobs/{job_name}/pause", tags=["jobs"], status_code=status.HTTP_200_OK)
async def pause_job(job_name: Annotated[str, Path(description="The name of the job to pause")], response: Response) -> JobDTO | str:
    if instance.job_exists(job_name):
        return JobDTO.map(instance.pause_job(job_name))
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.put("/jobs/{job_name}/resume", tags=["jobs"], status_code=status.HTTP_200_OK)
async def resume_job(job_name: Annotated[str, Path(description="The name of the job to resume")], response: Response) -> JobDTO | str:
    if instance.job_exists(job_name):
        return JobDTO.map(instance.resume_job(job_name))
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.delete("/jobs/{job_name}", tags=["jobs"], status_code=status.HTTP_200_OK)
async def delete_scheduled_job(job_name: Annotated[str, Path(description="The name of the job to delete")], response: Response) -> JobDTO | str:
    if instance.job_exists(job_name):
        return JobDTO.map(instance.delete_job(job_name))
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"

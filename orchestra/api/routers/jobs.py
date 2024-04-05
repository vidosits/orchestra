from typing import Annotated

from fastapi import APIRouter, Body, Path, Query, Response, status
from orchestra.api.dto import RunDTO, JobDTO, ScheduleDefinitionDTO
from orchestra.core import instance
from orchestra.job import StatefulJob
from orchestra.models import Run


router = APIRouter(prefix="/api")


def filter_jobs(tags: list[str] | None = None,
                name_fragment: str | None = None,
                job_names: list[str] | None = None,
                any_tag: bool = False,
                is_paused: bool | None = None) -> set[StatefulJob]:
    tags: set[str] = set(tags) if tags else set()
    name_fragment: str = name_fragment or ""

    results: set[StatefulJob] = set()

    for stateful_job in instance.get_jobs(tags, any_tag, is_paused):
        if name_fragment not in stateful_job.job.alias:
            continue

        if job_names and stateful_job.job.alias not in job_names:
            continue

        results.add(stateful_job)

    return results


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


@router.get("/jobs", tags=["jobs"])
async def get_jobs(tags: Annotated[list[str] | None, Query(description="Optional list of tags to filter scheduled jobs")] = None,
                   name_fragment: Annotated[str | None, Query(description="Optional string to filter scheduled jobs' names")] = None,
                   job_names: Annotated[list[str] | None, Query(description="An optional set of exact job names to match")] = None,
                   any_tag: Annotated[bool, Query(description="Set to true to match if only at least one tag has to match")] = False,
                   is_paused: Annotated[bool | None, Query(description="Filter jobs by their state when searching for a match")] = None) -> list[JobDTO]:
    jobs = filter_jobs(tags, name_fragment, job_names, any_tag, is_paused)
    return list(map(lambda job: JobDTO.map(job), jobs))


@router.delete("/jobs", tags=["jobs"])
async def delete_scheduled_jobs(tags: Annotated[list[str] | None, Body(description="Optional list of tags to filter scheduled jobs")] = None,
                                name_fragment: Annotated[str | None, Body(description="Optional string to filter scheduled jobs' names")] = None,
                                job_names: Annotated[list[str] | None, Body(description="An optional set of exact job names to match")] = None,
                                any_tag: Annotated[bool, Body(description="Set to true to match if only at least one tag has to match")] = False,
                                is_paused: Annotated[bool | None, Body(description="Filter jobs by their state when searching for a match")] = None) -> list[JobDTO]:
    jobs = filter_jobs(tags, name_fragment, job_names, any_tag, is_paused)
    return list(map(lambda job: JobDTO.map(job), instance.delete_jobs(jobs)))


@router.post("/jobs/trigger", tags=["jobs"])
async def trigger_scheduled_jobs(tags: Annotated[list[str] | None, Body(description="Optional list of tags to filter scheduled jobs")] = None,
                                 name_fragment: Annotated[str | None, Body(description="Optional string to filter scheduled jobs' names")] = None,
                                 job_names: Annotated[list[str] | None, Body(description="An optional set of exact job names to match")] = None,
                                 any_tag: Annotated[bool, Body(description="Set to true to match if only at least one tag has to match")] = False,
                                 is_paused: Annotated[bool | None, Body(description="Filter jobs by their state when searching for a match")] = None) -> list[JobDTO]:
    jobs = filter_jobs(tags, name_fragment, job_names, any_tag, is_paused)
    return list(map(lambda job: JobDTO.map(job), await instance.trigger_jobs(jobs)))


@router.put("/jobs/pause", tags=["jobs"])
async def pause_scheduled_jobs(tags: Annotated[list[str] | None, Body(description="Optional list of tags to filter scheduled jobs")] = None,
                               name_fragment: Annotated[str | None, Body(description="Optional string to filter scheduled jobs' names")] = None,
                               job_names: Annotated[list[str] | None, Body(description="An optional set of exact job names to match")] = None,
                               any_tag: Annotated[bool, Body(description="Set to true to match if only at least one tag has to match")] = False,
                               is_paused: Annotated[bool | None, Body(description="Filter jobs by their state when searching for a match")] = None) -> list[JobDTO]:
    jobs = filter_jobs(tags, name_fragment, job_names, any_tag, is_paused)
    return list(map(lambda job: JobDTO.map(job), instance.pause_jobs(jobs)))


@router.put("/jobs/resume", tags=["jobs"])
async def resume_scheduled_jobs(tags: Annotated[list[str] | None, Body(description="Optional list of tags to filter scheduled jobs")] = None,
                                name_fragment: Annotated[str | None, Body(description="Optional string to filter scheduled jobs' names")] = None,
                                job_names: Annotated[list[str] | None, Body(description="An optional set of exact job names to match")] = None,
                                any_tag: Annotated[bool, Body(description="Set to true to match if only at least one tag has to match")] = False,
                                is_paused: Annotated[bool | None, Body(description="Filter jobs by their state when searching for a match")] = None) -> list[JobDTO]:
    jobs = filter_jobs(tags, name_fragment, job_names, any_tag, is_paused)
    return list(map(lambda job: JobDTO.map(job), instance.resume_jobs(jobs)))


@router.get("/jobs/{job_name}", tags=["jobs"], status_code=status.HTTP_200_OK)
async def get_job_by_name(job_name: Annotated[str, Path(description="The name of the job to for which to fetch runs")], response: Response) -> JobDTO | str:
    if instance.job_exists(job_name):
        return JobDTO.map(instance.get_job_by_name(job_name))
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Job {job_name} does not exist"


@router.get("/jobs/{job_name}/runs", tags=["jobs"], status_code=status.HTTP_200_OK)
async def get_runs_of_job(job_name: Annotated[str, Path(description="The name of the job to for which to fetch runs")], response: Response,
                          run_status: Annotated[str, Query(description="Filter the runs of the job to this status, leave empty to not filter")] = "",
                          page_size: Annotated[int, Query(description="The amount of runs returned per page")] = 50,
                          page: Annotated[int, Query(description="The page of results to return")] = 1) -> list[RunDTO] | str:
    if instance.job_exists(job_name):
        return list(map(lambda run: RunDTO.map(run), instance.get_runs_of_a_job(job_name, run_status, page_size, page)))
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

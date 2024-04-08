from typing import Annotated

from fastapi import APIRouter, Query
from orchestra.core import instance

router = APIRouter(prefix="/api")


@router.get("/tags", tags=["tags"])
async def get_all_tags(is_paused: Annotated[bool | None, Query(description="Filter jobs by their state when searching for a match")] = None) -> set[str]:
    collected_tags: set[str] = set()
    for stateful_job in instance.get_jobs(set(), any_tag=True, is_paused=is_paused):
        collected_tags.update(stateful_job.job.tags)
    return collected_tags

from typing import Annotated

from fastapi import APIRouter, Query
from orchestra.core import instance

router = APIRouter(prefix="/api")


@router.get("/tags", tags=["tags"])
async def get_all_tags(include_paused: Annotated[bool, Query(description="Set to true to include paused jobs when collecting tags")] = True) -> set[str]:
    collected_tags: set[str] = set()
    for job in instance.get_jobs(set(), any_tag=True, include_paused=include_paused):
        collected_tags.update(job.tags)
    return collected_tags

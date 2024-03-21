from typing import Annotated

from fastapi import APIRouter, Path, Response, status

from orchestra.api.dto import TaskDTO
from orchestra.core import instance

router = APIRouter(prefix="/api")


@router.get("/tasks/{task_id}", tags=["tasks"])
async def get_task_by_id(task_id: Annotated[str, Path(description="The ID of the task to return")], response: Response) -> TaskDTO | str:
    task = instance.get_task_by_id(task_id)
    if task is None:
        response.status_code = status.HTTP_404_NOT_FOUND
        return f"Task with id {task_id} does not exist"
    else:
        return TaskDTO.map(task)

from pydantic import BaseModel


class ScheduleDefinitionDTO(BaseModel):
    name: str
    task: str
    timing: str
    timezone: str | None = None
    tags: list[str] | None = None
    resume: bool = False

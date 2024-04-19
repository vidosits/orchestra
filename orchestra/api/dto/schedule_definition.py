from pydantic import BaseModel


class ScheduleDefinitionDTO(BaseModel):
    name: str
    task: str
    timing: str
    timezone: str | None = None
    tags: list[str] | None = None
    resume_from_previous: bool = False

    @classmethod
    def map(cls, definition: dict) -> "ScheduleDefinitionDTO":
        return ScheduleDefinitionDTO(name=definition.get("name"),
                                     task=definition.get("task"),
                                     timing=definition["schedule"].get("timing"),
                                     timezone=definition["schedule"].get("timezone"),
                                     tags=definition.get("tags"),
                                     resume_from_previous=definition["schedule"].get("resume_from_previous"))

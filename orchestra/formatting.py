import yaml
from rich import box
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table
from scheduler import Scheduler
from scheduler.asyncio.job import Job


def pretty_print_block(config_block: dict) -> str:
    return yaml.dump(config_block, allow_unicode=True, default_flow_style=False)


def transform_timing_to_schedule(timing: str) -> str:
    """
    timing: "day", "hour", "minute"

    returns daily, hourly, minutely, whichever is appropriate
    """

    return "daily" if timing == "day" else f"{timing}ly"


def get_job_state(job: Job) -> list[str]:
    row = job._str()
    return [
        row[0],
        row[1] + row[2],
        job.handle.__name__,
        row[3],
        str(job.datetime.tzinfo),
        row[5],
        f"{row[6]}/{row[7]}",
        ",".join(job.tags)
    ]
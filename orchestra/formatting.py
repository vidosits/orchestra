import yaml
from scheduler.asyncio.job import Job

from orchestra.job import StatefulJob


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


def get_job_state_for_always_running(job: StatefulJob) -> list[str]:
    row = job.job._str()
    return [
        "[yellow]Paused[/]" if job.is_paused else "[green]Running[/]",
        "Always On",
        row[1] + row[2],
        job.job.handle.__name__,
        "-",
        str(job.job.datetime.tzinfo),
        "∞",
        "1",
        ",".join(job.job.tags)
    ]

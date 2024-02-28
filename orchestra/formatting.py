import yaml
from rich import box
from rich.table import Table
from scheduler import Scheduler


def pretty_print_block(config_block: dict) -> str:
    return yaml.dump(config_block, allow_unicode=True, default_flow_style=False)


def transform_timing_to_frequency(timing: str) -> str:
    """
    timing: "day", "hour", "minute"

    returns daily, hourly, minutely, whichever is appropriate
    """

    return "daily" if timing == "day" else f"{timing}ly"


def get_scheduler_status_table(scheduler: Scheduler) -> Table:
    table = Table()
    table.border_style = "bright_yellow"
    table.box = box.ROUNDED
    table.pad_edge = False

    table.add_column("Frequency", style="green")
    table.add_column("Job name", style="blue")
    table.add_column("Module and task", style="magenta")
    table.add_column("Due at", style="red")
    table.add_column("Timezone", style="bright_blue")
    table.add_column("Due in", style="cyan")
    table.add_column("Attempts")

    for job in scheduler.jobs:
        row = job._str()
        entries = (
            row[0],
            row[1] + row[2],
            job.handle.__name__,
            row[3],
            str(job.datetime.tzinfo),
            row[5],
            f"{row[6]}/{row[7]}",
        )
        table.add_row(*entries)

    return table

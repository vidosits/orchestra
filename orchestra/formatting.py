import yaml
from rich import box
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
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
    grid = Table.grid(expand=True)
    grid.add_column()

    table = Table(expand=True)
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
    table.add_column("Tags")

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
            ",".join(job.tags)
        )
        table.add_row(*entries)

    progress = Progress(TextColumn("{task.description}"), BarColumn(bar_width=None), expand=True, transient=True)
    progress.add_task("Orchestrating jobs", total=None)
    grid.add_row(table)
    grid.add_row(Panel(progress, expand=True, border_style="bright_yellow"))

    return grid

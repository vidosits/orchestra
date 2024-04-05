import asyncio
import datetime
import importlib
import logging
from typing import Callable, Any

import celery
import pytz
import uvicorn
from celery import Celery
from celery.backends.database import session_cleanup
from rich import box
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, TextColumn, BarColumn
from rich.table import Table
from scheduler.asyncio import Scheduler
from scheduler.asyncio.job import Job
from scheduler.trigger.core import Weekday
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from orchestra.formatting import pretty_print_block, get_job_state
from orchestra.job import StatefulJob
from orchestra.models import Run, TimingAwareTask
from orchestra.scheduling import Schedule

logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, tracebacks_show_locals=True)]
)

logger = logging.getLogger("orchestra.core")

instance: "Orchestra | None" = None


class Orchestra(Celery):
    def __init__(self, backend_conn_str: str, backend_module_name: str = "orchestra.backend.OrchestraBackend", enable_api=False, api_address: str = "localhost:5000",
                 scheduler_loop_resolution_in_seconds: float | int = 0.1, **kwargs):
        super().__init__(
            backend=f"{backend_module_name}+{backend_conn_str}",
            result_extended=True,
            task_track_started=True,
            task_send_sent_event=True,
            **kwargs,
        )
        self.scheduler: Scheduler | None = None
        self.loop = None
        self.enable_api = enable_api
        self.api_address = api_address
        self.loop_resolution_in_seconds = scheduler_loop_resolution_in_seconds
        self.server: uvicorn.Server | None = None
        self.paused_jobs: set[Job] = set()

    def get_event_loop(self, loop):
        self.loop = loop or asyncio.get_running_loop()

    async def api_server(self):
        log_config = uvicorn.config.LOGGING_CONFIG
        log_config["loggers"] = []

        try:
            host, port_str = self.api_address.split(":")
            port = int(port_str)
        except ValueError:
            host = self.api_address
            port = 8000

        config = uvicorn.Config("orchestra.api.main:app", host=host, port=port, log_config=log_config)

        self.server = uvicorn.Server(config)
        await self.server.serve()

    async def live_output(self):
        with Live(
                self.get_scheduler_status_table(), refresh_per_second=10
        ) as live:
            while True:
                await asyncio.sleep(self.loop_resolution_in_seconds)
                live.update(self.get_scheduler_status_table())

    async def run(self) -> None:
        logger.info("Orchestra starting")

        # we need to run 2 tasks in parallel
        # first one is the live output on the CLI/TUI:
        tasks = [asyncio.create_task(self.live_output())]

        # the second one is the optionally enabled API server
        if self.enable_api:
            tasks.append(asyncio.create_task(self.api_server()))
            global instance
            instance = self
        try:
            # uvicorn swallows Cancellation requests >>>:((((
            # https://github.com/encode/uvicorn/issues/1579
            # https://docs.python.org/3/library/asyncio-task.html#task-cancellation
            # we have to exit when either uvicorn (api server) or the Orchestra scheduler stops
            _, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            # if we get to this point then one of the above tasks were cancelled by the user
            logger.warning("User-requested shutdown.")
            for task in pending_tasks:
                task.cancel()

            await asyncio.wait(pending_tasks)

        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.warning("User-requested shutdown.")

    def wrap_celery_task(self, job_name: str, task: celery.Task, additional_options: dict) -> Callable:
        async def celery_task():
            task_meta = task.apply_async(**additional_options)
            logger.debug(f"Triggered job {job_name}")
            task_meta.created_date = datetime.datetime.now(tz=pytz.utc)

            session = self.backend.ResultSession()
            with session_cleanup(session):
                trigger_timestamp = datetime.datetime.now(tz=pytz.utc)
                stateful_job = self.get_job_by_name(job_name)
                if stateful_job is None:
                    logger.critical(f"Unknown job with name {job_name}")

                module_name, _, task_name = task.name.rpartition(".")

                scheduler_log = Run(
                    job=job_name,
                    module=module_name,
                    task=task_name,
                    task_id=task_meta.id,
                    schedule=stateful_job.job.type.name if stateful_job.job.max_attempts != 1 else "ONCE",
                    timezone=stateful_job.job.datetime.tzinfo.zone,
                    triggered_date=trigger_timestamp,
                )
                session.add(scheduler_log)
                session.commit()

        celery_task.__name__ = task.name
        return celery_task

    def __create_job_from_celery_task_schedule(
            self, job_name: str, module_name: str, task_name: str, additional_options: dict) -> Callable:
        module = importlib.import_module(module_name)
        task: celery.Task = getattr(module, task_name)

        return self.wrap_celery_task(job_name, task, additional_options)

    def schedule_celery_task(self, job_name: str, task: Callable, schedule: Callable[..., Job], timing: datetime.datetime | datetime.timedelta | datetime.time | Weekday,
                             tags: set[str] | None = None, attempt_resume: bool = False, additional_options: dict = None):
        module_name, _, task_name = task.name.rpartition(".")  # type:ignore
        self.schedule_job(job_name=job_name, module_name=module_name, task_name=task_name, schedule=schedule, timing=timing, tags=tags, attempt_resume=attempt_resume,
                          additional_options=additional_options)

    async def create_schedule(self, module_definitions: list[dict] = None, loop=None) -> None:
        self.scheduler = self.scheduler or Scheduler(tzinfo=pytz.utc, loop=self.get_event_loop(loop))
        if module_definitions is not None:
            self.add_schedules(module_definitions)

    def schedule_job(self, job_name: str, module_name: str, task_name: str, schedule: Callable[..., Job], timing: datetime.datetime | datetime.timedelta | datetime.time | Weekday,
                     tags: set[str] | None = None, attempt_resume: bool = False, additional_options: dict = None, schedule_definition: Any = None):
        session = self.backend.ResultSession()
        with session_cleanup(session):
            resume_parameters: dict = {}
            if attempt_resume:
                logs = (
                    select(Run)
                    .where(Run.job == job_name)
                    .order_by(Run.triggered_date.desc())
                    .limit(1)
                )
                last_run: Run | None = next(session.scalars(logs), None)
                if last_run is not None:
                    last_run_utc = last_run.triggered_date.replace(tzinfo=pytz.utc)
                    last_running_time_local = last_run_utc.astimezone(
                        pytz.timezone(last_run.timezone)
                    )
                    match last_run.schedule:
                        case "ONCE":
                            logger.warning(
                                f"Job {job_name} was scheduled to run exactly once, but it has already ran on {last_running_time_local}, tz={last_run.timezone}, not scheduling again."
                            )
                            return None

                        case "CYCLIC":
                            utc_now = datetime.datetime.now(tz=pytz.utc)
                            next_run_utc = last_run_utc + datetime.timedelta(
                                seconds=((utc_now - last_run_utc).total_seconds() // timing.total_seconds()) * timing.total_seconds() + timing.total_seconds()
                            )
                            next_run_local = next_run_utc.astimezone(
                                pytz.timezone(last_run.timezone)
                            )

                            resume_parameters = {
                                "start": next_run_local - datetime.timedelta(seconds=timing.total_seconds())
                            }

            job = schedule(
                timing=timing,
                handle=self.__create_job_from_celery_task_schedule(
                    job_name, module_name, task_name, additional_options or {}
                ),
                alias=job_name,
                tags=tags or set(),
                **resume_parameters,
            )

            job.definition = schedule_definition or {}

            if "start" in resume_parameters:
                logger.warning(
                    f"Job {job_name} was scheduled to run {timing}, resuming using {last_running_time_local}, tz={last_run.timezone} as reference time."
                    f" Next run at {next_run_local}, tz={last_run.timezone}"
                )
            else:
                logger.info(f"Job {job_name} was scheduled to run {timing}")

    def add_schedules(self, module_definitions: list[dict], attempt_resume: bool = True):
        for index, block in enumerate(module_definitions or []):
            block_name: str = block.get("name", f"{index + 1}. schedule block")
            block_module: str | None = block.get("module")

            if block_module is None:
                error_message: str = f"Module is undefined for {block_name}. Definition:\n\n{pretty_print_block(block)}"
                logger.error(error_message)
                raise ValueError(error_message)

            schedule_definitions: dict | None = block.get("schedules")

            if schedule_definitions is None:
                error_message: str = f'Missing schedule definition for "{block_name}" module. Module definition:\n\n{pretty_print_block(block)}'
                logger.error(error_message)
                raise ValueError(error_message)

            for task_index, schedule_definition in enumerate(schedule_definitions):
                if "name" not in schedule_definition:
                    logger.critical(f"Missing job name in schedule in {block_name} module. Job definition:\n\n{pretty_print_block(schedule_definition)}")
                    continue

                job_name: str = schedule_definition.get("name")

                if not schedule_definition.get("enabled", False):
                    logger.info(f"Ignoring disabled job {job_name} in {block_name}. Job definition:\n\n{pretty_print_block(schedule_definition)}")
                    continue

                task_name: str = schedule_definition.get("task")
                if task_name is None:
                    logger.error(f"Task is undefined for {job_name} in {block_name}. Definition:\n\n{pretty_print_block(schedule_definition)}")
                    continue

                if self.get_job_by_name(job_name) is not None:
                    logger.critical(f"Can't add job with name {job_name}, because a job with that name already exists")
                    continue

                if not schedule_definition.get("schedule", False):
                    logger.critical(f'Missing schedule definition for enabled Job "{block_name}". Job definition:\n\n{pretty_print_block(schedule_definition)}')
                    continue

                schedule = Schedule(
                    definition=schedule_definition, scheduler=self.scheduler
                )

                self.schedule_job(job_name=job_name,
                                  module_name=block_module,
                                  task_name=task_name,
                                  schedule=schedule.job_type,
                                  timing=schedule.get_timing(),
                                  tags=set(schedule_definition.get("tags", set())),
                                  attempt_resume=attempt_resume,
                                  additional_options=schedule_definition.get("additional_options"),
                                  schedule_definition=schedule_definition)

    def apply_state_to_jobs(self, jobs: set[Job]) -> set[StatefulJob]:
        return set([StatefulJob(job, is_paused=True if job in self.paused_jobs else False) for job in jobs])

    def get_jobs(self, tags: set[str] | None = None, any_tag: bool = True, is_paused: bool | None = None) -> set[StatefulJob]:
        tags = tags or set()
        if is_paused is None:
            jobs = self.paused_jobs.union(self.scheduler.jobs)
        else:
            jobs = self.paused_jobs if is_paused else self.scheduler.jobs

        if len(tags) > 0:
            jobs = set(filter(lambda job: job.tags.intersection(tags) if any_tag else job.tags == tags, jobs))

        return self.apply_state_to_jobs(jobs)

    def job_exists(self, job_name: str) -> bool:
        return self.get_job_by_name(job_name) is not None

    def get_jobs_by_state(self, is_paused: bool | None) -> set[StatefulJob]:
        match is_paused:
            case None:
                return self.apply_state_to_jobs(self.scheduler.jobs.union(self.paused_jobs))
            case True:
                return self.apply_state_to_jobs(self.paused_jobs)
            case False:
                return self.apply_state_to_jobs(self.scheduler.jobs)

    def get_job_by_name(self, job_name: str) -> StatefulJob | None:
        for stateful_job in self.get_jobs():
            if stateful_job.job.alias == job_name:
                return stateful_job

    def pause_job(self, job_name: str) -> StatefulJob:
        job = self.get_job_by_name(job_name).job
        assert job is not None
        self.scheduler.delete_job(job)
        self.paused_jobs.add(job)
        logger.info(f"Paused job {job_name}")
        return StatefulJob(job, is_paused=True)

    def pause_jobs(self, jobs_to_pause: set[StatefulJob]) -> set[StatefulJob]:
        for stateful_job in jobs_to_pause:
            self.pause_job(stateful_job.job.alias)

        return self.apply_state_to_jobs(set(map(lambda state: state.job, jobs_to_pause)))

    def resume_job(self, job_name: str) -> StatefulJob:
        stateful_job = self.get_job_by_name(job_name)
        if stateful_job.is_paused:
            self.paused_jobs.remove(stateful_job.job)
            task = self.scheduler._Scheduler__loop.create_task(self.scheduler._Scheduler__supervise_job(stateful_job.job))
            self.scheduler._Scheduler__jobs[stateful_job.job] = task
            logger.info(f"Resumed job {job_name}")

        return StatefulJob(stateful_job.job, is_paused=False)

    def resume_jobs(self, jobs_to_resume: set[StatefulJob]) -> set[StatefulJob]:
        for stateful_job in jobs_to_resume:
            self.resume_job(stateful_job.job.alias)

        return self.apply_state_to_jobs(set(map(lambda state: state.job, jobs_to_resume)))

    def delete_job(self, job_name: str) -> StatefulJob | None:
        stateful_job = self.get_job_by_name(job_name)
        if stateful_job.is_paused:
            self.paused_jobs.remove(stateful_job.job)
        else:
            self.scheduler.delete_job(stateful_job.job)

        return stateful_job

    def delete_jobs(self, jobs_to_delete: set[StatefulJob]) -> set[StatefulJob]:
        for stateful_job in jobs_to_delete:
            if stateful_job.is_paused:
                self.paused_jobs.remove(stateful_job.job)
            else:
                self.scheduler.delete_job(stateful_job.job)

        return jobs_to_delete

    async def trigger_job(self, job_name: str):
        job = self.get_job_by_name(job_name).job
        assert job is not None
        await job.handle()
        logger.info(f"Job {job_name} triggered manually")

    async def trigger_jobs(self, jobs_to_trigger: set[StatefulJob]) -> set[StatefulJob]:
        async with asyncio.TaskGroup() as tg:
            [tg.create_task(stateful_job.job.handle()) for stateful_job in jobs_to_trigger]

        for stateful_job in jobs_to_trigger:
            logger.info(f"Job {stateful_job.job.alias} triggered manually")

        return jobs_to_trigger

    def get_runs_of_a_job(self, job_name: str, run_status: str, page_size: int, page: int) -> list[Run]:
        session = self.backend.ResultSession()
        with session_cleanup(session):
            if run_status == "pending":
                runs = list(session.scalars(select(Run).where(Run.job == job_name, ~Run.task_status.has()).order_by(Run.triggered_date.desc()).options(joinedload(Run.task_object))))
            else:
                runs: list[Run] = list(
                    session.scalars(
                        select(Run).where(Run.job == job_name, Run.task_status == run_status.upper()).order_by(Run.triggered_date.desc()).offset((page - 1) * page_size).limit(
                            page_size).options(joinedload(Run.task_object))))
            return runs

    def get_run_of_a_job_by_id(self, job_name: str, run_id: int) -> Run | None:
        session = self.backend.ResultSession()
        with session_cleanup(session):
            run: Run = next(session.scalars(select(Run).where(Run.job == job_name, Run.id == run_id).limit(1).options(joinedload(Run.task_object))), None)
            return run

    def get_task_by_id(self, task_id: str) -> TimingAwareTask | None:
        session = self.backend.ResultSession()
        with session_cleanup(session):
            run_of_task = next(session.scalars(select(Run).where(Run.task_object.has(TimingAwareTask.task_id == task_id)).limit(1).options(joinedload(Run.task_object))), None)
            if run_of_task is None:
                return None
            task: TimingAwareTask = run_of_task.task_object
            task.run_id = run_of_task.id
            task.job_name = run_of_task.job
            return task

    def get_scheduler_status_table(self) -> Table:
        grid = Table.grid(expand=True)
        grid.add_column()

        table = Table(expand=True)
        table.border_style = "bright_yellow"
        table.box = box.ROUNDED
        table.pad_edge = False

        table.add_column("State")
        table.add_column("Shedule", style="green")
        table.add_column("Job name", style="blue")
        table.add_column("Module and task", style="magenta")
        table.add_column("Due at", style="red")
        table.add_column("Timezone", style="bright_blue")
        table.add_column("Due in", style="cyan")
        table.add_column("Attempts")
        table.add_column("Tags")

        for job in self.scheduler.jobs:
            table.add_row(*(["[green]Running[/]"] + get_job_state(job)))

        for job in self.paused_jobs:
            table.add_row(*(["[yellow]Paused[/]"] + get_job_state(job)))

        progress = Progress(TextColumn("{task.description}"), BarColumn(bar_width=None), expand=True, transient=True)
        progress.add_task("Orchestrating jobs", total=None)
        grid.add_row(table)
        grid.add_row(Panel(progress, expand=True, border_style="bright_yellow"))

        return grid

import asyncio
import datetime
import importlib
import logging
from typing import Callable

import pytz
import uvicorn
from celery import Celery
from celery.backends.database import session_cleanup
from rich.live import Live
from rich.logging import RichHandler
from scheduler.asyncio import Scheduler
from scheduler.asyncio.job import Job
from sqlalchemy import select

from orchestra.formatting import get_scheduler_status_table, pretty_print_block
from orchestra.models import Log
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

        config = uvicorn.Config("orchestra.api.main:app", host=host, port=port, log_config=log_config, reload=True, reload_includes="./**")

        self.server = uvicorn.Server(config)
        await self.server.serve()

    async def live_output(self):
        with Live(
                get_scheduler_status_table(self.scheduler), refresh_per_second=10
        ) as live:
            while True:
                await asyncio.sleep(self.loop_resolution_in_seconds)
                live.update(get_scheduler_status_table(self.scheduler))

    async def run(self) -> None:
        logger.info("Orchestra starting")

        # we need to run 2 tasks in parallel
        # first one is the live output on the CLI/TUI:sdf
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

    def get_job_by_name(self, job_name: str) -> Job | None:
        for job in self.scheduler.jobs:
            if job.alias == job_name:
                return job

    def create_task_from_schedule(
            self, module_name: str, task_name: str, schedule: Schedule
    ) -> Callable:
        module = importlib.import_module(module_name)
        task = getattr(module, schedule.definition.get("task"))

        async def celery_task():
            session = self.backend.ResultSession()
            with session_cleanup(session):
                trigger_timestamp = datetime.datetime.now(tz=pytz.utc)
                job = self.get_job_by_name(task_name)
                if job is None:
                    logger.critical(f"Unknown job with name {task_name}")

                scheduler_log = Log(
                    job=task_name,
                    module=module_name,
                    task=schedule.definition.get("task"),
                    frequency=job.type.name if job.max_attempts != 1 else "ONCE",
                    timezone=schedule.timezone.tzname(None),
                    triggered_date=trigger_timestamp,
                )
                session.add(scheduler_log)
                session.commit()

            task_meta = task.delay()
            task_meta.created_date = datetime.datetime.now(tz=pytz.utc)

        celery_task.__name__ = task.name
        return celery_task

    async def create_schedule(self, module_definitions: list[dict] = None, loop=None) -> None:
        self.scheduler = self.scheduler or Scheduler(tzinfo=pytz.utc, loop=self.get_event_loop(loop))
        if module_definitions is not None:
            self.add_schedules(module_definitions)

    def add_schedules(self, module_definitions: list[dict], attempt_resume: bool = True):
        session = self.backend.ResultSession()
        with session_cleanup(session):
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
                        logger.critical(f"Missing job name in schedule in {block_name} module. Task definition:\n\n{pretty_print_block(schedule_definition)}")
                        continue

                    task_name: str = schedule_definition.get("name")

                    if self.get_job_by_name(task_name) is not None:
                        logger.critical(f"Can't add job with name {task_name}, because there is already a task named like that")
                        continue

                    if not schedule_definition.get("enabled", False):
                        logger.info(f"Ignoring disabled task {task_name} in {block_name} Task definition:\n\n{pretty_print_block(schedule_definition)}")
                        continue

                    if not schedule_definition.get("schedule", False):
                        logger.critical(f'Missing schedule definition for enabled task "{block_name}". Task definition:\n\n{pretty_print_block(schedule_definition)}')
                        continue

                    schedule = Schedule(
                        definition=schedule_definition, scheduler=self.scheduler
                    )

                    resume_parameters: dict = {}
                    if attempt_resume:
                        logs = (
                            select(Log)
                            .where(Log.job == task_name)
                            .order_by(Log.triggered_date.desc())
                            .limit(1)
                        )
                        last_run: Log | None = next(session.scalars(logs), None)
                        if last_run is not None:
                            last_run_utc = last_run.triggered_date.replace(tzinfo=pytz.utc)
                            last_running_time_local = last_run_utc.astimezone(
                                pytz.timezone(last_run.timezone)
                            )
                            match last_run.frequency:
                                case "ONCE":
                                    logger.warning(
                                        f"Job {task_name} was scheduled to run exactly once, but it has already ran on {last_running_time_local}, tz={last_run.timezone}, not scheduling again."
                                    )
                                    continue

                                case "CYCLIC":
                                    utc_now = datetime.datetime.now(tz=pytz.utc)
                                    next_run_utc = last_run_utc + datetime.timedelta(
                                        seconds=(
                                                        (utc_now - last_run_utc).total_seconds()
                                                        // schedule.get_timing().total_seconds()
                                                )
                                                * schedule.get_timing().total_seconds()
                                                + schedule.get_timing().total_seconds()
                                    )
                                    next_run_local = next_run_utc.astimezone(
                                        pytz.timezone(last_run.timezone)
                                    )

                                    resume_parameters = {
                                        "start": next_run_local - datetime.timedelta(seconds=schedule.get_timing().total_seconds())
                                    }

                    schedule.job_type(
                        timing=schedule.get_timing(),
                        handle=self.create_task_from_schedule(
                            block_module, task_name, schedule
                        ),
                        alias=task_name,
                        tags=set(schedule_definition.get("tags", {})),
                        **resume_parameters,
                    )
                    if "start" in resume_parameters:
                        logger.warning(
                            f"Job {task_name} was scheduled to run every {schedule.get_timing()}, resuming using {last_running_time_local}, tz={last_run.timezone} as reference time."
                            f" Next run at {next_run_local}, tz={last_run.timezone}"
                        )
                    else:
                        logger.info(f"Job {task_name} was scheduled to run every {schedule_definition.get('schedule').get('timing')}, {schedule.timezone}")

    def get_jobs(self, tags: set[str], any_tag: bool):
        return self.scheduler.get_jobs(tags, any_tag=any_tag)

    def delete_job(self, job_name: str):
        self.scheduler.delete_job(self.get_job_by_name(job_name))

    def delete_jobs_with_tags(self, tags: set[str], any_tag: bool):
        jobs_to_delete = self.get_jobs(tags, any_tag)
        self.scheduler.delete_jobs(tags, any_tag)
        return jobs_to_delete

    async def trigger_job(self, job_name: str):
        job = self.get_job_by_name(job_name)
        await self.get_job_by_name(job_name).handle()
        if job.max_attempts == 1:
            self.scheduler.delete_job(job)

    async def trigger_jobs_with_tags(self, tags: set[str], any_tag: bool):
        jobs_to_trigger = self.get_jobs(tags, any_tag)

        async with asyncio.TaskGroup() as tg:
            [tg.create_task(job.handle()) for job in jobs_to_trigger]

        for job in jobs_to_trigger:
            if job.max_attempts == 1:
                self.scheduler.delete_job(job)

        return jobs_to_trigger

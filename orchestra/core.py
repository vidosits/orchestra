import asyncio
import datetime
import importlib
import logging
import signal
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


class Orchestra(Celery):
    def __init__(self, backend_conn_str: str, backend_module_name: str = "orchestra.backend.OrchestraBackend", enable_api=False, scheduler_loop_resolution_in_seconds: float | int = 0.1, **kwargs):
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
        self.loop_resolution_in_seconds = scheduler_loop_resolution_in_seconds
        self.server: uvicorn.Server | None = None

    async def api_server(self):
        config = uvicorn.Config("orchestra.api:app", port=5000)

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

        tasks = [asyncio.create_task(self.live_output())]

        if self.enable_api:
            tasks.append(asyncio.create_task(self.api_server()))
        try:
            # uvicorn swallows Cancellation requests >>>:((((
            # https://github.com/encode/uvicorn/issues/1579
            # https://docs.python.org/3/library/asyncio-task.html#task-cancellation
            _, pending_tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            logger.warning("User-requested shutdown.")
            for task in pending_tasks:
                task.cancel()
            await asyncio.wait(pending_tasks)

        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.warning("User-requested shutdown.")

    @staticmethod
    def get_job_by_name(scheduler: Scheduler, job_name: str) -> Job:
        for job in scheduler.jobs:
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
                job = self.get_job_by_name(self.scheduler, task_name)
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

    async def create_schedule(self, module_definitions: dict = None, loop=None) -> None:
        self.scheduler = Scheduler(tzinfo=pytz.utc, loop=self.get_event_loop(loop))

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
                    task_name: str = schedule_definition.get(
                        "name", f"{task_index + 1}. task in {block_name}"
                    )
                    if not schedule_definition.get("enabled", False):
                        logger.info(
                            f"Ignoring disabled task {task_name} in {block_name}"
                        )
                        continue

                    if not schedule_definition.get("schedule", False):
                        logger.critical(
                            f'Missing schedule definition for enabled task "{block_name}". '
                        )

                    schedule = Schedule(
                        definition=schedule_definition, scheduler=self.scheduler
                    )

                    logs = (
                        select(Log)
                        .where(Log.job == task_name)
                        .order_by(Log.triggered_date.desc())
                        .limit(1)
                    )
                    last_run: Log | None = next(session.scalars(logs), None)
                    resume_parameters: dict = {}
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
                                    "start": next_run_local
                                             - datetime.timedelta(
                                        seconds=schedule.get_timing().total_seconds()
                                    )
                                }

                                logger.warning(
                                    f"Job {task_name} was scheduled to run every {schedule.get_timing()}, resuming using {last_running_time_local}, tz={last_run.timezone} as reference time."
                                    f" Next run at {next_run_local}, tz={last_run.timezone}"
                                )

                    schedule.job_type(
                        timing=schedule.get_timing(),
                        handle=self.create_task_from_schedule(
                            block_module, task_name, schedule
                        ),
                        alias=task_name,
                        tags=schedule_definition.get("tags"),
                        **resume_parameters,
                    )

    def get_event_loop(self, loop):
        self.loop = loop or asyncio.get_running_loop()

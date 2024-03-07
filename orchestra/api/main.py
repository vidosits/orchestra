import logging
import os
import signal

from fastapi import FastAPI
from rich.logging import RichHandler

from orchestra.api.routers import jobs, tasks, tags
from orchestra.core import instance

logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, tracebacks_show_locals=True)]
)

logger = logging.getLogger("orchestra.api")

app = FastAPI(title="Orchestra")
app.include_router(jobs.router)
app.include_router(tasks.router)
app.include_router(tags.router)

if instance is None:
    logger.error("Orchestra is not started, can't start API server")
    os.kill(os.getpid(), signal.SIGTERM)

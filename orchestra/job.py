from scheduler.asyncio.job import Job


class StatefulJob:
    def __init__(self, job: Job, is_paused: bool):
        self.job = job
        self.is_paused = is_paused

import logging
from datetime import datetime, timezone

from celery import states
from celery.backends.database import DatabaseBackend

from orchestra.models import TimingAwareTask

logger = logging.getLogger("orchestra.backend")


class OrchestraBackend(DatabaseBackend):

    def __init__(self, dburi=None, engine_options=None, url=None, **kwargs):
        super().__init__(dburi, engine_options, url, **kwargs)

        conf = self.app.conf

        if self.extended_result:
            self.task_cls = TimingAwareTask

        schemas = conf.database_table_schemas or {}
        tablenames = conf.database_table_names or {}
        self.task_cls.configure(
            schema=schemas.get('task'),
            name=tablenames.get('task'))
        self.taskset_cls.configure(
            schema=schemas.get('group'),
            name=tablenames.get('group'))

    def _update_result(self, task, result, state, traceback=None, request=None):

        meta = self._get_result_meta(result=result, state=state, traceback=traceback, request=request, format_date=False, encode=True)

        # Exclude the primary key id, task_id, date_created and date_started columns
        # as we should not set it None
        columns = [column.name for column in self.task_cls.__table__.columns if column.name not in {"id", "task_id", "date_created", "date_started"}]

        status = meta.get("status")
        match status:
            case states.STARTED:
                setattr(task, "date_started", datetime.now(timezone.utc))
                logger.debug(f"Started work on {task.id}")

        # Iterate through the columns name of the table
        # to set the value from meta.
        # If the value is not present in meta, set None
        for column in columns:
            value = meta.get(column)
            setattr(task, column, value)

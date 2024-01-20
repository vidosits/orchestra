from datetime import datetime

import sqlalchemy as sa
from celery.backends.database.models import TaskExtended, ResultModelBase
from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column


class TimingAwareTask(TaskExtended):
    """Task result/status."""
    __tablename__ = 'celery_taskmeta'
    __table_args__ = {'sqlite_autoincrement': True, 'extend_existing': True}

    date_created = sa.Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    date_started = sa.Column(TIMESTAMP(timezone=True), nullable=True)

    def to_dict(self):
        task_dict = super().to_dict()
        task_dict.update({
            'date_created': self.date_created,
            'date_started': self.date_started,
        })
        return task_dict


class Log(ResultModelBase):
    __tablename__ = "orchestra_scheduler_logs"
    __table_args__ = {'sqlite_autoincrement': True}

    id: Mapped[int] = mapped_column(primary_key=True)
    job: Mapped[str]
    module: Mapped[str]
    task: Mapped[str]
    frequency: Mapped[str]
    timezone: Mapped[str | None]
    triggered_date: Mapped[datetime] = sa.Column(TIMESTAMP(timezone=True))

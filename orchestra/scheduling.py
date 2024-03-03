import logging
import re
import sys
from datetime import datetime, time, timedelta
from typing import Callable, Optional

import pytz.exceptions
import scheduler.trigger as weekdays
from pytz import timezone
from scheduler import Scheduler

from orchestra.formatting import pretty_print_block, transform_timing_to_schedule

logger = logging.getLogger("orchestra.scheduling")


class Schedule:
    job_type: Callable
    trigger: Optional[Callable]
    timing: time | datetime | timedelta
    timezone: pytz.tzinfo.BaseTzInfo

    def __init__(self, definition: dict, scheduler: Scheduler):
        self.definition = definition
        self.scheduler = scheduler
        if "schedule" not in definition:
            logger.critical(
                f"Missing schedule definition in:\n\n{pretty_print_block(self.definition)}"
            )
        if "timing" not in definition["schedule"]:
            logger.critical(
                f"Missing timing definition in:\n\n{pretty_print_block(self.definition)}"
            )

        self.job_type, self.trigger, self.timing = self.parse_timing()
        self.timezone = self.maybe_parse_timezone()

    @staticmethod
    def clean_timing(timing_string: str) -> str:
        """
        - Remove all "on"-s with "at"-s
        - Remove all commas
        - Replace all "next" with "once at"
        - Remove any "and"-s
        - Replace all "X hour Y minutes Z seconds" with "XX:YY:ZZ"
        - Replace all "day at XX" with "00:00:XX"
        - Replace all "XX:YY" with "00:XX:YY
        - Remove all double spaces
        """

        timing_string = timing_string.replace(" on ", " at ")
        timing_string = timing_string.replace(",", "")
        timing_string = timing_string.replace("next", "once at")
        timing_string = timing_string.replace("and", "")

        seconds = "00"
        minutes = "00"
        hours = "00"
        normalize_time = False

        # remove any mention of seconds and remember the value
        if (match := re.search(r"(\d+)\s?seconds?", timing_string)) is not None:
            seconds = match.group(1)
            timing_string = timing_string.replace(match.group(0), "")
            normalize_time = True

        # remove any mention of minutes and remember the value
        if (match := re.search(r"(\d+)\s?minutes?", timing_string)) is not None:
            minutes = match.group(1)
            timing_string = timing_string.replace(match.group(0), "")
            normalize_time = True

        # remove any mention of hours and remember the value
        if (match := re.search(r"(\d+)\s?hours?", timing_string)) is not None:
            hours = match.group(1)
            timing_string = timing_string.replace(match.group(0), "")
            normalize_time = True

        # convert every hour|day [at] X:Y to pre or post zero-padded XX:YY:ZZ format, based on if timing is hourly or daily respectively
        if (
            match := re.search(
                r"\b(day|hour)(?:\Wat\W)?(\d+):(\d{2})(?!:)", timing_string
            )
        ) is not None:
            normalize_time = True
            timing_string = timing_string.replace(match.group(0), match.group(1))
            match match.group(1):
                case "day":
                    hours = match.group(2)
                    minutes = match.group(3)
                    seconds = "00"
                case "hour":
                    hours = "00"
                    minutes = match.group(2)
                    seconds = match.group(3)

        # normalize bare timestamps like XX:YY to XX:YY:ZZ format
        if (
            match := re.search(r"^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$", timing_string)
        ) is not None:
            normalize_time = True
            hours = match.group(1)
            minutes = match.group(2)
            seconds = match.group(3) if match.group(3) else "00"
            timing_string = timing_string.replace(match.group(0), "once at")

        # normalize bare timestamps like YYYY-mm-dd XX:YY to YYYY-mm-dd XX:YY:ZZ format
        if (
            match := re.search(
                r"^(\d{4}-\d{2}-\d{2}) (\d{1,2}):(\d{1,2})$", timing_string
            )
        ) is not None:
            normalize_time = True
            hours = match.group(2)
            minutes = match.group(3)
            seconds = "00"
            timing_string = timing_string.replace(match.group(0), match.group(1))

        if normalize_time:
            timing_string += f" {int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        while timing_string != timing_string.replace("  ", " "):
            timing_string = timing_string.replace("  ", " ")

        return timing_string

    def parse_timing(self):
        """
        every 00:00:01
        every 48:00:00
        every 31 minutes and 12 seconds
        every Monday at 03:15:00
        next Tuesday at 13:11:00
        once, Wednesday at 01:15:00
        once on Thursday, 00:31:41
        every day at 09:15:14
        every minute at 15 seconds
        every hour at 05:00
        every day on 9:03
        every day at 9:15
        every day at 13 hours
        once in 13:11:52
        11:23:00
        9:13
        2023-04-01 09:13
        2023-04-01 09:13:11
        next 23:00:00
        once in 5 hours 2 minutes and 15 seconds
        once in 1 hour and 10 minutes
        once in 2 minutes and 10 seconds
        once in 17 seconds
        """

        clean_timing = self.clean_timing(self.definition["schedule"]["timing"])

        # there is a complete datetime in the timing, those can only happen once, without a trigger
        if (
            match := re.search(
                r"(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})",
                clean_timing,
            )
        ) is not None:
            return (
                self.scheduler.once,
                None,
                self.get_localized_datetime(
                    year=int(match.group(1)),
                    month=int(match.group(2)),
                    day=int(match.group(3)),
                    hour=int(match.group(4)),
                    minute=int(match.group(5)),
                    second=int(match.group(6)),
                ),
            )

        # this is just a bare time, we schedule the job to run once at the next time specified
        if (
            match := re.search(r"^(?:once at )?(\d{2}):(\d{2}):(\d{2})", clean_timing)
        ) is not None:
            return (
                self.scheduler.once,
                None,
                self.get_localized_time(
                    hour=int(match.group(1)),
                    minute=int(match.group(2)),
                    second=int(match.group(3)),
                ),
            )

        # the matching schedules run once at a specific time, there may be an optional trigger/weekday
        if (
            match := re.search(
                r"^once\s(at|in)?\s?(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?\s?(?:at)?\s?(\d{2}):(\d{2}):(\d{2})",
                clean_timing,
            )
        ) is not None:
            weekday = match.group(2)
            trigger = getattr(weekdays, weekday) if weekday else None

            # if there's an "in" word after once, then it's a timedelta
            if match.group(1) == "in":
                return (
                    self.scheduler.once,
                    trigger,
                    timedelta(
                        hours=int(match.group(3)),
                        minutes=int(match.group(4)),
                        seconds=int(match.group(5)),
                    ),
                )
            else:
                return (
                    self.scheduler.once,
                    trigger,
                    self.get_localized_time(
                        hour=int(match.group(3)),
                        minute=int(match.group(4)),
                        second=int(match.group(5)),
                    ),
                )

        if (
            match := re.search(
                r"^every\s(?:(day|minute|hour)|(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday))?\s?(?:at)?\s?(\d{2}):(\d{2}):(\d{2})",
                clean_timing,
            )
        ) is not None:
            timing = match.group(1)
            weekday = match.group(2)

            # if there's a timing (every day/hour/minute) or a weekday (Monday, Tuesday, etc.) specified the time can only be a specific time
            if timing or weekday:
                trigger = getattr(weekdays, weekday) if weekday else None
                schedule = (
                    getattr(self.scheduler, transform_timing_to_schedule(timing))
                    if timing
                    else self.scheduler.weekly
                )

                return (
                    schedule,
                    trigger,
                    self.get_localized_time(
                        hour=int(match.group(3)),
                        minute=int(match.group(4)),
                        second=int(match.group(5)),
                    ),
                )

            # at this point the timing can only be an interval, e.g.: every 00:00:01, meaning every 1 second, etc.
            return (
                self.scheduler.cyclic,
                None,
                timedelta(
                    hours=int(match.group(3)),
                    minutes=int(match.group(4)),
                    seconds=int(match.group(5)),
                ),
            )

        logger.critical(
            f"Couldn't parse timing definition in:\n\n{pretty_print_block(self.definition)}"
        )

    def maybe_parse_timezone(self) -> pytz.tzinfo.BaseTzInfo | None:
        tz = self.definition.get("schedule").get("timezone")
        if tz is None:
            return pytz.utc
        try:
            return timezone(tz)
        except pytz.exceptions.UnknownTimeZoneError:
            logger.critical(
                f"Unable to parse timezone {tz} in:\n\n{pretty_print_block(self.definition)}"
            )

    def get_timing(self):
        if self.trigger:
            return self.trigger(self.timing)
        else:
            return self.timing

    def get_localized_datetime(
        self, year: int, month: int, day: int, hour: int, minute: int, second: int
    ) -> datetime:
        tz = self.maybe_parse_timezone()
        naive_datetime = datetime(
            year=year, month=month, day=day, hour=hour, minute=minute, second=second
        )
        return tz.localize(naive_datetime)

    def get_localized_time(self, hour: int, minute: int, second: int) -> time:
        tz = self.maybe_parse_timezone()
        return time(hour=hour, minute=minute, second=second, tzinfo=tz)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from .custom_executor import CustomAsyncioExecutor
import asyncio
import logging
import datetime
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR

logger = logging.getLogger(__name__)


class ScheduleTrigger:
    def __init__(self, config):
        jobstores = {
            "default": SQLAlchemyJobStore(url="sqlite:////app/data/jobs.sqlite"),
        }
        executors = {"default": CustomAsyncioExecutor()}
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores, executors=executors, timezone="UTC"
        )
        self.deffered_tasks = []
        self.config = config

    def my_listener(self,event):
        pass
        # logger.info(f"<><><><><><><> {event.job_id}")
        # job = self.scheduler.get_job(event.job_id)

        # logger.info(f"============== {job}")
        # if event.exception:
        #     logger.info(f"The job crashed {event.exception}    {event}")

    async def run(self):

        self.scheduler.start()

        all_jobs = self.scheduler.get_jobs()
        for job in all_jobs:
            logger.info(f"Loaded from Jobstore: {job.id}")

        for args in self.deffered_tasks:
            self.schedule_task(*args)

        all_jobs = self.scheduler.get_jobs()
        for job in all_jobs:
            job.modify(kwargs={**job.kwargs, "config": self.config})

        self.scheduler.add_listener(self.my_listener, EVENT_JOB_ERROR)

        while True:
            await asyncio.sleep(3600)
        # TODO use event listener to monitor

    def schedule_soon(self, func, args, kwargs):
        logger.info(f"ARGS {args}")
        job = self.scheduler.add_job(
            func, coalesce=False, args=args, kwargs={**kwargs, "config": self.config}
        )
        return job != None

    def schedule_task(self, func, schedule_spec, grace_time_seconds):
        job_id = func.__name__
        if self.scheduler.get_job(job_id) is None:
            match schedule_spec:
                case dict():
                    base_schedule = {
                        "year": None,
                        "month": None,
                        "day": None,
                        "week": None,
                        "day_of_week": None,
                        "hour": None,
                        "minute": None,
                        "second": None,
                    }
                    schedule = {}
                    for k, v in base_schedule.items():
                        if k in schedule_spec:
                            schedule[k] = schedule_spec[k]
                        else:
                            schedule[k] = v

                    trigger = CronTrigger(*schedule)
                case str():
                    trigger = CronTrigger.from_crontab(schedule_spec)

            self.scheduler.add_job(
                func,
                id=job_id,
                trigger=trigger,
                coalesce=False,
                replace_existing=True,
                misfire_grace_time=grace_time_seconds,
                kwargs={"last_run": datetime.datetime.now(), "config": self.config}
            )

    def deffered_schedule_task(self, *args):
        self.deffered_tasks.append(args)

    # this is a decorator
    def task(self, schedule_spec: dict[str, str] | str, grace_time_seconds=None):
        def inner(func):
            self.deffered_schedule_task(func, schedule_spec, grace_time_seconds)
            return func

        return inner

import datetime
from apscheduler.util import iscoroutinefunction_partial
from apscheduler.executors.base import BaseExecutor, run_job
import sys
import logging
from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)
import asyncio
import traceback
from apscheduler.executors.asyncio import AsyncIOExecutor

RATE_LIMIT_SECONDS = 1  # minimum seconds between job starts
# to track last run time across all jobs using this executor

last_run_for_rate_limit = datetime.datetime.now(
    datetime.timezone.utc
) - datetime.timedelta(
    seconds=2 * RATE_LIMIT_SECONDS
)  # to allow immediate first run


# Copied from apscheduler.executors.base.run_coroutine_job adding execution_time=run_time to function call
async def run_coroutine_job(job, jobstore_alias, run_times, logger_name):
    """Coroutine version of run_job()."""
    events = []
    logger = logging.getLogger(logger_name)
    last_run = None
    global last_run_for_rate_limit

    for run_time in run_times:
        # See if the job missed its run time window, and handle possible misfires accordingly
        if job.misfire_grace_time is not None:
            difference = datetime.datetime.now(datetime.timezone.utc) - run_time
            grace_time = datetime.timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                events.append(
                    JobExecutionEvent(
                        EVENT_JOB_MISSED, job.id, jobstore_alias, run_time
                    )
                )
                logger.warning('Run time of job "%s" was missed by %s', job, difference)
                continue
        logger.info(
            f'[{datetime.datetime.now(datetime.timezone.utc)}] Running job "{job}" (scheduled at {run_time})'
        )

        try:
            ## MODIFIED
            if last_run is not None and last_run > job.kwargs["last_run"]:
                kwargs = {
                    **job.kwargs,
                    "last_run": last_run,
                    "execution_time": run_time,
                }
            else:
                kwargs = {
                    **job.kwargs,
                    "execution_time": run_time,
                }

            time_since_last_run = (
                datetime.datetime.now(datetime.timezone.utc) - last_run_for_rate_limit
            )
            if time_since_last_run < datetime.timedelta(seconds=RATE_LIMIT_SECONDS):
                delay = RATE_LIMIT_SECONDS - time_since_last_run.total_seconds()
                await asyncio.sleep(delay)
            last_run_for_rate_limit = datetime.datetime.now(datetime.timezone.utc)

            retval = await job.func(
                *job.args,
                **kwargs,
            )

            # needed with the above to avoid case where not set on startup
            last_run = run_time
            job.modify(kwargs={**job.kwargs, "last_run": last_run})
        except BaseException:
            exc, tb = sys.exc_info()[1:]
            formatted_tb = "".join(traceback.format_tb(tb))
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_ERROR,
                    job.id,
                    jobstore_alias,
                    run_time,
                    exception=exc,
                    traceback=formatted_tb,
                )
            )
            logger.exception('Job "%s" raised an exception', job)
            traceback.clear_frames(tb)
        else:
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_EXECUTED, job.id, jobstore_alias, run_time, retval=retval
                )
            )
            logger.info('Job "%s" executed successfully', job)

    return events


class CustomAsyncioExecutor(AsyncIOExecutor):
    # Copied from base AsyncIOExecutor in order to replace original run_coroutine_job with our version above
    def _do_submit_job(self, job, run_times):
        def callback(f):
            self._pending_futures.discard(f)
            try:
                events = f.result()
            except BaseException:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        if iscoroutinefunction_partial(job.func):
            coro = run_coroutine_job(
                job, job._jobstore_alias, run_times, self._logger.name
            )
            f = self._eventloop.create_task(coro)
        else:
            f = self._eventloop.run_in_executor(
                None, run_job, job, job._jobstore_alias, run_times, self._logger.name
            )

        f.add_done_callback(callback)
        self._pending_futures.add(f)

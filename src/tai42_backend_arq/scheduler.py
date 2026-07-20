"""Redis-hash scheduler for recurring tool runs.

Each schedule lives in one hash (``arq:schedule:{name}``) holding its target,
args/kwargs, canonical schedule dict, compact ``cron_or_interval`` form, enabled
flag, and runtime state (pending job id, last scheduled timestamp, abort
marker). The self-rescheduling ``task_scheduler`` worker function enqueues the
target when due and defers its own replacement; every state change of a
schedule hash — a ``safe_schedule_transition``, an enabled-flag write, a
delete — runs under the per-schedule ``schedule_lock``, so a delete can never
interleave a transition's read-abort-write sequence and a flag write can never
recreate a hash a concurrent delete just removed; the
``recover_stalled_schedules`` startup watchdog restarts schedules whose pending
job is lost or finished without rescheduling.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
from arq.jobs import Job, JobStatus
from croniter import croniter

from tai42_backend_arq.pool import RedisPoolManager
from tai42_backend_arq.records import parse_cron_or_interval
from tai42_backend_arq.settings import TaskFailedError, arq_settings, job_deserializer

logger = logging.getLogger(__name__)


async def wait_job_result(job: Job, timeout: float | None = None) -> Any:
    """``Job.result`` with a replayed stored abort surfaced as ``TaskFailedError``.

    A failed job's stored outcome replays out of ``Job.result`` as its revived
    exception. For an aborted job that is an ``asyncio.CancelledError``, which
    must never escape a coroutine that was not itself cancelled — the caller's
    runtime would read it as a cancellation of the caller. A replayed abort
    re-raises here as ``TaskFailedError`` carrying the stored detail; a pending
    cancellation of the CALLING task re-raises as-is."""
    try:
        return await job.result(timeout=timeout)
    except asyncio.CancelledError as exc:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise
        raise TaskFailedError("CancelledError", str(exc) or repr(exc), None) from exc


async def abort_job(job: Job, timeout: float) -> bool:
    """Call ``Job.abort`` without ever swallowing this task's own cancellation.

    ``Job.abort`` treats every ``CancelledError`` raised while it polls the
    result key as the job's replayed cancellation outcome and returns ``True``
    — including a cancellation of the task CALLING it, which it catches and
    swallows. A cancellation of the current task must propagate, never read as
    a confirmed abort, so a pending one re-raises here.
    """
    aborted = await job.abort(timeout=timeout)
    task = asyncio.current_task()
    if task is not None and task.cancelling():
        raise asyncio.CancelledError
    return aborted


async def request_job_abort(job: Job) -> bool:
    """Request cancellation of ``job`` without blocking on fleet liveness.

    Returns ``True`` when the job is guaranteed not to run: the cancellation is
    confirmed, or the job already finished or no longer exists — there is
    nothing pending to cancel, and calling ``Job.abort`` on a finished job
    would replay its stored outcome (a stored abort replays as a confirmed
    cancellation; a stored failure replays as its revived exception as if the
    abort request had failed). A job that finishes on its own while the abort
    request is in flight counts the same way: ``Job.abort`` polls the result
    key while it waits, so such a job replays its stored outcome too — a
    failed job's replayed exception, or a succeeded/vanished job's plain
    ``False`` return — and both replay shapes are translated back into
    ``True`` here via a status re-check, never
    surfaced as an abort failure. Returns ``False`` when the request is
    recorded but confirmation is still pending — the worker cancels the job at
    pick-up (it runs with ``allow_abort_jobs``), so a pending confirmation is
    the expected asynchronous outcome, logged and reported, not an error. Every
    other failure (connection errors, undecodable job payloads) propagates.
    """
    status = await job.status()
    if status in (JobStatus.not_found, JobStatus.complete):
        return True
    try:
        if await abort_job(job, timeout=0):
            return True
    except TimeoutError:
        logger.info("abort of job %s requested; cancellation will be confirmed by the worker", job.job_id)
        return False
    except Exception:
        if await job.status() in (JobStatus.not_found, JobStatus.complete):
            # The job finished (or vanished) between the pre-check and the
            # abort's result poll, so the poll replayed its stored outcome —
            # an exception derived from the job's stored failure, not an abort
            # failure. Finished or gone is guaranteed not to run: the success
            # contract.
            return True
        raise
    if await job.status() in (JobStatus.not_found, JobStatus.complete):
        # ``Job.abort`` returns False (rather than raising) when its result
        # poll replayed a stored SUCCESS outcome, or when the job left the
        # queue with no retained result. Finished or gone is guaranteed not
        # to run: the success contract.
        return True
    logger.info("abort of job %s requested; cancellation will be confirmed by the worker", job.job_id)
    return False


# The complete durable definition of a schedule as stored in its hash. A
# transition may create a missing hash only when it writes all of these fields
# in one go — anything less would leave a hash with no target or schedule that
# can neither run nor be exported.
_SCHEDULE_DEFINITION_FIELDS = frozenset({"target", "args", "kwargs", "schedule", "cron_or_interval", "enabled"})


def schedule_lock(redis: Any, schedule_name: str) -> Any:
    """The per-schedule mutation lock.

    Every state change of a schedule hash — a ``safe_schedule_transition``, an
    enabled-flag write, a delete — acquires this lock, so an existence check
    made under it stays true for the write that follows: a delete cannot
    interleave a transition's read-abort-write sequence, and a flag write
    cannot recreate a hash a concurrent delete just removed.
    """
    return redis.lock(arq_settings().arq_schedule_lock_key(schedule_name), timeout=5, blocking_timeout=20)


async def safe_schedule_transition(
    redis: Any,
    schedule_name: str,
    defer_by: float,
    last_scheduled_ts: float,
    mapping_updates: dict[str, Any] | None = None,
    enforce_job_id: str | None = None,
) -> Any:
    """Atomically replace a schedule's pending job and update its hash.

    ``enforce_job_id`` makes the transition conditional: it proceeds only while
    the hash still points at that job (the self-reschedule path); a mismatch
    means an external update preempted this transition and it is skipped.
    Without ``enforce_job_id`` (create/update/import paths) any pending job is
    aborted before its replacement is enqueued.

    A transition never resurrects a deleted schedule: when the hash is gone and
    ``mapping_updates`` does not carry the full durable definition (only the
    create/import paths do), a concurrent delete removed the schedule after the
    caller last saw it — the transition is skipped and returns ``None``, since
    writing anyway would recreate the schedule as a partial hash.
    """
    settings = arq_settings()
    key = settings.arq_schedule_key(schedule_name)

    async with schedule_lock(redis, schedule_name):
        current_data = await redis.hgetall(key)
        current_job_id = current_data.get(b"job_id", b"").decode()

        if not current_data and not (mapping_updates and _SCHEDULE_DEFINITION_FIELDS.issubset(mapping_updates)):
            logger.info("Schedule '%s' transition skipped: schedule was deleted.", schedule_name)
            return None

        if enforce_job_id and current_job_id != enforce_job_id:
            logger.info("Schedule '%s' transition skipped: preempted by external update.", schedule_name)
            return None

        if not enforce_job_id and current_job_id:
            # Request abort of the job being replaced before enqueueing its
            # replacement. A failed abort request must surface: swallowing it
            # would leave the old job live alongside the new one (a duplicate/
            # dangling scheduled run), so it propagates and stops the transition
            # before anything is enqueued or written.
            await request_job_abort(Job(current_job_id, redis=redis, _deserializer=job_deserializer))

        next_job_id = uuid.uuid4().hex
        new_mapping: dict[str, Any] = {
            "job_id": next_job_id,
            "last_scheduled_ts": str(last_scheduled_ts),
        }
        if mapping_updates:
            new_mapping.update(mapping_updates)

        # The hash names the replacement (a pre-assigned id) BEFORE the job is
        # enqueued under that id, so a worker can never pick the job up while
        # the hash still points at its predecessor — ``task_scheduler`` refuses
        # to run when it is not the recorded pending job. A crash between the
        # two writes leaves the hash pointing at a not-found job, which the
        # startup watchdog recovers.
        await redis.hset(key, mapping=new_mapping)
        next_job = await redis.enqueue_job("task_scheduler", schedule_name, _job_id=next_job_id, _defer_by=defer_by)
        if next_job is None:
            raise RuntimeError(f"Schedule '{schedule_name}': job id {next_job_id} already exists; nothing enqueued")
        return next_job


async def abort_schedule_task(key: str) -> None:
    """Abort the pending job of the schedule hash at ``key``.

    The hash's ``aborted`` marker is set to the pending job id first, so even a
    job whose cancellation is never processed exits without effect when it
    fires. Failures propagate — a swallowed abort would leave a live job behind
    a schedule the caller believes is gone.
    """
    arq_redis: Any = await RedisPoolManager.get()

    if await arq_redis.exists(key):
        data = await arq_redis.hgetall(key)
        prev_job_id = data.get(b"job_id", b"").decode()
        if prev_job_id:
            await arq_redis.hset(key, "aborted", prev_job_id)
            await request_job_abort(Job(prev_job_id, redis=arq_redis, _deserializer=job_deserializer))


async def recover_stalled_schedules(ctx: dict[str, Any]) -> None:
    """Startup watchdog: restart enabled schedules whose pending job is broken.

    Scans every schedule hash; an enabled schedule whose recorded job id is
    missing, not found, or complete (finished but failed to reschedule itself)
    is restarted under a short recovery lock so concurrent worker startups
    recover it once. A failure on one schedule is logged at ERROR and does not
    stop recovery of the others.
    """
    redis = ctx["redis"]
    settings = arq_settings()
    logger.info("Watchdog: checking for stalled schedules...")

    schedule_keys = [key async for key in redis.scan_iter(match=settings.arq_schedule_pattern.encode())]

    recovered_count = 0

    for key in schedule_keys:
        try:
            name = key.decode().split(":")[-1]
            lock_key = settings.arq_schedule_recovery_lock_key(name)
            data = await redis.hgetall(key)

            enabled = data.get(b"enabled", b"false").decode("utf-8") == "true"
            if not enabled:
                continue

            job_id = data.get(b"job_id", b"").decode("utf-8")
            should_recover = False

            if not job_id:
                should_recover = True
            else:
                job = Job(job_id, redis=redis, _deserializer=job_deserializer)
                status = await job.status()

                # not_found = the pending job is lost; complete = it ran but its
                # self-reschedule never landed. Both leave the schedule dead.
                if status in (JobStatus.not_found, JobStatus.complete):
                    should_recover = True

            if should_recover and await redis.set(lock_key, "locked", nx=True, ex=10):
                logger.warning("Watchdog: schedule '%s' is stalled. Restarting.", name)
                restarted = await safe_schedule_transition(
                    redis,
                    name,
                    defer_by=0,
                    last_scheduled_ts=datetime.now(UTC).timestamp(),
                    enforce_job_id=None,
                )
                # None = the schedule was deleted between the scan and the
                # transition; there is nothing left to restart.
                if restarted is not None:
                    recovered_count += 1

        except Exception:
            # Loud, explicit per-schedule failure path: one broken schedule must
            # not stop the watchdog from recovering the rest.
            logger.error("Watchdog error checking schedule %s", key, exc_info=True)

    logger.info("Watchdog: recovery complete. Restarted %d schedules.", recovered_count)


async def task_scheduler(ctx: dict[str, Any], schedule_name: str) -> Any:
    """Self-rescheduling worker function driving one schedule.

    When due it enqueues the schedule's target (if enabled), waits for its
    result, and — win or lose — defers its own replacement via
    ``safe_schedule_transition`` conditioned on this job still being the
    recorded pending job. It runs only while it IS that recorded pending job:
    a stale invocation (an arq retry after this job's own transition already
    ran, or a replaced job whose abort request was never processed) exits
    without running the target, so one schedule never fires twice.
    """
    settings = arq_settings()
    key = settings.arq_schedule_key(schedule_name)

    # Fast exit if the schedule was deleted while this job was pending.
    if not await ctx["redis"].exists(key):
        return None

    data = await ctx["redis"].hgetall(key)

    # An abort marker naming this job means it was replaced/deleted: no run,
    # no reschedule.
    if not data or data.get(b"aborted", b"").decode("utf-8") == ctx["job_id"]:
        return None

    # Only the recorded pending job drives the schedule; anything else is a
    # stale duplicate whose slot another job already owns.
    if data.get(b"job_id", b"").decode("utf-8") != ctx["job_id"]:
        logger.info("Schedule '%s': job %s is no longer the pending job; skipping.", schedule_name, ctx["job_id"])
        return None

    enabled = data.get(b"enabled", b"false").decode("utf-8") == "true"
    target = data.get(b"target", b"").decode("utf-8")

    args = orjson.loads(data.get(b"args", b"[]"))
    kwargs = orjson.loads(data.get(b"kwargs", b"{}"))

    cron_or_interval = parse_cron_or_interval(data.get(b"cron_or_interval", b"").decode("utf-8"))

    last_scheduled_ts_str = data.get(b"last_scheduled_ts", b"0").decode("utf-8")
    try:
        last_scheduled_ts = float(last_scheduled_ts_str)
    except ValueError:
        last_scheduled_ts = 0.0

    now = datetime.now(UTC)
    now_ts = now.timestamp()

    base_time = now if last_scheduled_ts == 0 else datetime.fromtimestamp(last_scheduled_ts, tz=UTC)

    if isinstance(cron_or_interval, str):
        next_time = croniter(cron_or_interval, base_time).get_next(datetime)
        if next_time.timestamp() < now_ts:
            # The computed slot is already in the past (missed runs): realign to
            # the next slot after now instead of replaying the backlog.
            next_time = croniter(cron_or_interval, now).get_next(datetime)
    else:
        # ``parse_cron_or_interval`` yields a number for anything non-crontab.
        next_ts = base_time.timestamp() + cron_or_interval
        if next_ts < (now_ts - 60):
            # More than a minute behind: realign relative to now.
            next_time = now + timedelta(seconds=cron_or_interval)
        else:
            next_time = datetime.fromtimestamp(next_ts, tz=UTC)

    next_run_ts = next_time.timestamp()
    defer_by = next_run_ts - datetime.now(UTC).timestamp()

    try:
        if enabled:
            job = await ctx["redis"].enqueue_job(target, *args, **kwargs)
            # A failed target replays its revived stored failure here, which
            # records this scheduler job as failed with that detail. An aborted
            # target must do the same — its raw CancelledError replay would
            # read to the worker as a cancellation of THIS job and trigger a
            # pointless retry — so the replayed abort surfaces as
            # ``TaskFailedError`` via ``wait_job_result``.
            return await wait_job_result(job)
    finally:
        await safe_schedule_transition(
            ctx["redis"],
            schedule_name,
            defer_by=defer_by,
            last_scheduled_ts=next_run_ts,
            enforce_job_id=ctx["job_id"],
        )

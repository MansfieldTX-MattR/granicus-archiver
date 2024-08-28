from typing import Coroutine, Any, cast
import asyncio
import datetime

import pytest
import pytest_asyncio
import aiojobs

from granicus_archiver.utils import (
    JobWaiter, JobWaiters, JobResult,
    seconds_to_time_str,
)

WaiterCoros = dict[int, Coroutine[Any, Any, int]]
WaiterJobs = dict[aiojobs.Job, int]

@pytest_asyncio.fixture
async def scheduler():
    scheduler = aiojobs.Scheduler()
    yield scheduler
    await scheduler.close()


@pytest.fixture
def waiter_coros() -> WaiterCoros:
    async def job_coro(value: int) -> int:
        return value

    return {i: job_coro(i) for i in range(5)}



@pytest_asyncio.fixture
async def waiter_jobs(scheduler, waiter_coros) -> WaiterJobs:
    jobs: dict[aiojobs.Job, int] = {}
    for i, coro in waiter_coros.items():
        job = await scheduler.spawn(coro)
        jobs[job] = i
    return jobs


def check_waiter_job(w: JobWaiters, job: aiojobs.Job):
    assert job in w
    waiter = w[job]
    assert waiter.job is job
    assert w.waiters[job] is waiter
    assert w.waiter_tasks[waiter.task] is waiter

    # ensure adding it is a no-op
    w_len = len(w)
    _waiter = w.add(job)
    assert _waiter is waiter
    assert len(w) == w_len


async def check_waiter_result(w: JobWaiters, r: JobResult, expected_result: int):
    job_result = await r.job.wait()
    assert job_result == expected_result
    assert r.exception is None
    assert r.result == expected_result
    assert r.job not in w



@pytest.mark.asyncio
async def test_waiter_hash(scheduler):
    async def dummy():
        pass

    waiters: set[JobWaiter] = set()
    jobs: set[aiojobs.Job] = set()
    waiter_map: dict[aiojobs.Job, JobWaiter] = {}

    for i in range(5):
        job = await scheduler.spawn(dummy())
        assert job not in jobs
        jobs.add(job)
        assert job in jobs

        waiter = JobWaiter(job)
        assert waiter not in waiters
        waiters.add(waiter)
        assert waiter in waiters

        assert job not in waiter_map
        waiter_map[job] = waiter
        assert job in waiter_map

    assert len(waiters) == len(jobs) == len(waiter_map) == 5

    for job in jobs.copy():
        waiter = waiter_map[job]
        assert waiter.job is job
        waiters.discard(waiter)
        jobs.discard(job)
        del waiter_map[job]

        assert waiter not in waiters
        assert job not in jobs
        assert job not in waiter_map
        await job.wait()

    assert len(waiters) == len(jobs) == len(waiter_map) == 0


@pytest.mark.asyncio
async def test_waiters(waiter_jobs):
    waiter_jobs = cast(WaiterJobs, waiter_jobs)
    w: JobWaiters[int] = JobWaiters()

    for job, i in waiter_jobs.items():
        w.add(job)
        check_waiter_job(w, job)

    assert len(w) == len(waiter_jobs)
    done, pending = await w.wait(asyncio.ALL_COMPLETED)
    for r in done:
        i = waiter_jobs[r.job]
        await check_waiter_result(w, r, i)

    assert not len(pending)
    assert not len(w)


@pytest.mark.asyncio
async def test_as_completed(waiter_jobs):
    waiter_jobs = cast(WaiterJobs, waiter_jobs)
    w: JobWaiters[int] = JobWaiters()

    for job, i in waiter_jobs.items():
        w.add(job)
        check_waiter_job(w, job)

    async for r in w.as_completed():
        i = waiter_jobs[r.job]
        await check_waiter_result(w, r, i)

    assert not len(w)


@pytest.mark.asyncio
async def test_async_for(waiter_jobs):
    waiter_jobs = cast(WaiterJobs, waiter_jobs)
    w: JobWaiters[int] = JobWaiters()

    for job, i in waiter_jobs.items():
        w.add(job)
        check_waiter_job(w, job)

    async for r in w:
        i = waiter_jobs[r.job]
        await check_waiter_result(w, r, i)

    assert not len(w)


@pytest.mark.asyncio
async def test_waiters_await(waiter_jobs):
    w: JobWaiters[int] = JobWaiters()

    for job, i in waiter_jobs.items():
        w.add(job)
        check_waiter_job(w, job)

    result_list = await w.gather()
    assert set(result_list) == set(waiter_jobs.values())

    assert not len(w)


@pytest.mark.asyncio
async def test_waiter_await(waiter_jobs):
    waiter_jobs = cast(WaiterJobs, waiter_jobs)
    w: JobWaiters[int] = JobWaiters()

    for job, i in waiter_jobs.items():
        w.add(job)
        check_waiter_job(w, job)

    for waiter in w:
        result = await waiter
        expected = waiter_jobs[waiter.job]
        assert result == expected
        w.discard(waiter)

    assert not len(w)


@pytest.mark.asyncio
async def test_spawn(scheduler, waiter_coros):
    w: JobWaiters[int] = JobWaiters(scheduler=scheduler)
    jobs: dict[aiojobs.Job, int] = {}

    for i, coro in waiter_coros.items():
        job = await w.spawn(coro)
        check_waiter_job(w, job)
        jobs[job] = i

    async for r in w:
        i = jobs[r.job]
        await check_waiter_result(w, r, i)

    assert not len(w)


def test_seconds_to_time_str():
    for h in range(23):
        for m in range(59):
            for s in range(59):
                td = datetime.timedelta(hours=h, minutes=m, seconds=s)
                time_str = seconds_to_time_str(int(td.total_seconds()))
                assert time_str == f'{h:02d}:{m:02d}:{s:02d}'

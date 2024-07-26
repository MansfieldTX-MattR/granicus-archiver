from __future__ import annotations
from typing import (
    TypeVar, Generic, Coroutine, Iterator, Generator, Sized, Iterable,
    Container, Awaitable, AsyncIterable, AsyncGenerator, Literal, Any,
)
from pathlib import Path

import asyncio
import aiojobs

T = TypeVar('T')

RETURN_WHEN = Literal['FIRST_COMPLETED', 'FIRST_EXCEPTION', 'ALL_COMPLETED']

class NotSetType: ...

NotSet = NotSetType()



class JobWaiter(Generic[T], Awaitable[T]):
    """Wrapper for :class:`aiojobs.Job` to wait for its result

    Instances of this class are :term:`awaitable` and :term:`hashable`
    """
    job: aiojobs.Job[T]
    """The :class:`aiojobs.Job` instance"""

    task: asyncio.Task[T]
    """A :class:`asyncio.Task` to :keyword:`await` the :attr:`job's <job>`
    :meth:`~aiojobs.Job.wait` method
    """

    __slots__ = ('job', 'task')

    def __init__(self, job: aiojobs.Job[T]) -> None:
        self.job = job

    @property
    def id(self):
        return (self.__class__.__name__, self.job)

    def _maybe_cancel(self) -> None:
        if self.task.done() or self.task.cancelled() or self.task.cancelling():
            return
        self.task.cancel()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return other.id == self.id

    def __hash__(self):
        return hash(self.id)

    def __await__(self) -> Generator[Any, None, T]:
        return self.task.__await__()


class JobResult(Generic[T]):
    """A completed :class:`aiojobs.Job`
    """

    job: aiojobs.Job[T]
    """The job instance"""

    exception: BaseException|None
    """An exception, if one was encountered by the job"""

    __slots__ = ('job', '_result', 'exception')
    def __init__(
        self,
        job: aiojobs.Job[T],
        result: T|NotSetType,
        exception: BaseException|None = None
    ) -> None:
        self.job = job
        self._result = result
        self.exception = exception

    @property
    def result(self) -> T:
        if self._result is NotSet or isinstance(self._result, NotSetType):
            raise asyncio.InvalidStateError()
        return self._result



class JobWaiters(
    Sized, Iterable[JobWaiter[T]], Container[JobWaiter[T]|aiojobs.Job[T]],
    Awaitable[list[T]], AsyncIterable[JobResult[T]]
):
    """Container for :class:`aiojobs.Job` instances to :keyword:`await` their
    results

    Jobs may be awaited using the :meth:`wait` and :meth:`gather` methods
    as well as async iteration using :keyword:`async for`
    """

    jobs: set[JobWaiter[T]]
    """All currently tracked jobs wrapped in :class:`JobWaiter` instances"""

    waiters: dict[aiojobs.Job[T], JobWaiter[T]]
    """Mapping of :class:`aiojobs.Job` instances to their :class:`JobWaiter`"""

    waiter_tasks: dict[asyncio.Task[T], JobWaiter[T]]
    """Mapping of the :attr:`JobWaiter.task` for each :class:`JobWaiter`"""

    scheduler: aiojobs.Scheduler|None
    """Optional :class:`aiojobs.Scheduler` instance"""

    def __init__(self, scheduler: aiojobs.Scheduler|None = None) -> None:
        self.jobs = set()
        self.waiters = {}
        self.waiter_tasks = {}
        self.scheduler = scheduler

    def add(self, job: aiojobs.Job[T]) -> JobWaiter:
        """Add an existing :class:`aiojobs.Job` instance

        If the job is already tracked, this becomes a no-op
        """
        if job in self:
            return self[job]
        waiter = JobWaiter(job)
        self.jobs.add(waiter)
        self.waiters[job] = waiter
        waiter.task = asyncio.create_task(job.wait())
        self.waiter_tasks[waiter.task] = waiter
        return waiter

    async def spawn(
        self,
        coro: Coroutine[object, object, T],
        name: str|None = None
    ) -> aiojobs.Job[T]:
        """Spawn a job using the :attr:`scheduler` (if it was set)

        The arguments match that of :meth:`aiojobs.Scheduler.spawn` method
        """
        if self.scheduler is None:
            raise ValueError('No scheduler set')
        job = await self.scheduler.spawn(coro, name)
        self.add(job)
        return job

    def discard(self, job_or_waiter: aiojobs.Job[T]|JobWaiter[T]) -> None:
        """Remove a :class:`aiojobs.Job` (if it is currently being tracked)
        """
        if isinstance(job_or_waiter, JobWaiter):
            job = job_or_waiter.job
        else:
            job = job_or_waiter
        if job not in self:
            return
        waiter = self.waiters[job]
        self.jobs.discard(waiter)
        waiter._maybe_cancel()
        del self.waiters[job]
        del self.waiter_tasks[waiter.task]

    def clear(self) -> None:
        """Clear all tracked jobs
        """
        for waiter in self.waiters.values():
            waiter._maybe_cancel()
        self.jobs.clear()
        self.waiters.clear()
        self.waiter_tasks.clear()

    async def wait(
        self,
        return_when: RETURN_WHEN = 'FIRST_COMPLETED'
    ) -> tuple[list[JobResult[T]], set[aiojobs.Job[T]]]:
        """Wait for the next job completion

        This method is similar to :func:`asyncio.wait`, aside from the slight
        difference in return type.

        Returns:
            (tuple): a tuple of

                done: A list of completed :class:`JobResult` instances
                pending: A set of pending :class:`aiojobs.Job` instances

        """
        _done, _pending = await asyncio.wait(self.waiter_tasks.keys(), return_when=return_when)
        pending: set[aiojobs.Job[T]] = set()
        done: list[JobResult[T]] = []
        asyncio.FIRST_COMPLETED

        for t in _done:
            waiter = self.waiter_tasks[t]
            exc = t.exception()
            result = NotSet if exc is not None else t.result()
            r = JobResult(job=waiter.job, result=result, exception=t.exception())
            done.append(r)
            self.discard(waiter)
        for t in _pending:
            waiter = self.waiter_tasks[t]
            pending.add(waiter.job)
        return done, pending

    async def as_completed(self) -> AsyncGenerator[JobResult[T]]:
        """An :term:`asynchronous generator` of completed jobs
        (wrapped as :class:`JobResult`)::

            waiter = JobWaiters()
            ...
            async for result in waiter.as_completed():
                ...


        The same could be accomplished using :keyword:`async for` on the
        instance itself::

            waiter = JobWaiters()
            ...
            async for result in waiter:
                ...


        """
        done, pending = await self.wait(return_when=asyncio.FIRST_COMPLETED)
        for job in done:
            yield job

    def __aiter__(self):
        return self.as_completed()

    def __await__(self):
        return self.gather().__await__()

    async def gather(self) -> list[T]:
        """Wait for completion of all jobs and return their results as a list

        The same could be accomplished by awaiting the instance directly::

            waiter = JobWaiters()
            ...
            results = await waiter


        """
        waiters = set(self.waiters.values())
        result = await asyncio.gather(*waiters)
        for waiter in waiters:
            self.discard(waiter)
        return result

    async def close(self) -> None:
        """Closes the :attr:`scheduler` (if set)
        """
        if self.scheduler is not None:
            return await self.scheduler.close()

    def __contains__(self, job_or_waiter: aiojobs.Job|JobWaiter):
        if isinstance(job_or_waiter, JobWaiter):
            job = job_or_waiter.job
        else:
            job = job_or_waiter
        return job in self.waiters

    def __len__(self):
        return len(self.jobs)

    def __iter__(self) -> Iterator[JobWaiter[T]]:
        yield from self.waiters.copy().values()

    def __getitem__(self, key: aiojobs.Job) -> JobWaiter[T]:
        return self.waiters[key]


def find_mount_point(p: Path) -> Path:
    p = p.resolve()
    while not p.is_mount():
        p = p.parent
    return p


def is_same_filesystem(a: Path, b: Path) -> bool:
    return find_mount_point(a) == find_mount_point(b)

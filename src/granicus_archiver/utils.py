from __future__ import annotations
from typing import (
    TypeVar, Generic, NewType, Coroutine, Iterator, Generator, Sized, Iterable,
    Container, Awaitable, AsyncIterable, AsyncGenerator, Literal, Any, overload,
)
import hashlib
from pathlib import Path
from loguru import logger
import asyncio
import aiojobs
import aiofile
from pypdf import PdfWriter

HashType = Literal['md5', 'sha1', 'sha256']
MD5Hash = NewType('MD5Hash', str)
SHA1Hash = NewType('SHA1Hash', str)
SHA256Hash = NewType('SHA256Hash', str)
HashValueT = MD5Hash|SHA1Hash|SHA256Hash
T = TypeVar('T')

RETURN_WHEN = Literal['FIRST_COMPLETED', 'FIRST_EXCEPTION', 'ALL_COMPLETED']

class NotSetType: ...

NotSet = NotSetType()


def remove_pdf_links(infile: Path, outfile: Path) -> None:
    """Remove hyperlinks from a pdf file

    Arguments:
        infile: The input PDF file
        outfile: Output filename
    """
    writer = PdfWriter(clone_from=infile)
    writer.remove_links()
    writer.write(outfile)


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

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self.job!r}>'


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

    def raise_exc(self) -> None:
        """Raise the :attr:`exception` if it exists
        """
        if self.exception is not None:
            raise self.exception



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
        """Remove a :class:`~aiojobs.Job` (if it is currently being tracked)
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
            (tuple):
                - **done**: A list of completed :class:`JobResult` instances
                - **pending**: A :class:`set` of pending :class:`aiojobs.Job` instances

        """
        if not len(self.waiter_tasks):
            return [], set()
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
        while True:
            done, pending = await self.wait(return_when=asyncio.FIRST_COMPLETED)
            for job in done:
                yield job
            if not len(pending):
                break

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
        if not len(waiters):
            return []
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


@overload
def _typed_hash_value(hash_type: Literal['md5'], value: str) -> MD5Hash: ...
@overload
def _typed_hash_value(hash_type: Literal['sha1'], value: str) -> SHA1Hash: ...
@overload
def _typed_hash_value(hash_type: Literal['sha256'], value: str) -> SHA256Hash: ...
def _typed_hash_value(hash_type: HashType, value: str) -> HashValueT:
    if hash_type == 'md5':
        return MD5Hash(value)
    elif hash_type == 'sha1':
        return SHA1Hash(value)
    elif hash_type == 'sha256':
        return SHA256Hash(value)
    raise ValueError(f'unsupported hash type: {hash_type}')

@overload
def get_file_hash(p: Path, hash_type: Literal['md5']) -> MD5Hash: ...
@overload
def get_file_hash(p: Path, hash_type: Literal['sha1']) -> SHA1Hash: ...
@overload
def get_file_hash(p: Path, hash_type: Literal['sha256']) -> SHA256Hash: ...
def get_file_hash(p: Path, hash_type: HashType) -> HashValueT:
    """Get the hash for the contents of a file

    Arguments:
        p: The file path
        hash_type: The hash type (``'md5'``, ``'sha1'``, or ``'sha256'``)

    """
    assert p.is_file()
    h = hashlib.new(hash_type)
    h.update(p.read_bytes())
    return _typed_hash_value(hash_type, h.hexdigest())


@overload
async def get_file_hash_async(p: Path, hash_type: Literal['md5']) -> MD5Hash: ...
@overload
async def get_file_hash_async(p: Path, hash_type: Literal['sha1']) -> SHA1Hash: ...
@overload
async def get_file_hash_async(p: Path, hash_type: Literal['sha256']) -> SHA256Hash: ...
async def get_file_hash_async(p: Path, hash_type: HashType) -> HashValueT:
    """Get the hash for the contents of a file asynchronously using :mod:`aiofile`

    Arguments:
        p: The file path
        hash_type: The hash type (``'md5'``, ``'sha1'``, or ``'sha256'``)

    """
    assert p.is_file()
    chunk_size = 65536
    h = hashlib.new(hash_type)
    async with aiofile.async_open(p, 'rb') as fp:
        async for _b in fp.iter_chunked(chunk_size=chunk_size):
            assert type(_b) is bytes
            h.update(_b)
    return _typed_hash_value(hash_type, h.hexdigest())


def seconds_to_time_str(seconds: int) -> str:
    """Format *seconds* as ``HH:MM:SS``
    """
    h = seconds // 3600
    m = (seconds - h * 3600) // 60
    s = seconds % 60
    return f'{h:02d}:{m:02d}:{s:02d}'


async def aio_read_iter(
    fd: aiofile.FileIOWrapperBase,
    chunk_size: int = 65536,
    timeout_total: float|None = None,
    timeout_chunk: float|None = None
) -> AsyncGenerator[str|bytes]:
    """Iterate over chunked segments of a file descriptor as a
    :term:`asynchronous generator` with optional timeouts

    Arguments:
        fd: A :class:`aiofile.utils.FileIOWrapperBase` (the context manager returned
            when using :func:`aiofile.utils.async_open` with :keyword:`async with`)
        chunk_size: The chunk sized passed to the
            :meth:`aiofile.utils.FileIOWrapperBase.iter_chunked` method
        timeout_total: Timeout to apply for the entire read operation.  If
            not given, no timeout will be enforced.
        timeout_chunk: Timeout to apply for each chunk iteration.  If not
            given, no tiemout will be enforced.

    Raises:
        TimeoutError: If either timeout argument is supplied and its limit
            was reached
    """

    chunk_iter = fd.iter_chunked(chunk_size=chunk_size)
    async with asyncio.timeout(timeout_total):
        while True:
            async with asyncio.timeout(timeout_chunk):
                try:
                    chunk = await anext(chunk_iter)
                except StopAsyncIteration:
                    break
                yield chunk


class CompletionCounts:
    """Helper to track item queue and completion counts

    >>> counts = CompletionCounts(max_items=10)
    >>> counts
    <CompletionCounts: queued=0, completed=0, active=0, progress=0%>

    >>> counts.num_queued += 4
    >>> counts
    <CompletionCounts: queued=4, completed=0, active=4, progress=0%>

    >>> counts.num_completed += 1
    >>> counts
    <CompletionCounts: queued=4, completed=1, active=3, progress=10%>
    >>> counts.full
    False

    >>> counts.num_queued += 6
    >>> counts
    <CompletionCounts: queued=10, completed=1, active=9, progress=10%>
    >>> counts.full
    True

    >>> counts.complete
    False
    >>> for i in range(9):
    ...     counts.num_completed += 1
    ...     print(repr(counts))
    <CompletionCounts: queued=10, completed=2, active=8, progress=20%>
    <CompletionCounts: queued=10, completed=3, active=7, progress=30%>
    <CompletionCounts: queued=10, completed=4, active=6, progress=40%>
    <CompletionCounts: queued=10, completed=5, active=5, progress=50%>
    <CompletionCounts: queued=10, completed=6, active=4, progress=60%>
    <CompletionCounts: queued=10, completed=7, active=3, progress=70%>
    <CompletionCounts: queued=10, completed=8, active=2, progress=80%>
    <CompletionCounts: queued=10, completed=9, active=1, progress=90%>
    <CompletionCounts: queued=10, completed=10, active=0, progress=100%>

    >>> counts.complete
    True

    """
    max_items: int|None
    """Maximum number of items"""
    enable_log: bool
    """If ``True`` any changes to :attr:`num_queued` or :attr:`num_completed`
    will be logged
    """
    def __init__(self, max_items: int|None = None, enable_log: bool = False) -> None:
        self.max_items = max_items
        self.enable_log = enable_log
        self._num_queued = 0
        self._num_completed = 0

    @property
    def num_queued(self) -> int:
        """Number of items that have been queued
        """
        return self._num_queued
    @num_queued.setter
    def num_queued(self, value: int) -> None:
        if value == self._num_queued:
            return
        self._num_queued = value
        self._log_counts('queued:    ')

    @property
    def num_completed(self) -> int:
        """Number of items that have been completed
        """
        return self._num_completed
    @num_completed.setter
    def num_completed(self, value: int) -> None:
        if value == self._num_completed:
            return
        self._num_completed = value
        self._log_counts('completed: ')

    @property
    def num_active(self) -> int:
        """Number of active items (``num_queued - num_completed``)
        """
        return self.num_queued - self.num_completed

    @property
    def progress(self) -> int:
        """Percent of items :attr:`completed <num_completed>` versus
        :attr:`max_items`

        .. note::

            This will be zero if :attr:`max_items` is ``None``

        """
        if self.max_items is None or self.max_items == 0:
            return 0
        p = self.num_completed / self.max_items
        return int(round(p * 100))

    @property
    def full(self) -> bool:
        """Whether all items have been queued

        .. note::

            This will always be ``False`` if :attr:`max_items` is ``None``

        """
        if self.max_items is None:
            return False
        return self.num_queued >= self.max_items

    @property
    def complete(self) -> bool:
        """Whether all items have been completed

        .. note::

            This will always be ``False`` if :attr:`max_items` is ``None``

        """
        if self.max_items is None:
            return False
        return self.num_completed >= self.max_items

    def reset(self) -> None:
        """Reset all counters to zero

        >>> counts = CompletionCounts(max_items=4)
        >>> counts
        <CompletionCounts: queued=0, completed=0, active=0, progress=0%>

        >>> counts.num_queued = 4
        >>> counts.num_completed = 2
        >>> counts.full
        True
        >>> counts
        <CompletionCounts: queued=4, completed=2, active=2, progress=50%>

        >>> counts.reset()
        >>> counts.full
        False
        >>> counts
        <CompletionCounts: queued=0, completed=0, active=0, progress=0%>

        """
        enable_log = self.enable_log
        self.enable_log = False
        self.num_completed = 0
        self.num_queued = 0
        self.enable_log = enable_log

    def _log_counts(self, msg: str) -> None:
        if self.enable_log:
            logger.debug(f'{msg}{self}')

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self}>'

    def __str__(self) -> str:
        fields = [
            ('queued', self.num_queued, ''), ('completed', self.num_completed, ''),
            ('active', self.num_active, ''), ('progress', self.progress, '%'),
        ]
        field_str = [
            f'{name}={value}{suffix}' for name, value, suffix in fields
        ]
        return ', '.join(field_str)

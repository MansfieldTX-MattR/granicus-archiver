from __future__ import annotations
from typing import TypedDict, NotRequired, Unpack, Self
from pathlib import Path

from yarl import URL
from aiohttp import (
    ClientSession, ClientTimeout, ClientResponse, ClientResponseError
)
import aiojobs
import aiofile

from .model import FileMeta


class DownloadError(Exception):
    """Raised if the downloaded content size does not match the "Content-Length"
    from the response headers
    """

class StupidZeroContentLengthError(Exception):
    """Raised when the reported "Content-Length" is zero
    """

class ThisShouldBeA404ErrorButItsNot(ClientResponseError):
    """Exception raised when Granicus redirects you to a warning page saying
    a file doesn't exist even though that is something built into the HTTP
    protocol yes this is a runon sentence in a docstring header line but I
    am beyond the point of caring

    Yes, another lovely edge case discovered - shocking.

    The response can be detected by a ``302 "Found"`` redirect with
    ``"/Confirmation.aspx"`` as the url path and ``M1=Gone`` in the query.
    """

    @classmethod
    def detect(cls, response: ClientResponse) -> bool:
        """Check whether this exception should be raised from the given *response*
        """
        hist = response.history
        if len(hist) < 2:
            return False
        if hist[-2].status != 302:
            return False
        url = response.url
        if url.path.endswith('Confirmation.aspx') and url.query.get('M1') == 'Gone':
            return True
        return False

    @classmethod
    def detect_and_raise(cls, response: ClientResponse) -> None:
        """Check and raise this exception if its pattern is found on *response*
        """
        if not cls.detect(response):
            return
        raise cls(
            request_info=response.request_info,
            history=response.history,
            status=404,
            message='Not Found',
            headers=response.headers,
        )


class DownloadRequest(TypedDict):
    """Keyword arguments to create :class:`FileDownload` instances
    """
    url: URL
    """URL for the download"""
    filename: Path
    """Local filename for the download"""
    chunk_size: NotRequired[int]
    """Chunk size for streaming download segments"""
    timeout: NotRequired[ClientTimeout]
    """Custom :class:`~aiohttp.ClientTimeout` specification"""

class DownloadResult(TypedDict):
    """Result type used for :attr:`FileDownload.result`
    """
    url: URL
    """The :attr:`~DownloadRequest.url` of the request"""
    filename: Path
    """The :attr:`~DownloadRequest.filename` of the request"""
    meta: FileMeta
    """Metadata from the response headers as a :class:`~.model.FileMeta` instance"""


class FileDownload:
    """Downloads a single file

    Arguments:
        session (aiohttp.ClientSession):
        **kwargs (DownloadRequest):
    """
    session: ClientSession
    """The current :class:`aiohttp.ClientSession`"""
    progress: float
    """Current download progress (from ``0.0`` to ``1.0``)"""
    def __init__(
        self,
        session: ClientSession,
        **kwargs: Unpack[DownloadRequest]
    ) -> None:
        self.session = session
        self.url = kwargs['url']
        self.filename = kwargs['filename']
        self.chunk_size = kwargs.get('chunk_size', 65536)
        timeout = kwargs.get('timeout')
        if timeout is None:
            timeout = ClientTimeout(total=300)
        self.timeout = timeout
        self.progress: float = 0
        self._meta: FileMeta|None = None

    @property
    def meta(self) -> FileMeta:
        """Metadata from the response headers
        """
        if self._meta is None:
            raise RuntimeError('FileMeta not available')
        return self._meta

    @property
    def result(self) -> DownloadResult:
        """The completed :class:`DownloadResult`
        """
        return {
            'url': self.url,
            'filename': self.filename,
            'meta': self.meta,
        }

    async def __call__(self) -> Self:
        async with self.session.get(self.url, timeout=self.timeout) as resp:
            ThisShouldBeA404ErrorButItsNot.detect_and_raise(resp)
            if resp.headers.get('Content-Length') in ['0', 0]:
                raise StupidZeroContentLengthError(f'Content-Length 0 for {self.url}')
            meta = self._meta = FileMeta.from_headers(resp.headers)
            if meta.content_length == 0:
                raise StupidZeroContentLengthError(f'Content-Length 0 for {self.url}')
            total_bytes = meta.content_length
            bytes_recv = 0
            async with aiofile.async_open(self.filename, 'wb') as fd:
                async for chunk in resp.content.iter_chunked(self.chunk_size):
                    await fd.write(chunk)
                    bytes_recv += len(chunk)
                    self.progress = total_bytes / bytes_recv
        st = self.filename.stat()
        if st.st_size != total_bytes:
            self.filename.unlink()
            raise DownloadError(f'Filesize mismatch: {st.st_size=}, {total_bytes=}, {bytes_recv=}, {self.url=}, {self.filename=}')
        return self


class Downloader:
    """Manager for scheduling :class:`FileDownload` jobs
    """
    session: ClientSession
    """The current :class:`aiohttp.ClientSession`"""
    scheduler: aiojobs.Scheduler|None
    """The :class:`~aiojobs.Scheduler` to place download jobs on"""
    default_chunk_size: int
    """Default to use for :attr:`FileDownload.chunk_size`"""
    default_timeout: ClientTimeout
    """Default to use for :attr:`FileDownload.timeout`"""
    def __init__(
        self,
        session: ClientSession,
        scheduler: aiojobs.Scheduler|None = None
    ) -> None:
        self.session = session
        self.scheduler = scheduler
        self.default_chunk_size = 65536
        self.default_timeout = ClientTimeout(total=300)

    def _build_download_obj(self, **kwargs: Unpack[DownloadRequest]) -> FileDownload:
        kwargs.setdefault('chunk_size', self.default_chunk_size)
        kwargs.setdefault('timeout', self.default_timeout)
        return FileDownload(session=self.session, **kwargs)

    async def spawn(self, **kwargs: Unpack[DownloadRequest]) -> aiojobs.Job[FileDownload]:
        """Create a :class:`FileDownload` and run it on the :attr:`scheduler`

        - If :attr:`~DownloadRequest.chunk_size` is not provided, the
          :attr:`default_chunk_size` will be used.
        - If :attr:`~DownloadRequest.timeout` is not provided,
          :attr:`default_timeout` will be used.

        Arguments:
            **kwargs (DownloadRequest): Keyword arguments to initialize the
                :class:`FileDownload` instance
        """
        if self.scheduler is None:
            raise RuntimeError('scheduler not set')
        dl = self._build_download_obj(**kwargs)
        return await self.scheduler.spawn(dl())

    async def download(self, **kwargs: Unpack[DownloadRequest]) -> FileDownload:
        """Create a :class:`FileDownload` and begin downloading immediately
        (bypassing :attr:`scheduler`)

        - If :attr:`~DownloadRequest.chunk_size` is not provided, the
          :attr:`default_chunk_size` will be used.
        - If :attr:`~DownloadRequest.timeout` is not provided,
          :attr:`default_timeout` will be used.

        Arguments:
            **kwargs (DownloadRequest): Keyword arguments to initialize the
                :class:`FileDownload` instance
        """
        dl = self._build_download_obj(**kwargs)
        return await dl()

from __future__ import annotations
from typing import TypedDict, NotRequired, Unpack, Self
from pathlib import Path

from yarl import URL
from aiohttp import ClientSession, ClientTimeout
import aiojobs
import aiofile

from .model import FileMeta


class DownloadError(Exception): ...

class StupidZeroContentLengthError(Exception): ...

class DownloadRequest(TypedDict):
    url: URL
    filename: Path
    chunk_size: NotRequired[int]
    timeout: NotRequired[ClientTimeout]

class DownloadResult(TypedDict):
    url: URL
    filename: Path
    meta: FileMeta


class FileDowload:
    def __init__(
        self,
        session: ClientSession,
        url: URL,
        filename: Path,
        chunk_size: int = 65536,
        timeout: ClientTimeout|None = None
    ) -> None:
        self.session = session
        self.url = url
        self.filename = filename
        self.chunk_size = chunk_size
        if timeout is None:
            timeout = ClientTimeout(total=300)
        self.timeout = timeout
        self.progress: float = 0
        self._meta: FileMeta|None = None

    @property
    def meta(self) -> FileMeta:
        if self._meta is None:
            raise RuntimeError('FileMeta not available')
        return self._meta

    @property
    def result(self) -> DownloadResult:
        return {
            'url': self.url,
            'filename': self.filename,
            'meta': self.meta,
        }

    async def __call__(self) -> Self:
        async with self.session.get(self.url) as resp:
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
            raise DownloadError(f'Filesize mismatch: {self.url=}, {self.filename=}')
        return self


class Downloader:
    def __init__(
        self,
        session: ClientSession,
        scheduler: aiojobs.Scheduler|None = None
    ) -> None:
        self.session = session
        self.scheduler = scheduler
        self.default_chunk_size = 65536
        self.default_timeout = ClientTimeout(total=300)

    def _build_download_obj(self, **kwargs: Unpack[DownloadRequest]) -> FileDowload:
        kwargs.setdefault('chunk_size', self.default_chunk_size)
        kwargs.setdefault('timeout', self.default_timeout)
        return FileDowload(session=self.session, **kwargs)

    async def spawn(self, **kwargs: Unpack[DownloadRequest]) -> aiojobs.Job[FileDowload]:
        if self.scheduler is None:
            raise RuntimeError('scheduler not set')
        dl = self._build_download_obj(**kwargs)
        return await self.scheduler.spawn(dl())

    async def download(self, **kwargs: Unpack[DownloadRequest]) -> FileDowload:
        dl = self._build_download_obj(**kwargs)
        return await dl()

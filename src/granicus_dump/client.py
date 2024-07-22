from typing import TypeVar, Generic, Coroutine, Self, Any, TYPE_CHECKING
import asyncio
from pathlib import Path
import dataclasses
import json
from loguru import logger
import tempfile

import click
from aiohttp import ClientSession, ClientTimeout
import aiojobs
import aiofile
from yarl import URL

from .parser import parse_page
from .model import ClipCollection, Clip, ParseClipData, ParseClipLinks, ClipFileKey

DATA_URL = URL('https://mansfieldtx.granicus.com/ViewPublisher.php?view_id=6')


T = TypeVar('T')

class DownloadError(Exception): ...

# class BatchQueue(Generic[T]):
#     def __init__(self, max_batches: int = MAX_BATCHES) -> None:
#         self.max_batches = max_batches
#         self._q = asyncio.Queue(maxsize=max_batches)



#     # async def __aenter__(self) -> Self:

SCHEDULER: aiojobs.Scheduler|None = None
def get_scheduler(limit: int|None = None) -> aiojobs.Scheduler:
    global SCHEDULER
    if SCHEDULER is None:
        if limit is None:
            raise RuntimeError('scheduler limit must be set')
        SCHEDULER = aiojobs.Scheduler(limit=limit)
    elif limit is not None:
        raise RuntimeError('scheduler already created')
    return SCHEDULER


async def get_main_data(session: ClientSession, base_dir: Path) -> ClipCollection:
    async with session.get(str(DATA_URL)) as response:
        if not response.ok:
            response.raise_for_status()
        html = await response.text()
        clips = parse_page(html, base_dir=base_dir, scheme=DATA_URL.scheme)
    return clips

# @logger.catch
async def get_real_pdf_link(session: ClientSession, src_url: URL) -> URL:
    logger.info(f'get_real_pdf_link: {src_url=}')
    # https://docs.google.com/gview?url=http://legistar.granicus.com/Mansfield/meetings/2018/3/2571_M_City_Council_18-03-26_Meeting_Minutes.pdf&embedded=true
    async with session.get(src_url, allow_redirects=False) as response:
        assert response.status == 302
        logger.debug(f'{response.url=}, {response.real_url=}')
        logger.debug(f'{response.headers=}')
        re_url = response.headers['Location']
        assert len(re_url)
        re_url = URL(re_url)
        if re_url.host == 'docs.google.com':
            real_url = URL(re_url.query.getone('url'))
        else:
            real_url = re_url
        logger.debug(f'{real_url=}')
        return real_url


async def replace_pdf_links(
    session: ClientSession,
    parse_clip: ParseClipData,
) -> ParseClipData:
    if parse_clip.actual_links is not None:
        return parse_clip
    link_kw = {}
    for key, url in parse_clip.original_links.iter_existing():
        real_key = key.split('_')[0]
        if key == 'audio' or key == 'video':
            link_kw[real_key] = url
            continue
        real_url = await get_real_pdf_link(session, url)
        link_kw[real_key] = real_url
    links = ParseClipLinks(**link_kw)
    parse_clip.actual_links = links
    return parse_clip


async def replace_all_pdf_links(session: ClientSession, clips: ClipCollection) -> None:
    scheduler = get_scheduler()
    jobs: set[aiojobs.Job[ParseClipData]] = set()
    for clip in clips:
        if clip.parse_data.actual_links is not None:
            continue
        job = await scheduler.spawn(replace_pdf_links(session, clip.parse_data))
        jobs.add(job)
    if len(jobs):
        await asyncio.gather(*[job.wait() for job in jobs])


async def download_clip(session: ClientSession, clip: Clip):
    scheduler = get_scheduler()

    async def download_file(
        key: ClipFileKey,
        url: URL,
        filename: Path,
        temp_dir: Path
    ) -> asyncio.Task:
        temp_filename = temp_dir / filename.name
        logger.debug(f'download {url} to {filename}')
        chunk_size = 64*1024
        if key == 'video':
            # 1 hour total, 1 minute for connect, 1 minute between reads
            timeout = ClientTimeout(total=60*60, sock_connect=60, sock_read=60)
        else:
            timeout = ClientTimeout(total=300)
        async with session.get(url, timeout=timeout) as response:
            # logger.debug(f'{response.headers=}')
            meta = clip.files.set_metadata(key, response.headers)
            async with aiofile.async_open(temp_filename, 'wb') as fd:
                async for chunk in response.content.iter_chunked(chunk_size):
                    await fd.write(chunk)
        if meta.content_length is not None:
            stat = temp_filename.stat()
            if stat.st_size != meta.content_length:
                raise DownloadError(f'Filesize mismatch: {clip.unique_name=}, {key=}, {url=}, {stat.st_size=}, {response.headers=}')
        logger.debug(f'download complete for "{clip.unique_name} - {key}"')
        copy_task = asyncio.create_task(copy_clip_to_dest(key, src_file=temp_filename, dst_file=filename))
        return copy_task


    async def copy_clip_to_dest(key: ClipFileKey, src_file: Path, dst_file: Path) -> None:
        chunk_size = 64*1024
        logger.debug(f'copying "{clip.unique_name} - {key}"')

        async with aiofile.async_open(src_file, 'rb') as src_fd:
            async with aiofile.async_open(dst_file, 'wb') as dst_fd:
                async for chunk in src_fd.iter_chunked(chunk_size):
                    await dst_fd.write(chunk)
        logger.debug(f'copy complete for "{clip.unique_name} - {key}"')
        src_file.unlink()

    logger.info(f'downloading clip "{clip.unique_name}"')
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).resolve()
        clip.root_dir_abs.mkdir(exist_ok=True, parents=True)
        download_jobs: set[aiojobs.Job[asyncio.Task]] = set()
        download_job_waiters: set[asyncio.Task[asyncio.Task]] = set()
        copy_tasks: set[asyncio.Task] = set()
        for key, url, filename in clip.iter_url_paths():
            if filename.exists():
                logger.debug(f'filename exists: {key=}, {filename=}')
                continue
            logger.debug(f'{scheduler.active_count=}')
            job = await scheduler.spawn(download_file(key, url, filename, temp_dir))
            download_jobs.add(job)
            download_job_waiters.add(asyncio.create_task(job.wait()))
        while len(download_job_waiters):
            done, pending = await asyncio.wait(download_job_waiters, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                exc = t.exception()
                if exc is not None:
                    raise exc
                copy_task = t.result()
                copy_tasks.add(copy_task)
                download_job_waiters.discard(t)
        if len(copy_tasks):
            await asyncio.gather(*copy_tasks)

    logger.success(f'clip "{clip.unique_name}" complete')

def check_clip_files(clip: Clip):
    for key, url, filename in clip.iter_url_paths():
        if not filename.exists():
            continue
        meta = clip.files.get_metadata(key)
        if meta is None:
            logger.warning(f'No metadata for "{filename}"')
            filename.unlink()
            continue
        assert meta.content_length is not None
        if filename.stat().st_size != meta.content_length:
            logger.warning(f'Size mismatch for "{filename}"')
            filename.unlink()

def check_all_clip_files(clips: ClipCollection):
    for clip in clips:
        if clip.complete:
            continue
        check_clip_files(clip)


@logger.catch
async def amain(
    data_file: Path,
    out_dir: Path,
    scheduler_limit: int,
    max_clips: int|None = None,
):
    scheduler = get_scheduler(limit=scheduler_limit)
    local_clips: ClipCollection|None = None
    if data_file.exists():
        local_clips = ClipCollection.load(data_file)
    async with ClientSession() as session:
        clips = await get_main_data(session, out_dir)
        if local_clips is not None:
            clips = clips.merge(local_clips)
        await replace_all_pdf_links(session, clips)
        clips.save(data_file)
        check_all_clip_files(clips)
        i = 0
        jobs: set[aiojobs.Job] = set()
        for clip in clips:
            if max_clips == 0:
                break
            if clip.complete:
                logger.debug(f'skipping {clip.unique_name}')
                continue
            logger.debug(f'{scheduler.active_count=}')
            job = await scheduler.spawn(download_clip(session, clip))
            jobs.add(job)
            i += 1
            if max_clips is not None and i >= max_clips:
                break
        if len(jobs):
            logger.debug(f'awaiting {len(jobs)} jobs')
            await asyncio.gather(*[job.wait() for job in jobs])
        logger.info('closing scheduler..')
        await scheduler.close()
        logger.debug('scheduler closed')
        clips.save(data_file)
    return clips


@click.command
@click.argument(
    'out_dir',
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    '--data-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
)
@click.option('--max-clips', type=int, required=False)
@click.option('--job-limit', type=int, default=8, show_default=True)
@click.option('--check-only', is_flag=True)
def main(
    out_dir: Path,
    data_file: Path|None,
    max_clips: int|None,
    job_limit: int,
    check_only: bool,
):
    if data_file is None:
        data_file = out_dir / 'data.json'
    if check_only:
        clips = ClipCollection.load(data_file)
        check_all_clip_files(clips)
        return
    clips = asyncio.run(amain(
        data_file=data_file,
        out_dir=out_dir,
        scheduler_limit=job_limit,
        max_clips=max_clips,
    ))


if __name__ == '__main__':
    main()

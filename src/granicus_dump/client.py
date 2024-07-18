from typing import TypeVar, Generic, Coroutine, Self, Any, TYPE_CHECKING
import asyncio
from pathlib import Path
import dataclasses
import json
from loguru import logger

import click
from aiohttp import ClientSession
import aiojobs
from yarl import URL

from .parser import parse_page
from .model import ClipCollection, Clip, ParseClipData, ParseClipLinks, ClipFileKey

MAX_BATCHES = 8
DATA_URL = URL('https://mansfieldtx.granicus.com/ViewPublisher.php?view_id=6')


T = TypeVar('T')


# class BatchQueue(Generic[T]):
#     def __init__(self, max_batches: int = MAX_BATCHES) -> None:
#         self.max_batches = max_batches
#         self._q = asyncio.Queue(maxsize=max_batches)



#     # async def __aenter__(self) -> Self:

SCHEDULER: aiojobs.Scheduler|None = None
def get_scheduler() -> aiojobs.Scheduler:
    global SCHEDULER
    if SCHEDULER is None:
        SCHEDULER = aiojobs.Scheduler(limit=MAX_BATCHES)
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
    async def download_file(url: URL, filename: Path):
        logger.debug(f'download {url} to {filename}')
        chunk_size = 64*1024
        async with session.get(url) as response:
            with filename.open('wb') as fd:
                async for chunk in response.content.iter_chunked(chunk_size):
                    fd.write(chunk)
    logger.info(f'downloading clip "{clip.unique_name}"')
    clip.root_dir.mkdir(exist_ok=False, parents=True)
    coros: set[Coroutine[Any, Any, None]] = set()
    for url, filename in clip.iter_url_paths():
        coros.add(download_file(url, filename))
    if len(coros):
        await asyncio.gather(*coros)
    logger.success(f'clip "{clip.unique_name}" complete')


async def amain(data_file: Path, out_dir: Path):
    scheduler = get_scheduler()
    local_clips: ClipCollection|None = None
    if data_file.exists():
        local_clips = ClipCollection.load(data_file)
    async with ClientSession() as session:
        clips = await get_main_data(session, out_dir)
        if local_clips is not None:
            clips = clips.merge(local_clips)
        await replace_all_pdf_links(session, clips)
        clips.save(data_file)
        i = 0
        coros: set[Coroutine[Any, Any, None]] = set()
        jobs: set[aiojobs.Job] = set()
        for clip in clips:
            if clip.complete:
                logger.debug(f'skipping {clip.unique_name}')
                continue
            # coros.add(download_clip(session, clip))
            job = await scheduler.spawn(download_clip(session, clip))
            jobs.add(job)
            i += 1
            break
            if i >= 5:
                break
        # if len(coros):
        #     await asyncio.gather(*coros)
        if len(jobs):
            await asyncio.gather(*[job.wait() for job in jobs])
        await scheduler.close()
    return clips

def main():
    out_dir = Path('data')
    data_file = out_dir / 'data.json'
    clips = asyncio.run(amain(data_file, out_dir))

if __name__ == '__main__':
    main()

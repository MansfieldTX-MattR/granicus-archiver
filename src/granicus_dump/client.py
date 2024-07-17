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
from .model import ClipCollection, Clip, ParseClipData, ParseClipUrlKey

MAX_BATCHES = 30
DATA_URL = URL('https://mansfieldtx.granicus.com/ViewPublisher.php?view_id=6')


T = TypeVar('T')


# class BatchQueue(Generic[T]):
#     def __init__(self, max_batches: int = MAX_BATCHES) -> None:
#         self.max_batches = max_batches
#         self._q = asyncio.Queue(maxsize=max_batches)



#     # async def __aenter__(self) -> Self:



async def get_main_data(session: ClientSession, base_dir: Path, fix_pdf_links: bool = True) -> ClipCollection:
    async with session.get(str(DATA_URL)) as response:
        if not response.ok:
            response.raise_for_status()
        html = await response.text()
        clips = parse_page(html, base_dir=base_dir, scheme=DATA_URL.scheme)
    if not fix_pdf_links:
        return clips


    # for clip in clips:
    #     await replace_pdf_links(session, clip.parse_data)
    #     # print(clip.parse_data)
    #     break

    # scheduler = aiojobs.Scheduler(limit=MAX_BATCHES)

    # for clip in clips:
    #     await scheduler.spawn(replace_pdf_links(session, clip.parse_data))

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
    in_place: bool = True
) -> ParseClipData:
    existing_links = parse_clip.existing_links
    replace_links: dict[ParseClipUrlKey, URL] = {}
    for key, url in existing_links.items():
        if key == 'audio_link' or key == 'video_link':
            continue
        real_url = await get_real_pdf_link(session, url)
        replace_links[key] = real_url
    if len(replace_links):
        if in_place:
            for key, val in replace_links.items():
                setattr(parse_clip, key, val)
        else:
            return dataclasses.replace(parse_clip, **replace_links)
    return parse_clip


async def download_clip(session: ClientSession, clip: Clip):
    async def download_file(url: URL, filename: Path):
        logger.debug(f'download {url} to {filename}')
        chunk_size = 64*1024
        async with session.get(url) as response:
            with filename.open('wb') as fd:
                async for chunk in response.content.iter_chunked(chunk_size):
                    fd.write(chunk)
    logger.info(f'downloading clip "{clip.unique_name}"')
    parse_clip = await replace_pdf_links(session, clip.parse_data, in_place=False)
    clip = dataclasses.replace(clip, parse_data=parse_clip)
    clip.root_dir.mkdir(exist_ok=False, parents=True)
    coros: set[Coroutine[Any, Any, None]] = set()
    for url, filename in clip.iter_url_paths():
        coros.add(download_file(url, filename))
    if len(coros):
        await asyncio.gather(*coros)
    logger.success(f'clip "{clip.unique_name}" complete')


async def amain():
    async with ClientSession() as session:
        clips = await get_main_data(session, Path('data'), fix_pdf_links=True)
        i = 0
        coros: set[Coroutine[Any, Any, None]] = set()
        for clip in clips:
            if clip.complete:
                logger.debug(f'skipping {clip.unique_name}')
                continue
            # coros.add(download_clip(session, clip))
            await download_clip(session, clip)
            i += 1
            break
            if i >= 5:
                break
        # if len(coros):
        #     await asyncio.gather(*coros)
        return clips

def main():
    clips = asyncio.run(amain())
    outfile = clips.base_dir / 'data.json'
    d = clips.serialize()
    outfile.write_text(json.dumps(d, indent=2))

if __name__ == '__main__':
    main()

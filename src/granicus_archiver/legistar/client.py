from __future__ import annotations
from typing import ClassVar, Self, TYPE_CHECKING
from pathlib import Path
import tempfile

from aiohttp import ClientSession, ClientTimeout
import aiojobs
import aiofile
from yarl import URL
from loguru import logger

if TYPE_CHECKING:
    from ..config import Config
from ..client import DownloadError
from ..utils import JobWaiters
from ..model import CLIP_ID, ClipCollection, FileMeta
from .rss_parser import Category, Feed, FeedItem
from .model import LegistarData, DetailPageResult, IncompleteItemError



class Client:
    temp_dir: Path
    _temp_dir: tempfile.TemporaryDirectory
    _default_limit: ClassVar[int] = 8
    session: ClientSession
    scheduler: aiojobs.Scheduler
    waiter: JobWaiters[DetailPageResult]
    def __init__(
        self,
        clips: ClipCollection,
        config: Config,
    ) -> None:
        self.clips = clips
        self.data_filename = config.legistar.data_file
        self.feed_urls = config.legistar.feed_urls
        self.legistar_category_maps = config.legistar.category_maps
        root_dir = config.legistar.out_dir
        if self.data_filename.exists():
            self.legistar_data = LegistarData.load(self.data_filename, root_dir=root_dir)
        else:
            self.legistar_data = LegistarData(root_dir=root_dir)

    async def open(self) -> None:
        self.session = ClientSession()
        self.scheduler = aiojobs.Scheduler(limit=self._default_limit)
        self.waiter = JobWaiters(self.scheduler)
        self._temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir = Path(self._temp_dir.name).resolve()

    async def close(self) -> None:
        await self.scheduler.close()
        await self.session.close()
        self._temp_dir.cleanup()

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def parse_feed(self, feed_url: URL) -> Feed:
        async with self.session.get(feed_url) as resp:
            if not resp.ok:
                resp.raise_for_status()
            content = await resp.text()
        feed = Feed.from_feed(content, category_maps=self.legistar_category_maps)
        assert len(feed.item_list) == len(feed.items)
        for feed_item in feed.item_list:
            assert feed_item.guid in feed.items
            assert feed_item.guid in feed
        logger.debug(f'{len(feed)=}')
        return feed

    async def parse_detail_page(self, feed_item: FeedItem) -> DetailPageResult:
        logger.debug(f'parse page: {feed_item.link}')
        async with self.session.get(feed_item.link) as resp:
            if not resp.ok:
                resp.raise_for_status()
            resp_text = await resp.text()
        return DetailPageResult.from_html(
            html_str=resp_text,
            feed_item=feed_item,
        )

    async def parse_detail_pages(self, feed: Feed):
        logger.debug('parsing detail pages')
        for feed_item in feed:
            parsed_item = self.legistar_data.get(feed_item.guid)
            if parsed_item is not None:
                continue
            try:
                result = await self.parse_detail_page(feed_item)
            except IncompleteItemError:
                logger.warning(f'incomplete item: {feed_item.guid=}, {feed_item.category=}, {feed_item.title=}')
                continue
            self.legistar_data.add_detail_result(result)
        logger.debug('detail wait complete')

    def match_clips_to_feed(self):
        for clip in self.clips:
            if clip.id in self.legistar_data.matched_guids:
                continue
            item = self.legistar_data.find_match_for_clip_id(clip.id)
            if item is None:
                continue
            self.legistar_data.add_guid_match(clip.id, item.feed_guid)

    async def parse_feeds(self):
        for name, feed_url in self.feed_urls.items():
            logger.info(f'parsing feed "{name}"')
            feed = await self.parse_feed(feed_url)
            await self.parse_detail_pages(feed)
            self.legistar_data.save(self.data_filename)
            logger.success(f'feed parse complete for "{name}"')

    async def parse_all(self):
        await self.parse_feeds()
        self.match_clips_to_feed()
        self.legistar_data.save(self.data_filename)

    @logger.catch
    async def do_download(self, clip_id: CLIP_ID, detail_result: DetailPageResult):
        url = detail_result.links.agenda_packet
        assert url is not None
        chunk_size = 64*1024
        clip = self.clips[clip_id]
        filename = clip.get_file_path('agenda_packet', absolute=True)
        temp_filename = self.temp_dir / clip.id / filename.name
        temp_filename.parent.mkdir()
        timeout = ClientTimeout(total=60*60, sock_connect=60, sock_read=60)
        logger.debug(f'begin download for {clip.id} > {filename.name}')
        async with self.session.get(url, timeout=timeout) as resp:
            # meta = clip.files.set_metadata('agenda_packet', resp.headers)
            meta = FileMeta.from_headers(resp.headers)
            async with aiofile.async_open(temp_filename, 'wb') as fd:
                async for chunk in resp.content.iter_chunked(chunk_size):
                    await fd.write(chunk)
        if meta.content_length is not None:
            stat = temp_filename.stat()
            if stat.st_size != meta.content_length:
                raise DownloadError(f'Filesize mismatch: {clip.unique_name=}, {url=}, {stat.st_size=}, {resp.headers=}')
        logger.debug(f'download complete for {clip.id} > {filename.name}')
        temp_filename.rename(filename)
        clip.files.check_agenda_packet_file()
        return detail_result

    async def download_agenda_packets(self, max_clips: int) -> int:
        dl_waiter = JobWaiters[DetailPageResult](scheduler=self.scheduler)
        logger.debug(f'downloading... {max_clips=}, {len(self.legistar_data)=}')
        count = 0
        for clip_id, detail_result in self.legistar_data.iter_guid_matches():
            if detail_result.links.agenda_packet is None:
                continue
            clip = self.clips[clip_id]
            filename = clip.get_file_path('agenda_packet', absolute=True)
            if filename.exists():
                continue
            # skip non-existent local clips (for now)
            if not filename.parent.exists():
                continue
            logger.info(f'download agenda packet for {clip_id} > {filename=}')
            # filename.parent.mkdir(exist_ok=True)
            await dl_waiter.spawn(self.do_download(clip_id, detail_result))
            count += 1
            if count > max_clips:
                break
        await dl_waiter.gather()
        return count

    def check_files(self):
        for clip_id, detail_result in self.legistar_data.iter_guid_matches():
            if detail_result.links.agenda_packet is None:
                continue
            clip = self.clips[clip_id]
            filename = clip.get_file_path('agenda_packet', absolute=True)
            if not filename.exists():
                continue
            meta = clip.files.get_metadata('agenda_packet')
            assert meta is not None
            assert filename.stat().st_size == meta.content_length


@logger.catch
async def amain(
    config: Config,
    max_clips: int = 0,
    check_only: bool = False
):
    clips = ClipCollection.load(config.data_file)
    client = Client(
        clips=clips,
        config=config,
    )
    client.check_files()
    if check_only:
        return
    async with client:
        await client.parse_all()
        if max_clips > 0:
            count = await client.download_agenda_packets(max_clips=max_clips)
            if count > 0:
                clips.save(data_file)

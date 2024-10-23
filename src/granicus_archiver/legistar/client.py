from __future__ import annotations
from typing import ClassVar, Self, TypedDict, NotRequired, Unpack, TYPE_CHECKING
from pathlib import Path
import tempfile
import asyncio

from aiohttp import ClientSession, ClientTimeout, ServerTimeoutError
import aiojobs
import aiofile
from yarl import URL
from loguru import logger

if TYPE_CHECKING:
    from ..config import Config
from ..utils import JobWaiters, remove_pdf_links, is_same_filesystem
from ..model import ClipCollection, Clip
from .types import GUID, LegistarFileKey, LegistarFileUID
from .rss_parser import Feed, FeedItem, ParseError, LegistarThinksRSSCanPaginateError
from .model import (
    LegistarData, DetailPageResult, IncompleteItemError, HiddenItemError,
    is_attachment_uid, uid_to_file_key,
)
from ..downloader import Downloader, DownloadResult, StupidZeroContentLengthError



class Client:
    temp_dir: Path
    _temp_dir: tempfile.TemporaryDirectory
    _default_limit: ClassVar[int] = 8
    session: ClientSession
    scheduler: aiojobs.Scheduler
    download_scheduler: aiojobs.Scheduler
    waiter: JobWaiters[DetailPageResult]
    def __init__(
        self,
        clips: ClipCollection,
        config: Config,
        allow_updates: bool = False,
        strip_pdf_links: bool = False
    ) -> None:
        self.clips = clips
        self.data_filename = config.legistar.data_file
        self.feed_urls = config.legistar.feed_urls
        self.legistar_category_maps = config.legistar.category_maps
        self.allow_updates = allow_updates
        self.strip_pdf_links = strip_pdf_links
        root_dir = config.legistar.out_dir
        self.config = config
        if self.data_filename.exists():
            self.legistar_data = LegistarData.load(self.data_filename, root_dir=root_dir)
        else:
            self.legistar_data = LegistarData(root_dir=root_dir)
        self.all_feeds: dict[str, Feed] = {}

    async def open(self) -> None:
        self.session = ClientSession()
        self.scheduler = aiojobs.Scheduler(limit=self._default_limit)
        self.waiter = JobWaiters(self.scheduler)
        self.download_scheduler = aiojobs.Scheduler(limit=4)
        self.downloader = Downloader(session=self.session, scheduler=self.download_scheduler)
        self.completion_scheduler = aiojobs.Scheduler(limit=8)
        self._temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir = Path(self._temp_dir.name).resolve()

    async def close(self) -> None:
        try:
            await self.completion_scheduler.close()
            await self.download_scheduler.close()
            await self.scheduler.close()
            await self.session.close()
        finally:
            self._temp_dir.cleanup()

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(self, *args):
        await self.close()

    def get_item_count(self) -> int:
        return len(self.legistar_data)

    def get_file_count(self) -> int:
        root_dir = self.legistar_data.root_dir
        return len([p for p in root_dir.glob('**/*.pdf')])

    async def parse_feed(self, name: str, feed_url: URL) -> tuple[str, Feed]:
        logger.info(f'parsing feed "{name}"')
        async with self.session.get(feed_url) as resp:
            if not resp.ok:
                resp.raise_for_status()
            content = await resp.text()
        overflow_allowed = self.config.legistar.is_feed_overflow_allowed(name)
        try:
            feed = Feed.from_feed(
                content,
                category_maps=self.legistar_category_maps,
                overflow_allowed=overflow_allowed,
            )
        except LegistarThinksRSSCanPaginateError:
            # Recreate the exception, but show the feed url for clarity
            raise LegistarThinksRSSCanPaginateError(f'{feed_url=}')
        assert len(feed.item_list) == len(feed.items)
        for feed_item in feed.item_list:
            assert feed_item.guid in feed.items
            assert feed_item.guid in feed
        logger.debug(f'{len(feed)=}')
        return name, feed

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

    async def parse_detail_pages(self, name: str, feed: Feed) -> tuple[str, bool]:
        async def do_page_parse(feed_item: FeedItem) -> DetailPageResult|None:
            try:
                result = await self.parse_detail_page(feed_item)
            except HiddenItemError:
                result = None
            except IncompleteItemError:
                logger.warning(f'incomplete item: {feed_item.to_str()}')
                result = None
            return result

        async def handle_update(feed_item: FeedItem, existing_item: DetailPageResult) -> tuple[bool, list[str]]:
            # NOTE: This is untested so the mutations are currently disabled
            result = await do_page_parse(feed_item)
            if result is None:
                return False, []
            changed, file_keys, attachment_keys = existing_item.update(result)
            if not changed:
                return False, []
            item_files = self.legistar_data.files.get(existing_item.feed_guid)
            actions = []
            if item_files is not None:
                for file_key in file_keys:
                    filename = item_files[file_key]
                    if filename is None:
                        continue
                    actions.append(f'rm {filename}')
                    # filename.unlink()
                    # TODO: maybe add a `__delitem__` method to `LegistarFiles`
                    # item_files[file_key] = None
                for att_name in attachment_keys:
                    attachment = item_files.attachments.get(att_name)
                    if attachment is None:
                        continue
                    actions.append(f'rm {attachment.filename}')
                    # attachment.filename.unlink()
                    actions.append(f'del attachments["{att_name}"]')
                    # del item_files.attachments[att_name]
            return True, actions

        logger.debug(f'parsing detail pages for "{name}"')
        changed = False
        try:
            for feed_item in feed:
                if feed_item.is_future:
                    continue
                parsed_item = self.legistar_data.get(feed_item.guid)
                if parsed_item is None:
                    similar_item = self.legistar_data.get_by_real_guid(feed_item.real_guid)
                    if similar_item is not None:
                        if self.allow_updates:
                            logger.info(f'Updating item "{similar_item.feed_guid}"')
                            _changed, actions = await handle_update(feed_item, similar_item)
                            if _changed:
                                raise Exception('\n'.join(actions))
                                changed = True
                            continue
                        else:
                            logger.warning(f'real_guid exists: {similar_item.feed_guid=}, {feed_item.guid=}')
                            continue

                if parsed_item is not None:
                    if parsed_item.is_hidden:
                        continue
                    if not parsed_item.is_final and not parsed_item.is_in_past:
                        logger.warning(f'existing item not final: {feed_item.to_str()}')
                    continue
                result = await do_page_parse(feed_item)
                if result is None:
                    continue

                assert parsed_item is None
                self.legistar_data.add_detail_result(result)
                changed = True
        except Exception as exc:
            logger.exception(exc)
            raise
        logger.debug('detail wait complete')
        return name, changed

    def match_clips(self) -> None:
        for clip in self.clips:
            if clip.id in self.legistar_data.matched_guids:
                continue
            item = self.legistar_data.find_match_for_clip_id(clip.id)
            if item is None:
                item = self.get_clip_feed_match(clip)
                if item is None:
                    continue
                logger.debug(f'match item from feed: {clip.unique_name=}, {item.feed_item.to_str()}')
            if item.feed_guid in self.legistar_data.matched_guids.values():
                logger.warning(f'ambiguous feed match: {clip.unique_name=}, {item.feed_item.to_str()}')
                continue
            self.legistar_data.add_guid_match(clip.id, item.feed_guid)

    def get_clip_feed_match(self, clip: Clip) -> DetailPageResult|None:
        feed_item: FeedItem|None = None
        for feed in self.all_feeds.values():
            try:
                feed_item = feed.find_clip_match(clip)
            except ParseError:
                feed_item = None
            if feed_item is None or feed_item.is_future:
                continue
            parsed_item = self.legistar_data[feed_item.guid]
            if parsed_item.agenda_status == 'Draft' or parsed_item.agenda_status == 'Not Viewable by the Public':
                return None
            return parsed_item


    async def parse_feeds(self):
        feed_waiters = JobWaiters[tuple[str, Feed]](self.scheduler)
        for name, feed_url in self.feed_urls.items():
            await feed_waiters.spawn(self.parse_feed(name, feed_url))

        async for result in feed_waiters.as_completed():
            result.raise_exc()
            name, feed = result.result
            self.all_feeds[name] = feed

        for name, feed in self.all_feeds.items():
            _, changed = await self.parse_detail_pages(name, feed)
            logger.success(f'feed parse complete for "{name}"')
            if changed:
                self.legistar_data.save(self.data_filename)

    async def parse_all(self):
        self.legistar_data.ensure_no_future_items()
        await self.parse_feeds()
        self.legistar_data.ensure_unique_item_folders()
        self.match_clips()
        self.legistar_data.save(self.data_filename)

    # @logger.catch
    async def download_feed_item(self, guid: GUID) -> None:
        loop = asyncio.get_running_loop()
        waiters = JobWaiters[tuple[LegistarFileUID, Path, DownloadResult]|None](self.download_scheduler)
        completion_waiters = JobWaiters(self.completion_scheduler)
        temp_dir = self.temp_dir / guid
        temp_dir.mkdir()
        chunk_size = 16384

        @logger.catch
        async def do_download(
            uid: LegistarFileUID,
            abs_filename: Path,
            url: URL
        ) -> tuple[LegistarFileUID, Path, DownloadResult]|None:
            temp_filename = temp_dir / abs_filename.name
            temp_filename.parent.mkdir(exist_ok=True)
            logger.debug(f'downloading file "{url}"')
            try:
                dl = await self.downloader.download(
                    url=url, filename=temp_filename, chunk_size=chunk_size,
                    timeout=ClientTimeout(total=60*60, sock_connect=60, sock_read=60),
                )
            except ServerTimeoutError as exc:
                if temp_filename.exists():
                    temp_filename.unlink()
                logger.exception(exc)
                return None
            except StupidZeroContentLengthError as exc:
                logger.warning(str(exc))
                return None
            logger.debug(f'download complete for "{url}"')
            return uid, abs_filename, dl.result

        @logger.catch
        async def complete_download(
            uid: LegistarFileUID,
            abs_filename: Path,
            dl_result: DownloadResult
        ) -> None:
            temp_filename = dl_result['filename']
            meta = dl_result['meta']
            strip_output: Path|None = None
            try:
                if self.strip_pdf_links:
                    logger.debug(f'strip pdf links for {abs_filename}')
                    strip_output = temp_filename.with_name(f'stripped-{temp_filename.name}')
                    await loop.run_in_executor(None,
                        remove_pdf_links, temp_filename, strip_output,
                    )
                    logger.debug(f'pdf links stripped for {abs_filename}')
                    assert strip_output.exists()
                    meta.content_length = strip_output.stat().st_size
                    temp_filename.unlink()
                    temp_filename = strip_output
                    strip_output = None
            except Exception as exc:
                if strip_output is not None and strip_output.exists():
                    strip_output.unlink()
                if temp_filename.exists():
                    temp_filename.unlink()
                raise

            if is_same_filesystem(temp_filename, abs_filename):
                temp_filename.rename(abs_filename)
                file_obj = self.legistar_data.set_uid_complete(
                    guid, uid, meta, pdf_links_removed=self.strip_pdf_links,
                )
                assert file_obj.pdf_links_removed
                assert file_obj.metadata.content_length == meta.content_length
                return
            chunk_size = 64*1024
            logger.debug(f'copying "{temp_filename} -> {abs_filename}"')

            async with aiofile.async_open(temp_filename, 'rb') as src_fd:
                async with aiofile.async_open(abs_filename, 'wb') as dst_fd:
                    async for chunk in src_fd.iter_chunked(chunk_size):
                        await dst_fd.write(chunk)
            logger.debug(f'copy complete for "{abs_filename}"')
            temp_filename.unlink()
            file_obj = self.legistar_data.set_uid_complete(
                guid, uid, meta, pdf_links_removed=self.strip_pdf_links,
            )
            assert file_obj.pdf_links_removed
            assert file_obj.metadata.content_length == meta.content_length

        detail_item = self.legistar_data[guid]
        p = self.legistar_data.get_folder_for_item(detail_item)
        logger.info(f'downloading files for {p.name}')
        it = self.legistar_data.iter_url_paths_uid(guid)
        for uid, abs_filename, url, is_complete in it:
            if is_complete:
                continue
            is_attachment = is_attachment_uid(uid)
            if not is_attachment:
                key = uid_to_file_key(uid)
                if key == 'video':
                    continue
            abs_filename.parent.mkdir(exist_ok=True, parents=True)
            coro = do_download(uid, abs_filename, url)
            await waiters.spawn(coro)

        async for job in waiters:
            if job.exception is not None:
                raise job.exception
            result = job.result
            if result is None:
                continue
            uid, abs_filename, dl_result = result
            coro = complete_download(uid, abs_filename, dl_result)
            await completion_waiters.spawn(coro)

        try:
            await completion_waiters
        except Exception as exc:
            logger.exception(exc)
            raise
        att_dir = temp_dir / 'attachments'
        if att_dir.exists():
            att_dir.rmdir()
        temp_dir.rmdir()

    async def download_feed_items(self, max_items: int):
        self.legistar_data.root_dir.mkdir(exist_ok=True)
        waiters = JobWaiters(self.scheduler)
        keys: list[LegistarFileKey] = ['agenda', 'minutes', 'agenda_packet']
        count = 0
        for guid in self.legistar_data.detail_results.keys():
            detail_item = self.legistar_data[guid]
            if not detail_item.is_final:
                continue
            guid = detail_item.feed_guid
            files = self.legistar_data.get_or_create_files(guid)
            if files.get_is_complete(self.legistar_data, keys):
                continue
            await waiters.spawn(self.download_feed_item(guid))
            count += 1
            if count >= max_items:
                break
        logger.success(f'all feed items queued: {count=}')
        try:
            await waiters
        finally:
            self.legistar_data.save(self.data_filename)

    def check_files(self):
        self.legistar_data.ensure_unique_item_folders()

        for clip_id, detail_result in self.legistar_data.iter_guid_matches():
            clip = self.clips[clip_id]
            filename = clip.get_file_path('agenda_packet', absolute=True)
            if detail_result.links.agenda_packet is None:
                continue
            if not filename.exists():
                continue
            meta = clip.files.get_metadata('agenda_packet')
            assert meta is not None
            assert filename.stat().st_size == meta.content_length

        asset_paths: set[Path] = set()
        for files in self.legistar_data.files.values():
            for t in files.iter_url_paths_uid(self.legistar_data):
                if not t.complete:
                    assert not t.filename.exists()
                    continue
                file_obj = files.resolve_uid(t.key)
                assert file_obj is not None
                meta = file_obj.metadata
                assert t.filename.exists()
                assert t.filename.stat().st_size == meta.content_length
                assert t.filename not in asset_paths
                asset_paths.add(t.filename)

        root_dir = self.legistar_data.root_dir
        local_files: set[Path] = set()
        if not root_dir.exists():
            assert not len(asset_paths)
        else:
            for p in root_dir.glob('**/*.pdf'):
                if p.name.startswith('._'):
                    continue
                assert p in asset_paths, f'untracked filename: {p}'
                local_files.add(p)
            assert len(local_files) == len(asset_paths)
            assert local_files == asset_paths



@logger.catch
async def amain(
    config: Config,
    max_clips: int = 0,
    check_only: bool = False,
    allow_updates: bool = False,
    strip_pdf_links: bool = False
):
    if not config.legistar.out_dir_abs.exists():
        config.legistar.out_dir_abs.mkdir(parents=True)
    clips = ClipCollection.load(config.data_file)
    client = Client(
        clips=clips,
        config=config,
        allow_updates=allow_updates,
        strip_pdf_links=strip_pdf_links,
    )
    client.check_files()
    if check_only:
        return client
    async with client:
        await client.parse_all()
        if max_clips > 0:
            await client.download_feed_items(max_items=max_clips)
    return client

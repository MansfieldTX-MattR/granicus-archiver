from __future__ import annotations
from typing import ClassVar, Self, TypedDict, NotRequired, Unpack, TYPE_CHECKING
from pathlib import Path
import tempfile

from aiohttp import ClientSession, ClientTimeout, ServerTimeoutError
import aiojobs
import aiofile
from yarl import URL
from loguru import logger

if TYPE_CHECKING:
    from ..config import Config
from ..client import DownloadError
from ..utils import JobWaiters
from ..model import ClipCollection, Clip, FileMeta
from .rss_parser import Feed, FeedItem, ParseError, GUID
from .model import (
    LegistarFileKey, AttachmentName, LegistarData,
    DetailPageResult, IncompleteItemError,
)



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
            meta = self._meta = FileMeta.from_headers(resp.headers)
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
        allow_updates: bool = False
    ) -> None:
        self.clips = clips
        self.data_filename = config.legistar.data_file
        self.feed_urls = config.legistar.feed_urls
        self.legistar_category_maps = config.legistar.category_maps
        self.allow_updates = allow_updates
        root_dir = config.legistar.out_dir
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
        self._temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir = Path(self._temp_dir.name).resolve()

    async def close(self) -> None:
        try:
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
        feed = Feed.from_feed(content, category_maps=self.legistar_category_maps)
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
            except IncompleteItemError:
                logger.warning(f'incomplete item: {feed_item.guid=}, {feed_item.category=}, {feed_item.title=}')
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
                    item_files[file_key] = None
                    if file_key in item_files.metadata:
                        actions.append(f'del metadata["{file_key}"]')
                        # del item_files.metadata[file_key]
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
                    if not parsed_item.is_final:
                        logger.warning(f'existing item not final: {feed_item.guid=}, {feed_item.category=}, {feed_item.title=}')
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
                logger.debug(f'match item from feed: {clip.unique_name=}, {item.page_url=}')
            if item.feed_guid in self.legistar_data.matched_guids.values():
                logger.warning(f'ambiguous feed match: {clip.unique_name=}, {item.page_url=}')
                continue
            self.legistar_data.add_guid_match(clip.id, item.feed_guid)

    def get_clip_feed_match(self, clip: Clip) -> DetailPageResult|None:
        feed_item: FeedItem|None = None
        for feed in self.all_feeds.values():
            try:
                feed_item = feed.find_clip_match(clip)
            except ParseError:
                feed_item = None
            if feed_item is None:
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
        waiters = JobWaiters(self.download_scheduler)
        temp_dir = self.temp_dir / guid
        temp_dir.mkdir()
        chunk_size = 16384

        @logger.catch
        async def do_download(key: LegistarFileKey, abs_filename: Path, url: URL):
            temp_filename = temp_dir / abs_filename.name
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
                return
            logger.debug(f'download complete for "{url}"')
            temp_filename.rename(abs_filename)
            self.legistar_data.set_file_complete(guid, key, dl.meta)

        @logger.catch
        async def download_attachment(name: AttachmentName, abs_filename: Path, url: URL):
            temp_filename = temp_dir / 'attachments' / abs_filename.name
            temp_filename.parent.mkdir(exist_ok=True)
            logger.debug(f'downloading attachment "{url}"')
            try:
                dl = await self.downloader.download(
                    url=url, filename=temp_filename, chunk_size=chunk_size,
                    timeout=ClientTimeout(total=60*60, sock_connect=60, sock_read=60),
                )
            except ServerTimeoutError as exc:
                if temp_filename.exists():
                    temp_filename.unlink()
                logger.exception(exc)
                return
            logger.debug(f'download complete for attachment "{url}"')
            temp_filename.rename(abs_filename)
            self.legistar_data.set_attachment_complete(guid, name, dl.meta)

        detail_item = self.legistar_data[guid]
        p = self.legistar_data.get_folder_for_item(detail_item)
        logger.info(f'downloading files for {p.name}')
        it = self.legistar_data.iter_incomplete_url_paths(guid)
        for key, abs_filename, url in it:
            if key == 'video':
                continue
            abs_filename.parent.mkdir(exist_ok=True, parents=True)
            coro = do_download(key, abs_filename, url)
            await waiters.spawn(coro)

        for name, abs_filename, url in self.legistar_data.iter_incomplete_attachments(guid):
            abs_filename.parent.mkdir(exist_ok=True, parents=True)
            await waiters.spawn(download_attachment(name, abs_filename, url))

        try:
            await waiters
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
            for t in files.iter_url_paths(self.legistar_data):
                if not t.complete:
                    continue
                meta = files.get_metadata(t.key)
                assert meta is not None, f'{files.guid=}, {t.key=}'
                assert t.filename.exists()
                assert t.filename.stat().st_size == meta.content_length
                assert t.filename not in asset_paths
                asset_paths.add(t.filename)
            for t in files.iter_attachments(self.legistar_data):
                if not t.complete:
                    continue
                attachment = files.attachments[t.key]
                assert attachment is not None
                assert attachment.filename.exists()
                assert attachment.filename.stat().st_size == attachment.metadata.content_length
                asset_paths.add(attachment.filename)

        root_dir = self.legistar_data.root_dir
        local_files: set[Path] = set()
        if not root_dir.exists():
            assert not len(asset_paths)
        else:
            for p in root_dir.glob('**/*.pdf'):
                assert p in asset_paths, f'untracked filename: {p}'
                local_files.add(p)
            assert len(local_files) == len(asset_paths)
            assert local_files == asset_paths



@logger.catch
async def amain(
    config: Config,
    max_clips: int = 0,
    check_only: bool = False,
    allow_updates: bool = False
):
    clips = ClipCollection.load(config.data_file)
    client = Client(
        clips=clips,
        config=config,
        allow_updates=allow_updates,
    )
    client.check_files()
    if check_only:
        return client
    async with client:
        await client.parse_all()
        if max_clips > 0:
            await client.download_feed_items(max_items=max_clips)
    return client

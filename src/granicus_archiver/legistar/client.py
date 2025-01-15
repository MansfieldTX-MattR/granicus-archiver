from __future__ import annotations
from typing import TypeVar, Generic, ClassVar, Self, Iterator, TYPE_CHECKING, cast

from abc import ABC, abstractmethod
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
from ..utils import (
    JobWaiters,
    CompletionCounts,
    remove_pdf_links,
    is_same_filesystem,
)
from ..model import ClipCollection, Clip, FileMeta
from .types import (
    GUID, REAL_GUID, LegistarFileKey, LegistarFileUID, NoClip,
    _GuidT, _ItemT,
)
from .rss_parser import Feed, FeedItem, ParseError, LegistarThinksRSSCanPaginateError, GuidCompare
from .model import (
    LegistarData, DetailPageResult, IncompleteItemError, HiddenItemError,
    is_attachment_uid, uid_to_file_key, make_path_legal,
    FilePathURLComplete, AbstractFile, AbstractLegistarModel, UpdateResult,
)
from ..downloader import Downloader, DownloadResult, StupidZeroContentLengthError


_ModelT = TypeVar('_ModelT', bound=AbstractLegistarModel)


class ClientBase(ABC, Generic[_GuidT, _ItemT, _ModelT]):
    temp_dir: Path
    _temp_dir: tempfile.TemporaryDirectory
    _default_limit: ClassVar[int] = 8
    session: ClientSession
    scheduler: aiojobs.Scheduler
    download_scheduler: aiojobs.Scheduler
    waiter: JobWaiters[_ItemT]
    legistar_data: AbstractLegistarModel[_GuidT, _ItemT]
    incomplete_items: dict[_GuidT, FeedItem]
    incomplete_existing_items: dict[_GuidT, FeedItem]
    guid_collisions: dict[REAL_GUID, tuple[FeedItem, list[str]]]
    ambiguous_matches: dict[_GuidT, tuple[_ItemT, Clip]]
    def __init__(
        self,
        clips: ClipCollection,
        config: Config,
        allow_updates: bool = False,
        strip_pdf_links: bool = False
    ) -> None:
        self.clips = clips
        self.config = config
        self.feed_urls = config.legistar.feed_urls
        self.legistar_category_maps = config.legistar.category_maps
        self.allow_updates = allow_updates
        self.strip_pdf_links = strip_pdf_links
        self.all_feeds: dict[str, Feed] = {}
        self.incomplete_items = {}
        self.incomplete_existing_items = {}
        self.guid_collisions = {}
        self.ambiguous_matches = {}
        self.completion_counts = CompletionCounts(enable_log=True)
        mcls = self._get_model_cls()
        if self.data_filename.exists():
            self.legistar_data = mcls.load(self.data_filename, root_dir=self.root_dir)
        else:
            self.legistar_data = mcls(root_dir=self.root_dir)

    @classmethod
    @abstractmethod
    def _get_model_cls(cls) -> type[_ModelT]:
        raise NotImplementedError

    @property
    @abstractmethod
    def root_dir(self) -> Path:
        raise NotImplementedError

    @property
    @abstractmethod
    def data_filename(self) -> Path:
        raise NotImplementedError


    async def open(self) -> None:
        self.session = ClientSession()
        self.scheduler = aiojobs.Scheduler(limit=self._default_limit, pending_limit=1)
        self.waiter = JobWaiters(self.scheduler)
        self.download_scheduler = aiojobs.Scheduler(limit=4)
        self.downloader = Downloader(session=self.session, scheduler=self.download_scheduler)
        self.completion_scheduler = aiojobs.Scheduler(limit=8, pending_limit=1)
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

    async def parse_detail_page(self, feed_item: FeedItem) -> _ItemT:
        logger.debug(f'parse page: {feed_item.link}')
        async with self.session.get(feed_item.link) as resp:
            if not resp.ok:
                resp.raise_for_status()
            resp_text = await resp.text()
            return self._create_detail_page(resp_text, feed_item)

    @abstractmethod
    def _create_detail_page(self, html_str: str|bytes, feed_item: FeedItem) -> _ItemT:
        raise NotImplementedError

    @abstractmethod
    def _add_detail_page_to_model(self, item: _ItemT) -> None:
        raise NotImplementedError

    @abstractmethod
    def _id_from_feed_item(self, feed_item: FeedItem) -> _GuidT:
        raise NotImplementedError

    @abstractmethod
    def _id_from_item(self, item: _ItemT) -> _GuidT:
        raise NotImplementedError

    @abstractmethod
    def _get_model_item(self, key: _GuidT) -> _ItemT|None:
        raise NotImplementedError

    @abstractmethod
    async def check_item_update(self, feed_item: FeedItem) -> tuple[_ItemT|None, bool, bool]:
        raise NotImplementedError

    @abstractmethod
    def _iter_model_items(self) -> Iterator[_ItemT]:
        raise NotImplementedError

    @abstractmethod
    def _get_folder_for_item(self, item: _ItemT) -> Path:
        raise NotImplementedError

    @abstractmethod
    def _item_needs_download(self, item: _ItemT) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _iter_files_for_item(self, item: _ItemT) -> Iterator[FilePathURLComplete[LegistarFileUID]]:
        raise NotImplementedError

    @abstractmethod
    def _set_file_download_complete(
        self,
        item: _ItemT,
        uid: LegistarFileUID,
        metadata: FileMeta,
        pdf_links_removed: bool
    ) -> AbstractFile:
        raise NotImplementedError

    @abstractmethod
    async def handle_item_update(
        self,
        feed_item: FeedItem,
        existing_item: _ItemT,
        apply_actions: bool,
        parsed_item: _ItemT|None = None
    ) -> tuple[bool, list[str]]:
        raise NotImplementedError

    @abstractmethod
    def check_files(self):
        raise NotImplementedError

    async def do_page_parse(self, feed_item: FeedItem) -> _ItemT|None:
        try:
            result = await self.parse_detail_page(feed_item)
        except HiddenItemError:
            result = None
        except IncompleteItemError:
            logger.warning(f'incomplete item: {feed_item.to_str()}')
            self.incomplete_items[self._id_from_feed_item(feed_item)] = feed_item
            result = None
        return result

    async def parse_detail_pages(self, name: str, feed: Feed) -> tuple[str, bool]:
        logger.debug(f'parsing detail pages for "{name}"')
        changed = False
        try:
            for feed_item in feed:
                if feed_item.is_future:
                    continue
                parsed_item, exists, _changed = await self.check_item_update(feed_item)
                if _changed:
                    changed = True
                if exists:
                    continue
                if parsed_item is not None:
                    if parsed_item.is_hidden:
                        continue
                    if not parsed_item.agenda_final and not parsed_item.is_in_past:
                        logger.warning(f'existing item not final: {feed_item.to_str()}')
                        self.incomplete_existing_items[self._id_from_feed_item(feed_item)] = feed_item
                    continue
                result = await self.do_page_parse(feed_item)
                if result is None:
                    continue

                assert parsed_item is None
                self._add_detail_page_to_model(result)
                changed = True
        except Exception as exc:
            logger.exception(exc)
            raise
        logger.debug('detail wait complete')
        return name, changed

    def match_clips(self) -> bool:
        changed = False
        for clip in self.clips:
            if not self.legistar_data.is_clip_id_available(clip.id):
                continue
            item = self.legistar_data.find_match_for_clip_id(clip.id)
            if item is NoClip:
                continue
            elif item is None:
                item = self.get_clip_feed_match(clip)
                if item is None:
                    continue
                logger.debug(f'match item from feed: {clip.unique_name=}, {item.feed_item.to_str()}')
            guid = self._id_from_item(item)
            if self.legistar_data.is_guid_matched(guid):
                logger.warning(f'ambiguous feed match: {clip.unique_name=}, {item.feed_item.to_str()}')
                self.ambiguous_matches[guid] = (item, clip)
                continue
            self.legistar_data.add_guid_match(clip.id, guid)
            changed = True
        return changed

    def get_clip_feed_match(self, clip: Clip) -> _ItemT|None:
        feed_item: FeedItem|None = None
        for feed in self.all_feeds.values():
            try:
                feed_item = feed.find_clip_match(clip)
            except ParseError:
                feed_item = None
            if feed_item is None or feed_item.is_future:
                continue
            parsed_item = self._get_model_item(self._id_from_feed_item(feed_item))
            if parsed_item is None:
                continue
            if parsed_item.is_draft or parsed_item.is_hidden:
                return None
            return parsed_item


    async def parse_feeds(self) -> bool:
        feed_waiters = JobWaiters[tuple[str, Feed]](self.scheduler)
        for name, feed_url in self.feed_urls.items():
            await feed_waiters.spawn(self.parse_feed(name, feed_url))

        async for result in feed_waiters.as_completed():
            result.raise_exc()
            name, feed = result.result
            self.all_feeds[name] = feed

        changed = False
        for name, feed in self.all_feeds.items():
            _, _changed = await self.parse_detail_pages(name, feed)
            logger.success(f'feed parse complete for "{name}"')
            if _changed:
                self.legistar_data.save(self.data_filename)
                changed = True
        return changed

    async def parse_all(self):
        self.legistar_data.ensure_no_future_items()
        items_changed = await self.parse_feeds()
        self.legistar_data.ensure_unique_item_folders()
        clips_changed = self.match_clips()
        if items_changed or clips_changed:
            self.legistar_data.save(self.data_filename)

    def get_warning_items(self) -> str:
        """Get all parse warnings as a text block
        """
        lines: list[str] = ['']
        lines.append('INCOMPLETE ITEMS:')
        for feed_item in self.incomplete_items.values():
            lines.append(f'{feed_item.to_str()}')
        lines.append('')

        lines.append('EXISTING ITEMS NOT FINAL:')
        for feed_item in self.incomplete_existing_items.values():
            lines.append(feed_item.to_str())
        lines.append('')

        lines.append('REAL GUID COLLISIONS:')
        for feed_item, update_actions in self.guid_collisions.values():
            lines.append(f'{feed_item.to_str()}:')
            for action in update_actions:
                lines.append(f'    {action}')
        lines.append('')

        lines.append('AMBIGUOUS CLIP MATCHES:')
        for detail_result, clip in self.ambiguous_matches.values():
            lines.append(f'{detail_result.feed_item.to_str()} --> {clip.unique_name}')
        lines.append('')

        return '\n'.join(lines)

    def get_incomplete_csv(self) -> str:
        items = list(self.incomplete_items.values())
        items.extend(list(self.incomplete_existing_items.values()))
        return FeedItem.to_csv(*items)

    # @logger.catch
    async def download_feed_item(self, guid: _GuidT) -> None:
        loop = asyncio.get_running_loop()
        waiters = JobWaiters[tuple[LegistarFileUID, Path, DownloadResult]|None](self.download_scheduler)
        completion_waiters = JobWaiters(self.completion_scheduler)
        temp_dir = self.temp_dir / guid
        temp_dir.mkdir()
        chunk_size = 16384
        detail_item = self._get_model_item(guid)
        assert detail_item is not None

        @logger.catch(reraise=True)
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

        @logger.catch(reraise=True)
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
                file_obj = self._set_file_download_complete(
                    detail_item, uid, meta, pdf_links_removed=self.strip_pdf_links,
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
            file_obj = self._set_file_download_complete(
                detail_item, uid, meta, pdf_links_removed=self.strip_pdf_links,
            )
            assert file_obj.pdf_links_removed
            assert file_obj.metadata.content_length == meta.content_length

        p = self._get_folder_for_item(detail_item)
        logger.info(f'downloading files for {p.name}')
        it = self._iter_files_for_item(detail_item)
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
        self.completion_counts.num_completed += 1

    async def download_feed_items(self, max_items: int):
        self.completion_counts.max_items = max_items
        self.completion_counts.reset()
        self.legistar_data.root_dir.mkdir(exist_ok=True)
        waiters = JobWaiters(self.scheduler)
        for detail_item in self._iter_model_items():
            if not detail_item.can_download:
                continue
            if not self._item_needs_download(detail_item):
                continue
            guid = self._id_from_item(detail_item)
            await waiters.spawn(self.download_feed_item(guid))
            self.completion_counts.num_queued += 1
            if self.completion_counts.full:
                break
        logger.success(f'all feed items queued: {self.completion_counts}')
        try:
            await waiters
            logger.info(f'{self.completion_counts=}')
        finally:
            self.legistar_data.save(self.data_filename)



class Client(ClientBase[GUID, DetailPageResult, LegistarData]):
    @classmethod
    def _get_model_cls(cls) -> type[LegistarData]:
        return LegistarData

    @property
    def root_dir(self) -> Path:
        return self.config.legistar.out_dir

    @property
    def data_filename(self) -> Path:
        return self.config.legistar.data_file

    @property
    def _legistar_data(self) -> LegistarData:
        return cast(LegistarData, self.legistar_data)

    def get_item_count(self) -> int:
        return len(self.legistar_data)

    def get_file_count(self) -> int:
        root_dir = self.legistar_data.root_dir
        return len([p for p in root_dir.glob('**/*.pdf')])

    def _create_detail_page(self, html_str: str | bytes, feed_item: FeedItem) -> DetailPageResult:
        return DetailPageResult.from_html(
            html_str=html_str,
            feed_item=feed_item,
        )

    def _id_from_feed_item(self, feed_item: FeedItem) -> GUID:
        return feed_item.guid

    def _get_model_item(self, key: GUID) -> DetailPageResult|None:
        return self.legistar_data.get(key)

    def _add_detail_page_to_model(self, item: DetailPageResult) -> None:
        self.legistar_data.add_detail_result(item)

    def _id_from_item(self, item: DetailPageResult) -> GUID:
        return item.feed_guid

    def _iter_model_items(self) -> Iterator[DetailPageResult]:
        yield from self.legistar_data

    def _get_folder_for_item(self, item: DetailPageResult) -> Path:
        return self.legistar_data.get_folder_for_item(item)

    def _item_needs_download(self, item: DetailPageResult) -> bool:
        if not item.can_download:
            return False
        keys: list[LegistarFileKey] = ['agenda', 'minutes', 'agenda_packet']
        guid = item.feed_guid
        files = self._legistar_data.get_or_create_files(guid)
        if files.get_is_complete(self._legistar_data, keys):
            return False
        return True

    def _iter_files_for_item(self, item: DetailPageResult) -> Iterator[FilePathURLComplete[LegistarFileUID]]:
        return self._legistar_data.iter_url_paths_uid(item.feed_guid)

    def _set_file_download_complete(
        self,
        item: DetailPageResult,
        uid: LegistarFileUID,
        metadata: FileMeta,
        pdf_links_removed: bool
    ) -> AbstractFile:
        return self._legistar_data.set_uid_complete(
            item.feed_guid, uid, metadata, pdf_links_removed=pdf_links_removed,
        )

    async def handle_item_update(
        self,
        feed_item: FeedItem,
        existing_item: DetailPageResult,
        apply_actions: bool,
        parsed_item: DetailPageResult|None = None
    ) -> tuple[bool, list[str]]:
        # NOTE: `existing_item` is a copy of the item from the model.
        #      Modifications to it happen here and whether they're propagated
        #      back happens in the caller (`check_item_update`).
        if parsed_item is not None:
            result = parsed_item
        else:
            result = await self.do_page_parse(feed_item)
        if result is None:
            return False, []
        update_result = existing_item.update(result)
        changed, file_keys, attachment_keys, changed_attrs = update_result
        assert changed_attrs is not None
        if not changed:
            assert not len(changed_attrs)
            return False, []
        item_files = self._legistar_data.files.get(existing_item.feed_guid)
        link_update: UpdateResult|None = changed_attrs.pop('links', None)
        actions = [
            f'setattr(obj, "{key}", {val})'
            for key, val in changed_attrs.items()
        ]
        if link_update is not None:
            # `existing_item.links` is also a copy, so no need to modify it.
            # We're just logging the changes that would take place.
            assert link_update.changed
            for link_key in link_update.link_keys:
                new_value = getattr(existing_item.links, link_key)
                actions.append(f'setattr(obj.links, "{link_key}", {new_value})')
            for att_key in link_update.attachment_keys:
                new_value = existing_item.links.attachments[att_key]
                actions.append(f'obj.links.attachments["{att_key}"] = {new_value}')

        if item_files is not None:
            for file_key in file_keys:
                file_obj = item_files[file_key]
                if file_obj is None:
                    continue
                filename = file_obj.filename
                actions.append(f'rm {filename}')
                actions.append(f'del files["{file_key}"]')
                if apply_actions:
                    # This and the attachment deletion below are the only places
                    # where we actually change anything.
                    # Since the links changed, remove the files and the saved file meta.
                    filename.unlink()
                    del item_files.files[file_key]
            for att_name in attachment_keys:
                attachment = item_files.attachments.get(att_name)
                if attachment is None:
                    continue
                actions.append(f'rm {attachment.filename}')
                actions.append(f'del attachments["{att_name}"]')
                if apply_actions:
                    attachment.filename.unlink()
                    del item_files.attachments[att_name]
        dirs_changed, dir_actions = self.relocate_files(
            feed_item, existing_item, apply_actions, parsed_item,
        )
        actions.extend(dir_actions)

        assert len(actions) > 0
        return True, actions

    def relocate_files(
        self,
        feed_item: FeedItem,
        existing_item: DetailPageResult,
        apply_actions: bool,
        parsed_item: DetailPageResult|None = None
    ) -> tuple[bool, list[str]]:
        actions: list[str] = []
        item_files = self._legistar_data.files.get(existing_item.feed_guid)
        if item_files is None:
            return False, actions
        dirs_changed = False
        new_dir_name = existing_item.get_unique_folder().name
        existing_base_dir = self.legistar_data.get_folder_for_item(existing_item.feed_guid)
        base_dir = existing_base_dir.with_name(new_dir_name)
        att_dir: Path|None = None
        existing_att_dir: Path|None = None

        if existing_base_dir == base_dir:
            return False, actions
        if existing_base_dir.exists():
            assert not base_dir.exists()
            actions.append(f'mkdir "{base_dir}"')
            if apply_actions:
                base_dir.mkdir()
        for file_key, existing_fobj in item_files:
            if existing_fobj is None:
                continue
            logger.debug(f'{existing_fobj.filename=}')
            if existing_fobj.filename.parent == base_dir:
                continue
            dirs_changed = True
            new_filename = base_dir / existing_fobj.filename.name
            assert not new_filename.exists()
            actions.append(f'mv "{existing_fobj.filename}" "{new_filename}"')
            if apply_actions:
                existing_fobj.filename.rename(new_filename)
                existing_fobj.filename = new_filename
        for att_key, att_file in item_files.attachments.items():
            if att_file is None:
                continue
            if att_dir is None:
                existing_att_dir = att_file.filename.parent
                att_dir = base_dir / item_files.build_attachment_filename(att_key).parent.name
                if att_dir != existing_att_dir and not att_dir.exists():
                    actions.append(f'mkdir "{att_dir}"')
                    if apply_actions:
                        att_dir.mkdir(parents=True)
            if att_file.filename.parent == att_dir:
                continue
            dirs_changed = True
            new_filename = att_dir / att_file.filename.name
            assert not new_filename.exists()
            actions.append(f'mv "{att_file.filename}" "{new_filename}"')
            if apply_actions:
                att_file.filename.rename(new_filename)
                att_file.filename = new_filename
        if dirs_changed:
            assert existing_base_dir is not None
            if existing_att_dir is not None and existing_att_dir.exists():
                actions.append(f'rmdir "{existing_att_dir}"')
                if apply_actions:
                    existing_att_dir.rmdir()
            if existing_base_dir.exists():
                actions.append(f'rmdir "{existing_base_dir}"')
                if apply_actions:
                    existing_base_dir.rmdir()
        return dirs_changed, actions

    async def check_item_update(self, feed_item: FeedItem) -> tuple[DetailPageResult|None, bool, bool]:
        changed = False
        parsed_item = self._legistar_data.get(feed_item.guid)
        exists = parsed_item is not None
        if parsed_item is not None:
            return parsed_item, exists, changed

        similar_item = self._legistar_data.get_by_real_guid(feed_item.real_guid)
        exists = similar_item is not None
        if similar_item is None:
            return parsed_item, exists, changed
        if similar_item.last_fake_stupid_guid is not None:
            # We're here because the fake guid doesn't exist in the model.
            # Ensure the real guid of the feed item is newer than what's stored
            # from the last update.
            assert GuidCompare(feed_item.guid) >= similar_item.last_fake_stupid_guid
            return parsed_item, exists, changed

        if self.allow_updates:
            logger.info(f'Updating item "{similar_item.feed_guid}"')
            tmp_item = similar_item.copy()
            _changed, actions = await self.handle_item_update(
                feed_item, tmp_item, apply_actions=True,
            )
            self.guid_collisions[feed_item.real_guid] = (feed_item, actions)
            logger.info(f'Update item: {actions=}')
            if _changed:
                tmp_item.last_fake_stupid_guid = feed_item.guid
                self._legistar_data.detail_results[similar_item.feed_guid] = tmp_item
                changed = True
            elif similar_item.last_fake_stupid_guid != feed_item.guid:
                similar_item.last_fake_stupid_guid = feed_item.guid
                changed = True
            assert self._legistar_data.detail_results[similar_item.feed_guid].last_fake_stupid_guid == feed_item.guid
        else:
            _changed, actions = await self.handle_item_update(
                feed_item, similar_item.copy(), apply_actions=False,
            )
            if _changed:
                logger.warning(f'existing item needs update: {feed_item.to_str()}, {actions=}')
                self.guid_collisions[feed_item.real_guid] = (feed_item, actions)
        return parsed_item, exists, changed

    def check_files(self):
        self._legistar_data.ensure_unique_item_folders()

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
        illegal_dirs: set[Path] = set()
        for files in self._legistar_data.files.values():
            for t in files.iter_url_paths_uid(self._legistar_data):
                legal_dir = make_path_legal(t.filename.parent, is_dir=True)
                if legal_dir != t.filename.parent:
                    if t.filename.parent not in illegal_dirs:
                        logger.warning(f'Illegal dir: {t.filename.parent}, {legal_dir=}')
                    illegal_dirs.add(t.filename.parent)
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
        assert not len(illegal_dirs)

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




@logger.catch(reraise=True)
async def amain(
    config: Config,
    max_clips: int = 0,
    check_only: bool = False,
    allow_updates: bool = False,
    strip_pdf_links: bool = False
) -> Client:
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

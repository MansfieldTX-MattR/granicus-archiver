from __future__ import annotations
from typing import Iterator, TYPE_CHECKING, cast
from pathlib import Path

from loguru import logger

from granicus_archiver.legistar.model import AbstractFile, FilePathURLComplete
from granicus_archiver.model import FileMeta

if TYPE_CHECKING:
    from ..config import Config

from ..model import ClipCollection
from .types import REAL_GUID, LegistarFileUID
from .rss_parser import FeedItem
from .model import (
    make_path_legal, is_attachment_uid, uid_to_file_key, file_key_to_uid,
    attachment_name_to_uid, FilePathURLComplete, AbstractFile, UpdateResult,
)
from .guid_model import (
    RGuidLegistarData, RGuidDetailResult,
)
from .client import ClientBase


class RGClient(ClientBase[REAL_GUID, RGuidDetailResult, RGuidLegistarData]):
    def __init__(
        self,
        clips: ClipCollection,
        config: Config,
        allow_updates: bool = False,
        strip_pdf_links: bool = False
    ) -> None:
        super().__init__(clips, config, allow_updates, strip_pdf_links)

    @property
    def root_dir(self) -> Path:
        return RGuidLegistarData._get_root_dir(self.config)

    @property
    def data_filename(self) -> Path:
        return RGuidLegistarData._get_data_file(self.config)

    @property
    def _legistar_data(self) -> RGuidLegistarData:
        return cast(RGuidLegistarData, self.legistar_data)

    @classmethod
    def _get_model_cls(cls) -> type[RGuidLegistarData]:
        return RGuidLegistarData

    def _create_detail_page(self, html_str: str|bytes, feed_item: FeedItem) -> RGuidDetailResult:
        return self._legistar_data._build_item(html_str, feed_item)

    def _add_detail_page_to_model(self, item: RGuidDetailResult) -> None:
        self._legistar_data.add_item(item)

    def _id_from_feed_item(self, feed_item: FeedItem) -> REAL_GUID:
        return feed_item.real_guid

    def _id_from_item(self, item: RGuidDetailResult) -> REAL_GUID:
        return item.real_guid

    def _get_model_item(self, key: REAL_GUID) -> RGuidDetailResult|None:
        return self.legistar_data.get(key)

    def _get_item_by_real_guid(self, key: REAL_GUID) -> RGuidDetailResult | None:
        return self.legistar_data.get(key)

    def _iter_model_items(self) -> Iterator[RGuidDetailResult]:
        yield from self.legistar_data

    def _get_folder_for_item(self, item: RGuidDetailResult) -> Path:
        return item.files.full_base_dir

    def _iter_files_for_item(self, item: RGuidDetailResult) -> Iterator[FilePathURLComplete[LegistarFileUID]]:
        for uid, url in item.links.iter_uids():
            if url is None:
                continue
            if not is_attachment_uid(uid) and uid_to_file_key(uid) == 'video':
                continue
            filename = item.files.get_file_path(uid, absolute=True)
            if uid in item.files:
                assert filename.exists()
                complete = True
            else:
                complete = False
            yield FilePathURLComplete(
                key=uid, filename=filename, url=url, complete=complete,
            )

    def _item_needs_download(self, item: RGuidDetailResult) -> bool:
        incomplete = [not r.complete for r in self._iter_files_for_item(item)]
        return any(incomplete)

    def _set_file_download_complete(
        self,
        item: RGuidDetailResult,
        uid: LegistarFileUID,
        metadata: FileMeta,
        pdf_links_removed: bool
    ) -> AbstractFile:
        return item.files.add_file(uid, meta=metadata, pdf_links_removed=pdf_links_removed)

    async def handle_item_update(
        self,
        feed_item: FeedItem,
        existing_item: RGuidDetailResult,
        apply_actions: bool,
        parsed_item: RGuidDetailResult|None = None
    ) -> tuple[bool, list[str]]:
        # NOTE: This is untested so the mutations are currently disabled
        if parsed_item is not None:
            result = parsed_item
        else:
            result = await self.do_page_parse(feed_item)
        if result is None:
            return False, []
        update_result = existing_item.update(result)
        changed, file_keys, attachment_keys, files, changed_attrs = update_result
        if not changed:
            assert not len(changed_attrs)
            return False, []
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
        link_uids = set[LegistarFileUID]()
        link_uids |= set([file_key_to_uid(k) for k in file_keys])
        link_uids |= set([attachment_name_to_uid(n) for n in attachment_keys])
        for uid in link_uids:
            file_obj = existing_item.files.get(uid)
            if file_obj is None:
                continue
            filename = file_obj.filename
            actions.append(f'rm {filename}')
            actions.append(f'del files["{uid}"]')
            if apply_actions:
                filename.unlink()
                del existing_item.files[uid]
                # del item_files.files[file_key]
        return True, actions

    async def check_item_update(self, feed_item: FeedItem) -> tuple[RGuidDetailResult|None, bool, bool]:
        guid = feed_item.real_guid
        existing_item = self.legistar_data.get(guid)
        exists = existing_item is not None
        changed = False
        if existing_item is None:
            return None, exists, changed
        if existing_item.feed_guid == feed_item.guid:
            return existing_item, exists, changed
        if existing_item.guid_compare >= feed_item.guid:
            return existing_item, exists, changed
        parsed_item = await self.parse_detail_page(feed_item)
        assert parsed_item.guid_compare >= existing_item.guid_compare
        # update_result = existing_item.update(parsed_item)

        if self.allow_updates:
            logger.info(f'Updating item "{existing_item.feed_guid}"')
            tmp_item = existing_item.copy()
            _changed, actions = await self.handle_item_update(
                feed_item, tmp_item, apply_actions=True, parsed_item=parsed_item,
            )
            logger.info(f'Update item: {actions=}')
            if _changed:
                assert tmp_item.links == parsed_item.links
                tmp_item.feed_guid = feed_item.guid
                self._legistar_data.detail_results[existing_item.real_guid] = tmp_item
                changed = True
        else:
            tmp_item = existing_item.copy()
            _changed, actions = await self.handle_item_update(
                feed_item, tmp_item, apply_actions=False, parsed_item=parsed_item,
            )
            if _changed:
                # return parsed_item
                logger.warning(f'existing item needs update: {feed_item.to_str()}, {actions=}')
                self.guid_collisions[feed_item.real_guid] = (feed_item, actions)
            else:
                assert existing_item.links == parsed_item.links
                existing_item.feed_guid = feed_item.guid
                changed = True
        return existing_item, exists, changed

    def check_files(self):
        # self.legistar_data.ensure_unique_item_folders()

        # for clip_id, detail_result in self.legistar_data.iter_guid_matches():
        #     clip = self.clips[clip_id]
        #     filename = clip.get_file_path('agenda_packet', absolute=True)
        #     if detail_result.links.agenda_packet is None:
        #         continue
        #     if not filename.exists():
        #         continue
        #     meta = clip.files.get_metadata('agenda_packet')
        #     assert meta is not None
        #     assert filename.stat().st_size == meta.content_length

        asset_paths: set[Path] = set()
        illegal_dirs: set[Path] = set()
        for item in self.legistar_data:
            for t in self._iter_files_for_item(item):
                legal_dir = make_path_legal(t.filename.parent, is_dir=True)
                if legal_dir != t.filename.parent:
                    if t.filename.parent not in illegal_dirs:
                        logger.warning(f'Illegal dir: {t.filename.parent}, {legal_dir=}')
                    illegal_dirs.add(t.filename.parent)
                if not t.complete:
                    assert not t.filename.exists()
                    continue
                file_obj = item.files[t.key]
                assert file_obj is not None
                meta = file_obj.metadata
                assert t.filename.exists()
                assert t.filename.stat().st_size == meta.content_length
                assert t.filename not in asset_paths
                asset_paths.add(t.filename)
        assert not len(illegal_dirs)

        # root_dir = self.legistar_data.root_dir
        local_files: set[Path] = set()
        if not self.root_dir.exists():
            assert not len(asset_paths)
        else:
            for p in self.root_dir.glob('**/*.pdf'):
                if p.name.startswith('._'):
                    continue
                assert p in asset_paths, f'untracked filename: {p}'
                local_files.add(p)
            extra_local = asset_paths - local_files
            extra_assets = local_files - asset_paths
            assert not len(extra_local)
            assert not len(extra_assets)
            assert len(local_files) == len(asset_paths)
            assert local_files == asset_paths

@logger.catch(reraise=True)
async def amain(
    config: Config,
    max_clips: int = 0,
    check_only: bool = False,
    allow_updates: bool = False,
    strip_pdf_links: bool = False
) -> RGClient:
    # if not config.legistar.out_dir_abs.exists():
    #     config.legistar.out_dir_abs.mkdir(parents=True)
    clips = ClipCollection.load(config.data_file)
    client = RGClient(
        clips=clips,
        config=config,
        allow_updates=allow_updates,
        strip_pdf_links=strip_pdf_links,
    )
    if not client.root_dir.exists():
        client.root_dir.mkdir(parents=True)
    client.check_files()
    if check_only:
        return client
    async with client:
        await client.parse_all()
        if max_clips > 0:
            await client.download_feed_items(max_items=max_clips)
    return client

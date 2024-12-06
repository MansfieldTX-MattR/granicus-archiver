"""Data model using :obj:`"Real" GUID's <.types.REAL_GUID>` as identifiers

This may eventually replace the :mod:`granicus_archiver.legistar.model` module
since its storage method is more robust.
For now however, the two exist in parallel.
"""
from __future__ import annotations
from typing import Literal, Iterator, Self, Any, overload, TYPE_CHECKING
from typing_extensions import NamedTuple
from os import PathLike
from pathlib import Path
import json
from dataclasses import dataclass, field


if TYPE_CHECKING:
    from ..config import Config
from ..model import CLIP_ID, Serializable, FileMeta
from .types import (
    REAL_GUID, LegistarFileKey, AttachmentName, LegistarFileUID,
    NoClipT, NoClip,
)
from .model import (
    LegistarFile as _LegistarFile,
    LegistarFiles,
    AttachmentFile as _AttachmentFile,
    DetailPageResult,
    AbstractLegistarModel,
    is_attachment_uid,
    uid_to_attachment_name,
    attachment_name_to_uid,
    uid_to_file_key,
    file_key_to_uid,
)
from .rss_parser import FeedItem, GuidCompare



@dataclass
class LegistarFile(_LegistarFile):
    """Information for a downloaded file within :attr:`RGuidLegistarFiles.files`
    """
    @property
    def uid(self) -> LegistarFileUID:
        """Unique id for the file
        """
        return file_key_to_uid(self.name)

    @classmethod
    def from_uid(
        cls,
        uid: LegistarFileUID,
        filename: Path,
        metadata: FileMeta,
        pdf_links_removed: bool
    ) -> Self:
        """Create an instance from a :obj:`uid <.types.LegistarFileUID>`
        """
        key = uid_to_file_key(uid)
        return cls(
            name=key, filename=filename, metadata=metadata,
            pdf_links_removed=pdf_links_removed,
        )


@dataclass
class AttachmentFile(_AttachmentFile):
    """Information for a downloaded attachment within :attr:`RGuidLegistarFiles.files`
    """
    @property
    def uid(self) -> LegistarFileUID:
        """Unique id for the file
        """
        return attachment_name_to_uid(self.name)

    @classmethod
    def from_uid(
        cls,
        uid: LegistarFileUID,
        filename: Path,
        metadata: FileMeta,
        pdf_links_removed: bool
    ) -> Self:
        """Create an instance from a :obj:`uid <.types.LegistarFileUID>`
        """
        assert is_attachment_uid(uid)
        key = uid_to_attachment_name(uid)
        return cls(
            name=key, filename=filename, metadata=metadata,
            pdf_links_removed=pdf_links_removed,
        )


def get_file_cls(uid: LegistarFileUID) -> type[LegistarFile|AttachmentFile]:
    if is_attachment_uid(uid):
        return AttachmentFile
    return LegistarFile


class RGuidUpdateResult(NamedTuple):
    """
    """
    changed: bool
    """Whether any changes were made"""
    link_keys: list[LegistarFileKey]
    """Any URL attributes from :class:`DetailPageLinks` that changed"""
    attachment_keys: list[AttachmentName]
    """Any keys in :attr:`DetailPageLinks.attachments` that changed"""
    files: dict[LegistarFileUID, LegistarFile|AttachmentFile]
    attributes: dict[str, Any]
    """Attributes of :class:`DetailPageResult` that changed"""



@dataclass
class RGuidLegistarFiles(Serializable):
    """Collection of files within a :class:`RGuidDetailResult`
    """
    parent: RGuidDetailResult = field(init=False, repr=False, compare=False)
    base_dir: Path
    """Base directory (relative to :attr:`RGuidLegistarData.root_dir`)"""
    files: dict[LegistarFileUID, LegistarFile|AttachmentFile] = field(default_factory=dict)
    """Mapping of :class:`LegistarFile` or :class:`AttachmentFile` objects
    using their :attr:`~LegistarFile.uid` as keys
    """

    @classmethod
    def build_filename(cls, uid: LegistarFileUID) -> Path:
        if is_attachment_uid(uid):
            name = uid_to_attachment_name(uid)
            return cls.build_attachment_filename(name)
            # return Path('attachments') / name
        key = uid_to_file_key(uid)
        return LegistarFiles.build_filename(key)

    @classmethod
    def build_attachment_filename(cls, name: AttachmentName) -> Path:
        return Path('attachments') / f'{name}.pdf'

    @property
    def full_base_dir(self) -> Path:
        """The complete file directory (:attr:`base_dir` prefixed with
        :attr:`RGuidLegistarData.root_dir`)
        """
        return self.parent.parent.root_dir / self.base_dir

    def get_file_path(self, uid: LegistarFileUID, absolute: bool) -> Path:
        """Get the filename for the given *uid*

        Arguments:
            uid: A :obj:`LegistarFileUID`
            absolute: If ``True`` the path will be within the :attr:`full_base_dir`,
                otherwise :attr:`base_dir` is used
        """
        if uid in self:
            filename = self[uid].filename
        else:
            filename = self.build_filename(uid)
        if absolute:
            return self.full_base_dir / filename
        return self.base_dir / filename

    def add_file(
        self,
        uid: LegistarFileUID,
        meta: FileMeta,
        pdf_links_removed: bool
    ) -> LegistarFile|AttachmentFile:
        """Add a file with the given *uid*
        """
        if uid in self:
            raise KeyError(f'uid "{uid}" already exists')
        filename = self.build_filename(uid)
        f_cls = get_file_cls(uid)
        f_obj = f_cls.from_uid(uid, filename, meta, pdf_links_removed)
        self.files[uid] = f_obj
        return f_obj

    def iter_legistar_files(self) -> Iterator[LegistarFile]:
        for obj in self:
            if isinstance(obj, LegistarFile):
                yield obj

    def iter_attachments(self) -> Iterator[AttachmentFile]:
        for obj in self:
            if isinstance(obj, AttachmentFile):
                yield obj

    def keys(self) -> Iterator[LegistarFileUID]:
        yield from self.files.keys()

    def __iter__(self) -> Iterator[LegistarFile|AttachmentFile]:
        yield from self.files.values()

    def __contains__(self, key: LegistarFileUID) -> bool:
        return key in self.files

    def get(self, key: LegistarFileUID) -> LegistarFile|AttachmentFile|None:
        return self.files.get(key)

    def __getitem__(self, key: LegistarFileUID) -> LegistarFile|AttachmentFile:
        return self.files[key]

    def __setitem__(self, key: LegistarFileUID, item: LegistarFile|AttachmentFile) -> None:
        self.files[key] = item

    def __delitem__(self, key: LegistarFileUID) -> None:
        del self.files[key]

    @classmethod
    def deserialize(cls, data: dict[str, Any], parent: RGuidDetailResult|None = None) -> Self:
        obj = cls(
            base_dir=Path(data['base_dir']),
            files={
                k:get_file_cls(k).deserialize(v) for k,v in data['files'].items()
            },
        )
        if parent is not None:
            obj.parent = parent
        return obj

    def serialize(self) -> dict[str, Any]:
        return dict(
            base_dir=str(self.base_dir),
            files={k:v.serialize() for k,v in self.files.items()},
        )


@dataclass
class RGuidDetailResult(DetailPageResult):
    """Subclass of :class:`.model.DetailPageResult` for this module
    """
    parent: RGuidLegistarData = field(init=False, repr=False)
    files: RGuidLegistarFiles = field(init=False)
    """Instance of :class:`RGuidLegistarFiles`"""

    @classmethod
    def from_html(cls, html_str: str|bytes, feed_item: FeedItem, parent: RGuidLegistarData) -> Self:
        """Create an instance from the raw html from
        :attr:`~.model.DetailPageResult.page_url`
        """
        obj = super().from_html(html_str, feed_item)
        obj.parent = parent
        obj.files = RGuidLegistarFiles(base_dir=obj.get_unique_folder())
        obj.files.parent = obj
        return obj

    @property
    def guid_compare(self) -> GuidCompare:
        """A helper to compare :obj:`GUID's <.types.GUID>`
        """
        return GuidCompare(self.feed_guid)

    def get_unique_folder(self) -> Path:
        """Get a local path to store files for this item

        The folder structure will be:

        ``<category>/<year>/<real_guid>``

        Where

        ``<category>``
            Is the :attr:`~.rss_parser.FeedItem.category` of the :attr:`feed_item`

        ``<year>``
            Is the 4-digit year of the :attr:`~.rss_parser.FeedItem.meeting_date`

        ``<real_guid>``
            Is the :attr:`~.model.DetailPageResult.real_guid`

        This makes it much less complex to ensure uniqueness compared to
        :meth:`.model.DetailPageResult.get_unique_folder`, but has a downside
        of being less user-friendly in a file browser (without the metadata).
        """
        p = super().get_unique_folder()
        return p.parent / self.real_guid

    def copy(self) -> Self:
        """Create a deep copy of the instance
        """
        return self.deserialize(self.serialize(), parent=self.parent)

    def update(self, other: Self) -> RGuidUpdateResult:
        """Update *self* with changed attributes in *other*
        """
        assert self.real_guid == other.real_guid
        if self.guid_compare <= other.guid_compare:
            return RGuidUpdateResult(
                changed=False,
                link_keys=[],
                attachment_keys=[],
                files={},
                attributes={},
            )
        r = super().update(other)
        new_uids = set(other.files.keys()) - set(self.files.keys())
        files_changed: dict[LegistarFileUID, LegistarFile|AttachmentFile] = {}
        files_changed.update({uid:other.files[uid] for uid in new_uids})
        changed = r.changed
        if len(files_changed):
            changed = True
        return RGuidUpdateResult(
            changed=changed,
            link_keys=r.link_keys,
            attachment_keys=r.attachment_keys,
            attributes=r.attributes if r.attributes else {},
            files=files_changed,
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any], parent: RGuidLegistarData|None = None) -> Self:
        data = data.copy()
        file_data = data.pop('files')
        obj = super().deserialize(data)
        obj.files = RGuidLegistarFiles.deserialize(file_data, parent=obj)
        if parent is not None:
            obj.parent = parent
        return obj

    def serialize(self) -> dict[str, Any]:
        d = super().serialize()
        assert self.files is not None
        d['files'] = self.files.serialize()
        return d


@dataclass
class RGuidLegistarData(AbstractLegistarModel[REAL_GUID, RGuidDetailResult]):
    """Container for data gathered from Legistar, using :obj:`real GUID's <.types.REAL_GUID>`
    as keys
    """
    root_dir: Path
    """Root filesystem path for downloading assets"""
    detail_results: dict[REAL_GUID, RGuidDetailResult] = field(default_factory=dict)
    """Mapping of parsed :class:`RGuidDetailResult` items with their
    :attr:`~.model.DetailPageResult.feed_guid` as keys
    """
    matched_guids: dict[CLIP_ID, REAL_GUID] = field(default_factory=dict)
    """:attr:`Clips <.model.Clip.id>` that have been matched to
    :attr:`FeedItems <.rss_parser.FeedItem.guid>`
    """
    items_by_clip_id: dict[CLIP_ID, RGuidDetailResult] = field(default_factory=dict)
    """Mapping of items in :attr:`detail_results` with a valid
    :attr:`~.model.DetailPageResult.clip_id`
    """
    clip_id_overrides: dict[REAL_GUID, CLIP_ID|NoClipT] = field(default_factory=dict)
    """Mapping of items manually-linked to :class:`Clips <.model.Clip>`
    """
    clip_id_overrides_rev: dict[CLIP_ID, REAL_GUID] = field(init=False)

    def __post_init__(self) -> None:
        for item in self.detail_results.values():
            item.parent = self
        self.clip_id_overrides_rev = {
            v: k for k, v in self.clip_id_overrides.items() if v is not NoClip
        }
        for item in self:
            clip_id = item.clip_id
            if clip_id is None:
                continue
            assert clip_id not in self.items_by_clip_id
            self.items_by_clip_id[clip_id] = item

    @classmethod
    def _get_root_dir(cls, config: Config) -> Path:
        # TODO: This should be stored in Config
        return config.legistar.out_dir.parent / 'legistar-rguid'

    @classmethod
    def _get_data_file(cls, config: Config) -> Path:
        # TODO: This should be stored in Config
        return cls._get_root_dir(config) / 'data.json'

    def get_clip_id_for_guid(
        self,
        guid: REAL_GUID,
        use_overrides: bool = True
    ) -> CLIP_ID|None|NoClipT:
        """Get the clip :attr:`~.model.Clip.id` linked to the given *guid*

        Arguments:
            guid: The item :attr:`~.model.DetailPageResult.feed_guid`
            use_overrides: Whether to use items in :attr:`clip_id_overrides`
                (default is ``True``)

        Returns one of:

        - clip_id (:obj:`~.model.CLIP_ID`)
            The matched :attr:`Clip.id <.model.Clip.id>` (if one was found)
        - :obj:`~.types.NoClip`
            If the item has been explicitly set to have no :class:`~.model.Clip`
            associated with it
        - :obj:`None`
            If no match was found

        """
        item = self[guid]
        if use_overrides and item.real_guid in self.clip_id_overrides:
            return self.clip_id_overrides[item.real_guid]
        if item.clip_id is not None:
            return item.clip_id
        matched_rev = {v:k for k,v in self.matched_guids.items()}
        assert len(matched_rev) == len(self.matched_guids)
        return matched_rev.get(guid)

    def ensure_no_future_items(self) -> None:
        for item in self:
            if item.is_future:
                raise ValueError(f'item is in the future: {item.feed_guid=}')

    def ensure_unique_item_folders(self) -> None:
        item_paths = [
            self.get_folder_for_item(item) for item in self if item.can_download
        ]
        s = set()
        for p in item_paths:
            if p in s:
                raise ValueError(f'folder collision for "{p}"')
            s.add(p)
        assert len(item_paths) == len(set(item_paths))

    def _build_item(self, html_str: str|bytes, feed_item: FeedItem) -> RGuidDetailResult:
        return RGuidDetailResult.from_html(html_str, feed_item, parent=self)

    @overload
    def create_item(
        self,
        html_str: str|bytes,
        feed_item: FeedItem,
        allow_update: Literal[True]
    ) -> tuple[RGuidDetailResult, RGuidUpdateResult|None]: ...
    @overload
    def create_item(
        self,
        html_str: str|bytes,
        feed_item: FeedItem,
        allow_update: Literal[False]
    ) -> RGuidDetailResult: ...
    def create_item(
        self,
        html_str: str|bytes,
        feed_item: FeedItem,
        allow_update: bool
    ) -> tuple[RGuidDetailResult, RGuidUpdateResult|None]|RGuidDetailResult:
        """Create and add an item from html

        Arguments:
            html_str: The raw html to pass to :meth:`RGuidDetailResult.from_html`
            feed_item: The :class:`.rss_parser.FeedItem`
            allow_update: If an item exists and this is ``True``, its
                :meth:`~RGuidDetailResult.update` method will be called.
                Otherwise, a :class:`KeyError` will be raised.


        :Returns:

            - If *allow_update* is ``False`` this returns

              - **item** (:class:`RGuidDetailResult`): The parsed item

            - If *allow_update* is ``True`` this returns a :class:`tuple` of

              - **item** (:class:`RGuidDetailResult`): The parsed item
              - **update_result**: The :class:`RGuidUpdateResult` if an item
                was updated (or ``None`` if no update was performed).

        """
        new_item = self._build_item(html_str, feed_item)
        update = None
        if new_item in self:
            item = self[new_item.real_guid]
            if allow_update:
                update = item.update(item)
                return item, update
            elif item.guid_compare >= new_item.guid_compare:
                return item
            raise KeyError(f'Item exists: {new_item.real_guid}')
        else:
            item = new_item
        self.add_item(item)
        if allow_update:
            return item, None
        return item

    def add_item(self, item: RGuidDetailResult) -> None:
        """Add an existing :class:`RGuidDetailResult` object
        """
        if item in self:
            if item is self[item.real_guid]:
                return
            raise KeyError(f'Item exists: {item.real_guid}')
        assert item.parent is self
        self.detail_results[item.real_guid] = item
        clip_id = item.links.get_clip_id_from_video()
        if clip_id is not None:
            self.items_by_clip_id[clip_id] = item

    def add_detail_result(self, item: RGuidDetailResult) -> None:
        """Add a parsed :class:`RGuidDetailResult` to :attr:`detail_results`
        """
        return self.add_item(item)

    def find_match_for_clip_id(self, clip_id: CLIP_ID) -> RGuidDetailResult|None|NoClipT:
        """Find a :class:`RGuidDetailResult` match for the given *clip_id*
        """
        if clip_id in self.clip_id_overrides_rev:
            guid = self.clip_id_overrides_rev[clip_id]
            return self.get(guid)

        item = self.items_by_clip_id.get(clip_id)
        if item is not None and item.real_guid in self.clip_id_overrides:
            _clip_id = self.clip_id_overrides[item.real_guid]
            if _clip_id is NoClip:
                return NoClip
        return item

    def is_clip_id_available(self, clip_id: CLIP_ID) -> bool:
        """Check whether the given clip id is linked to an item (returns ``True``
        if there is no link)
        """
        if clip_id in self.clip_id_overrides_rev:
            return False
        if clip_id in self.matched_guids:
            return False
        return True

    def is_guid_matched(self, guid: REAL_GUID) -> bool:
        """Check whether the item matching *guid* has a :class:`~.model.Clip`
        associated with it
        """
        item = self[guid]
        override = self.clip_id_overrides.get(item.real_guid)
        if override is NoClip:
            return True
        if override is not None:
            return True
        if guid in self.matched_guids.values():
            return True
        return False

    def add_guid_match(self, clip_id: CLIP_ID, guid: REAL_GUID) -> None:
        """Add a ``Clip.id -> FeedItem`` match to :attr:`matched_guids`

        This may seem redunant considering the :meth:`find_match_for_clip_id`
        method, but is intended for adding matches for items without a
        :attr:`~.model.DetailPageLinks.video` url to parse.
        """
        assert guid not in self.matched_guids.values()
        if clip_id in self.matched_guids:
            assert self.matched_guids[clip_id] == guid
            return
        self.matched_guids[clip_id] = guid

    def add_clip_match_override(
        self,
        real_guid: REAL_GUID,
        clip_id: CLIP_ID|None|NoClipT
    ) -> None:
        """Add a manual override for the given *real_guid*

        Arguments:
            real_guid: The :attr:`~.model.DetailPageResult.real_guid` of the
                legistar item
            clip_id: The clip :attr:`.model.Clip.id` matching the item. If
                :obj:`~.types.NoClip` is given, this signifies that
                the item should not have a :class:`~.model.Clip` associated
                with it. If :obj:`None` is given, any previously added overrides
                for *real_guid* will be removed.
        """
        if clip_id is None:
            if real_guid not in self.clip_id_overrides:
                return
            real_clip_id = self.clip_id_overrides[real_guid]
            if real_clip_id is not NoClip:
                assert self.clip_id_overrides_rev[real_clip_id] == real_guid
                del self.clip_id_overrides_rev[real_clip_id]
            del self.clip_id_overrides[real_guid]
            return
        if clip_id is not NoClip:
            assert clip_id not in self.clip_id_overrides_rev
            self.clip_id_overrides_rev[clip_id] = real_guid
        self.clip_id_overrides[real_guid] = clip_id

    def iter_guid_matches(self) -> Iterator[tuple[CLIP_ID, RGuidDetailResult]]:
        """Iterate over items added by the :meth:`add_guid_match`,
        :meth:`add_guid_match` and :meth:`add_clip_match_override` methods

        Results are tuples of :obj:`CLIP_ID` and :class:`RGuidDetailResult`
        """
        real_guids = set[REAL_GUID]()
        for clip_id, guid in self.matched_guids.items():
            item = self[guid]
            if item.real_guid in self.clip_id_overrides:
                continue
            real_guids.add(item.real_guid)
            yield clip_id, item
        for real_guid, clip_id in self.clip_id_overrides.items():
            if clip_id is NoClip:
                continue
            assert real_guid not in real_guids
            item = self[real_guid]
            assert item is not None
            yield clip_id, item

    def get_folder_for_item(self, item: REAL_GUID|RGuidDetailResult) -> Path:
        """Get a local path to store files for a :class:`RGuidDetailResult`

        This is the result of :attr:`RGuidLegistarFiles.full_base_dir` from
        :attr:`RGuidDetailResult.files`
        """
        if not isinstance(item, RGuidDetailResult):
            item = self[item]
        return item.files.full_base_dir

    def get(self, key: REAL_GUID) -> RGuidDetailResult|None:
        return self.detail_results.get(key)

    def __getitem__(self, key: REAL_GUID) -> RGuidDetailResult:
        return self.detail_results[key]

    def __contains__(self, key: REAL_GUID|RGuidDetailResult) -> bool:
        if isinstance(key, RGuidDetailResult):
            key = key.real_guid
        return key in self.detail_results

    def __iter__(self) -> Iterator[RGuidDetailResult]:
        yield from self.detail_results.values()

    def __len__(self) -> int:
        return len(self.detail_results)

    def keys(self) -> Iterator[REAL_GUID]:
        yield from self.detail_results.keys()

    def items(self) -> Iterator[tuple[REAL_GUID, RGuidDetailResult]]:
        yield from self.detail_results.items()

    def item_dict(self) -> dict[REAL_GUID, RGuidDetailResult]:
        return self.detail_results

    @classmethod
    def load(
        cls,
        filename: PathLike,
        root_dir: Path|None = None,
    ) -> Self:
        """Loads an instance from previously saved data
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = json.loads(filename.read_text())
        if root_dir is not None:
            assert not root_dir.is_absolute()
            data_root = data.get('root_dir')
            if data_root is not None:
                assert Path(data_root) == root_dir
            else:
                data['root_dir'] = str(root_dir)
        return cls.deserialize(data)

    def save(self, filename: PathLike, indent: int|None = 2) -> None:
        """Saves all clip data as JSON to the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = self.serialize()
        filename.write_text(json.dumps(data, indent=indent))

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        d: dict[REAL_GUID, CLIP_ID|Literal['NoClip']] = data.get('clip_id_overrides', {})
        overrides: dict[REAL_GUID, CLIP_ID|NoClipT] = {}
        for key, val in d.items():
            if val == 'NoClip':
                val = NoClip
            overrides[key] = val
        return cls(
            root_dir=Path(data['root_dir']),
            detail_results={
                k:RGuidDetailResult.deserialize(v) for
                k,v in data['detail_results'].items()
            },
            matched_guids=data.get('matched_guids', {}),
            clip_id_overrides=overrides,
        )

    def serialize(self) -> dict[str, Any]:
        overrides: dict[REAL_GUID, CLIP_ID|Literal['NoClip']] = {}
        for key, val in self.clip_id_overrides.items():
            if val is NoClip:
                val = 'NoClip'
            overrides[key] = val
        return dict(
            root_dir=str(self.root_dir),
            detail_results={k:v.serialize() for k,v in self.detail_results.items()},
            matched_guids=self.matched_guids,
            clip_id_overrides=overrides,
        )

from __future__ import annotations
from typing import (
    TypeVar, Generic, NamedTuple, Self, Any,
    Iterator, Literal, Collection,
)
from abc import ABC, abstractmethod
from pathlib import Path
from os import PathLike
import datetime
import dataclasses
from dataclasses import dataclass, field
import json
import tempfile
from loguru import logger

from pyquery.pyquery import PyQuery
from yarl import URL

from ..model import CLIP_ID, Serializable, FileMeta
from .rss_parser import FeedItem, get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info
from .types import (
    GUID, REAL_GUID, LegistarFileKey, AttachmentName, LegistarFileUID, Category,
    NoClipT, NoClip, _GuidT, _ItemT,
)
from ..utils import remove_pdf_links


def make_path_legal(p: Path, is_dir: bool) -> Path:
    def has_bad_end_chars(part: str) -> bool:
        bad_chars = [' ', '.']
        for c in bad_chars:
            if part.endswith(c) or part.startswith(c):
                return True
        return False
    if not is_dir:
        name = p.name
        p = p.parent
    else:
        name = None
    parts = []
    for part in p.parts:
        while has_bad_end_chars(part):
            part = part.strip(' ').strip('.')
        assert len(part)
        parts.append(part)
    if name is not None:
        parts.append(name)
    return Path(*parts)


class ThisShouldBeA500ErrorButItsNot(Exception):
    """Raised when a detail page request returns a ``200 - OK`` response,
    but with error information in the HTML content

    Yes, that really happens
    """
    def __str__(self):
        return "Yes, they really do that. Isn't it lovely?"


class IncompleteItemError(Exception):
    """Raised in :meth:`DetailPageResult.from_html` if a detail page is in an
    incomplete state

    This can be the case if the agenda status is not public or if no
    meeting time has been set.
    """

class HiddenItemError(IncompleteItemError):
    """Raised if a detail page is ``"Not Viewable by the Public"``
    """

class NoMeetingTimeError(IncompleteItemError):
    """Raised if no time was set for the meeting

    .. note::

        This exception is not raised if the item is older than a set amount
        of time (see :attr:`.rss_parser.FeedItem.is_in_past`)
    """


ATTACHMENT_UID_PREFIX = ':attachment:'

def is_attachment_uid(uid: LegistarFileUID) -> bool:
    """Returns ``True`` if the given *uid* is an attachment reference
    """
    return uid.startswith(ATTACHMENT_UID_PREFIX)

def uid_to_attachment_name(uid: LegistarFileUID) -> AttachmentName:
    """Convert the given :obj:`~.types.LegistarFileUID` to an
    :obj:`~.types.AttachmentName`

    Raises:
        TypeError: If the *uid* is not an attachment reference
    """
    if not is_attachment_uid(uid):
        raise TypeError(f'uid "{uid}" is not an AttachmentName')
    name = uid.replace(ATTACHMENT_UID_PREFIX, '')
    return AttachmentName(name)

def attachment_name_to_uid(name: AttachmentName) -> LegistarFileUID:
    """Convert the given :obj:`~.types.AttachmentName` to a
    :obj:`~.types.LegistarFileUID`
    """
    return LegistarFileUID(f'{ATTACHMENT_UID_PREFIX}{name}')

def uid_to_file_key(uid: LegistarFileUID) -> LegistarFileKey:
    """Convert the given :obj:`~.types.LegistarFileUID` to a
    :obj:`~.types.LegistarFileKey`

    Raises:
        TypeError: If the *uid* is not a valid key
    """
    if uid not in LegistarFileKeys:
        raise TypeError(f'uid "{uid}" is not a LegistarFileKey')
    return uid

def file_key_to_uid(key: LegistarFileKey) -> LegistarFileUID:
    """Convert the given :obj:`~.types.LegistarFileKey` to a
    :obj:`~.types.LegistarFileUID`
    """
    return LegistarFileUID(key)


LegistarFileKeys: list[LegistarFileKey] = ['agenda', 'minutes', 'agenda_packet', 'video']

ElementKey = Literal[
    'title', 'date', 'time', 'agenda_status', 'minutes_status', 'agenda_packet',
    'agenda', 'minutes', 'video', 'location', 'attachments',
]
AgendaStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
MinutesStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
AgendaStatusItems: list[AgendaStatus] = ['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
MinutesStatusItems: list[MinutesStatus] = ['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
ItemStatus = Literal['final', 'addendum', 'draft', 'hidden']

KT = TypeVar('KT')



class FilePathURL(NamedTuple, Generic[KT]):
    """
    """
    key: KT             # The file key
    filename: Path      # The local file path
    url: URL            # URL for download


class FilePathURLComplete(NamedTuple, Generic[KT]):
    """
    """
    key: KT             # The file key
    filename: Path      # The local file path
    url: URL            # URL for download
    complete: bool      # Whether the file has been downloaded


class UpdateResult(NamedTuple):
    """
    """
    changed: bool
    """Whether any changes were made"""
    link_keys: list[LegistarFileKey]
    """Any URL attributes from :class:`DetailPageLinks` that changed"""
    attachment_keys: list[AttachmentName]
    """Any keys in :attr:`DetailPageLinks.attachments` that changed"""
    attributes: dict[str, Any]|None = None
    """Attributes of :class:`DetailPageResult` that changed"""


ELEM_ID_PREFIX = 'ctl00_ContentPlaceHolder1_'
ELEM_IDS: dict[ElementKey, str] = {
    'title': 'hypName',
    'date': 'lblDate',
    'time': 'lblTime',
    'agenda': 'hypAgenda',
    'minutes': 'hypMinutes',
    'video': 'hypVideo',
    'attachments': 'ucAttachments_lblAttachments',
    'agenda_status': 'lblAgendaStatus',
    'minutes_status': 'lblMinutesStatus',
    'agenda_packet': 'hypAgendaPacket',
    'location': 'lblLocation',
}

def build_elem_id(key: ElementKey) -> str:
    suffix = ELEM_IDS[key]
    return f'ctl00_ContentPlaceHolder1_{suffix}'

def get_elem_text(doc: PyQuery, key: ElementKey) -> str:
    elem_id = build_elem_id(key)
    elem = doc(f'#{elem_id}').eq(0)
    txt = elem.text()
    assert isinstance(txt, str)
    return txt

def get_elem_attr(doc: PyQuery, key: ElementKey, attr: str) -> str|None:
    elem_id = build_elem_id(key)
    elem = doc(f'#{elem_id}').eq(0)
    value = elem.attr(attr)
    if value is None:
        return None
    assert isinstance(value, str)
    return value


def get_attachments_hrefs(doc: PyQuery, origin_url: URL|None = None) -> dict[AttachmentName, URL]:
    elem_id = build_elem_id('attachments')
    elem = doc(f'#{elem_id}')
    if not elem.length:
        return {}
    elem = elem.eq(0)
    anchors = elem('a')
    if not len(anchors):
        return {}
    results: dict[AttachmentName, URL] = {}
    for anchor in anchors.items():
        href = anchor.attr('href')
        if href is None:
            continue
        assert isinstance(href, str)
        href = URL(href)
        if origin_url is not None and not href.is_absolute():
            href = url_with_origin(origin_url, href)
        lbl = anchor.text()
        assert isinstance(lbl, str)
        key = AttachmentName(lbl)
        assert key not in results
        assert href not in results.values()
        results[key] = href
    return results


def get_elem_href(doc: PyQuery, key: ElementKey) -> URL|None:
    if key == 'attachments':
        raise RuntimeError('cannot parse attachments here')
    href = get_elem_attr(doc, key, 'href')
    if href is None:
        return None
    assert isinstance(href, str)
    return URL(href)

def url_with_origin(origin_url: URL, path_url: URL) -> URL:
    assert origin_url.is_absolute()
    return origin_url.origin().with_path(path_url.path).with_query(path_url.query)



@dataclass
class AbstractFile(Serializable, ABC, Generic[KT]):
    """Abstract base class for file information
    """
    name: KT                #: File key
    filename: Path          #: Local file path
    metadata: FileMeta      #: The file's metadata
    pdf_links_removed: bool = False
    """Whether the embedded pdf links of the file have been removed"""

    @property
    def is_pdf(self) -> bool:
        """``True`` if this is a pdf file
        """
        return self.filename.suffix.lower().endswith('pdf')

    def remove_pdf_links(self) -> bool:
        """Strip embedded links from the pdf file

        If the file is not a pdf or if :attr:`pdf_links_removed` is already
        set to ``True``, no alteration will be performed.

        This removes URL from the hardcoded links only and does not reformat the text.
        It will still appear as blue with an underline, but will no longer
        be clickable or have a URL action.

        The resulting file will have the same path and the :attr:`metadata`
        will be updated with the new file size
        (:attr:`.model.FileMeta.content_length`).

        The :attr:`pdf_links_removed` flag will then be set to ``True``
        """
        if not self.is_pdf or self.pdf_links_removed:
            return False
        logger.debug(f'remove_pdf_links for {self.filename}')
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir).resolve()
            tmp_filename = tmpdir / self.filename.name
            remove_pdf_links(self.filename, tmp_filename)
            self.filename.unlink()
            tmp_filename.rename(self.filename)
            st = self.filename.stat()
            self.metadata.content_length = st.st_size
            self.pdf_links_removed = True
            return True

    @classmethod
    @abstractmethod
    def _build_key(cls, key_str: str) -> KT:
        raise NotImplementedError

    def serialize(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'filename': str(self.filename),
            'metadata': self.metadata.serialize(),
            'pdf_links_removed': self.pdf_links_removed,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            name=cls._build_key(data['name']),
            filename=Path(data['filename']),
            metadata=FileMeta.deserialize(data['metadata']),
            pdf_links_removed=data.get('pdf_links_removed', False),
        )


@dataclass
class LegistarFile(AbstractFile[LegistarFileKey]):
    """Information for a downloaded file within :attr:`LegistarFiles.files`
    using :obj:`~.types.LegistarFileKey` for the :attr:`~AbstractFile.name` attribute
    """
    @classmethod
    def _build_key(cls, key_str: str) -> LegistarFileKey:
        assert key_str in LegistarFileKeys
        return key_str

@dataclass
class AttachmentFile(AbstractFile[AttachmentName]):
    """Information for a downloaded attachment within
    :attr:`LegistarFiles.attachments` using :obj:`~.types.AttachmentName` for the
    :attr:`~AbstractFile.name` attribute
    """
    @classmethod
    def _build_key(cls, key_str: str) -> AttachmentName:
        return AttachmentName(key_str)


@dataclass
class LegistarFiles(Serializable):
    """Collection of files for a :class:`DetailPageResult`
    """
    guid: GUID                  #: The guid of the :class:`DetailPageResult`
    files: dict[LegistarFileKey, LegistarFile] = field(default_factory=dict)
    """Downloaded :class:`LegistarFile` information"""

    attachments: dict[AttachmentName, AttachmentFile|None] = field(default_factory=dict)
    """Additional file attachments as :class:`AttachmentFile` objects"""

    @classmethod
    def build_filename(cls, key: LegistarFileKey) -> Path:
        ext = 'mp4' if key == 'video' else 'pdf'
        return Path(f'{key}.{ext}')

    def remove_all_pdf_links(self) -> bool:
        """Call :meth:`~AbstractFile.remove_pdf_links` on all files and
        attachments
        """
        changed = False
        for f in self.files.values():
            if not f.is_pdf or f.pdf_links_removed:
                continue
            _changed = f.remove_pdf_links()
            if _changed:
                changed = True
        for f in self.attachments.values():
            if f is None or not f.is_pdf or f.pdf_links_removed:
                continue
            _changed = f.remove_pdf_links()
            if _changed:
                changed = True
        return changed

    def get_file_uid(self, key: LegistarFileKey) -> LegistarFileUID:
        """Get a unique key for the given :obj:`~.types.LegistarFileKey`
        """
        return file_key_to_uid(key)

    def get_attachment_uid(self, name: AttachmentName) -> LegistarFileUID:
        """Get a unique key for the given :obj:`~.types.AttachmentName`
        """
        return attachment_name_to_uid(name)

    def resolve_file_uid(self, uid: LegistarFileUID) -> tuple[LegistarFileKey|AttachmentName, bool]:
        """Resolve the :obj:`~.types.LegistarFileUID` to its original form

        Returns:
            (tuple):
                - **key**: A :obj:`~.types.LegistarFileKey` or :obj:`~.types.AttachmentName`
                - **is_attachment**: ``True`` if the key represents an :attr:`attachment <attachments>`

        """
        is_attachment = is_attachment_uid(uid)
        if is_attachment:
            key = uid_to_attachment_name(uid)
        else:
            key = uid_to_file_key(uid)
        return key, is_attachment

    def resolve_uid(self, uid: LegistarFileUID) -> LegistarFile|AttachmentFile|None:
        """Get the :class:`LegistarFile` or :class:`AttachmentFile` referenced
        by the given *uid* (or ``None`` if it does not exist)
        """
        if is_attachment_uid(uid):
            key = uid_to_attachment_name(uid)
            return self.attachments[key]
        key = uid_to_file_key(uid)
        return self.files[key]

    def get_is_complete(
        self,
        legistar_data: LegistarData,
        keys: Collection[LegistarFileKey] = LegistarFileKeys
    ) -> bool:
        for key, _, _ in self.iter_incomplete(legistar_data):
            if key in keys:
                return False
        for t in self.iter_attachments(legistar_data):
            if not t.complete:
                return False
        return True

    def build_attachment_filename(self, name: AttachmentName) -> Path:
        return Path('attachments') / f'{name}.pdf'

    def get_metadata(self, key: LegistarFileKey) -> FileMeta|None:
        if key not in self:
            return None
        return self.files[key].metadata

    def set_metadata(self, key: LegistarFileKey, meta: FileMeta) -> None:
        assert key in self
        self.files[key].metadata = meta

    def iter_url_paths_uid(
        self,
        legistar_data: LegistarData
    ) -> Iterator[FilePathURLComplete[LegistarFileUID]]:
        for key, filename, url, is_complete in self.iter_url_paths(legistar_data):
            uid = file_key_to_uid(key)
            yield FilePathURLComplete(uid, filename, url, is_complete)
        for key, filename, url, is_complete in self.iter_attachments(legistar_data):
            uid = attachment_name_to_uid(key)
            yield FilePathURLComplete(uid, filename, url, is_complete)

    def iter_incomplete(
        self,
        legistar_data: LegistarData,
    ) -> Iterator[FilePathURL[LegistarFileKey]]:
        for t in self.iter_url_paths(legistar_data):
            if t.complete:
                continue
            yield FilePathURL(t.key, t.filename, t.url)

    def iter_existing_url_paths(
        self,
        legistar_data: LegistarData
    ) -> Iterator[FilePathURL[LegistarFileKey]]:
        for t in self.iter_url_paths(legistar_data):
            if t.complete:
                yield FilePathURL(t.key, t.filename, t.url)

    def iter_url_paths(
        self,
        legistar_data: LegistarData,
    ) -> Iterator[FilePathURLComplete[LegistarFileKey]]:
        detail_item = legistar_data[self.guid]
        for key, url in detail_item.links:
            if url is None:
                continue
            filename = legistar_data.get_file_path(self.guid, key)
            is_complete = key in self and filename.exists()
            yield FilePathURLComplete(key, filename, url, is_complete)

    def iter_attachments(
        self,
        legistar_data: LegistarData
    ) -> Iterator[FilePathURLComplete[AttachmentName]]:
        detail_item = legistar_data[self.guid]
        for key, url in detail_item.links.attachments.items():
            filename = legistar_data.get_attachment_path(self.guid, key)
            a = self.attachments.get(key)
            is_complete = a is not None and filename.exists()
            yield FilePathURLComplete(key, filename, url, is_complete)

    def set_attachment(
        self,
        name: AttachmentName,
        filename: Path,
        meta: FileMeta,
        pdf_links_removed: bool = False
    ) -> AttachmentFile:
        assert self.attachments.get(name) is None
        a = AttachmentFile(
            name=name,
            filename=filename,
            metadata=meta,
            pdf_links_removed=pdf_links_removed,
        )
        self.attachments[name] = a
        return a

    def __getitem__(self, key: LegistarFileKey) -> LegistarFile|None:
        return self.files.get(key)

    def __setitem__(self, key: LegistarFileKey, value: LegistarFile) -> None:
        if value is not None:
            assert not value.filename.is_absolute()
        self.files[key] = value

    def __contains__(self, key: LegistarFileKey):
        return self[key] is not None

    def __iter__(self) -> Iterator[tuple[LegistarFileKey, LegistarFile|None]]:
        for key in LegistarFileKeys:
            yield key, self[key]

    def serialize(self) -> dict[str, Any]:
        return dict(
            guid=self.guid,
            files={
                k: None if v is None else v.serialize()
                for k, v in self.files.items()
            },
            attachments={
                k: None if v is None else v.serialize()
                for k, v in self.attachments.items()
            },
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        # Handle legacy storage format with flat input of
        # `dict[LegistarFileKey, PathLike]` and
        # `{'metadata': dict[LegistarFileKey, FileMeta]}`
        paths: dict[LegistarFileKey, Path|None] = {
            k: None if data.get(k) is None else Path(data[k])
            for k in LegistarFileKeys
        }
        meta: dict[LegistarFileKey, FileMeta] = {
            k: FileMeta.deserialize(v)
            for k,v in data.get('metadata', {}).items()
        }
        files: dict[LegistarFileKey, LegistarFile] = {
            k: LegistarFile.deserialize(v) for k,v in data.get('files', {}).items()
        }
        if not len(files):
            for key, p in paths.items():
                if p is None:
                    continue
                m = meta[key]
                files[key] = LegistarFile(name=key, filename=p, metadata=m)

        attachments: dict[AttachmentName, AttachmentFile|None] = {
            AttachmentName(k): None if v is None else AttachmentFile.deserialize(v)
            for k, v in data.get('attachments', {}).items()
        }
        return cls(
            guid=GUID(data['guid']),
            files=files,
            attachments=attachments,
        )


@dataclass
class DetailPageLinks(Serializable):
    """Links gathered from a meeting detail page
    """
    agenda: URL|None        #: Agenda URL
    minutes: URL|None       #: Minutes URL
    agenda_packet: URL|None #: Agenda Packet URL
    video: URL|None         #: Video player URL
    attachments: dict[AttachmentName, URL] = field(default_factory=dict)
    """Attachment URLs"""

    @classmethod
    def from_html(cls, doc: PyQuery, feed_item: FeedItem) -> Self:
        def parse_href(key: ElementKey) -> URL|None:
            href = get_elem_href(doc, key)
            if href is not None and not href.is_absolute():
                href = url_with_origin(feed_item.link, href)
            return href
        keys: list[ElementKey] = ['agenda', 'minutes', 'agenda_packet']
        url_kw = {key: parse_href(key) for key in keys}
        attachments = get_attachments_hrefs(doc, origin_url=feed_item.link)
        vid_onclick = get_elem_attr(doc, 'video', 'onclick')
        if vid_onclick is None:
            vid_href = None
        else:
            # window.open('Video.aspx?Mode=Granicus&ID1=2203&Mode2=Video','video');return false;
            assert vid_onclick.startswith("window.open('")
            vid_href = vid_onclick.split("window.open('")[1].split("'")[0]
            vid_href = URL(vid_href)
            if not vid_href.is_absolute():
                vid_href = url_with_origin(feed_item.link, vid_href)
        return cls(video=vid_href, attachments=attachments, **url_kw)

    def get_clip_id_from_video(self) -> CLIP_ID|None:
        """Parse the :attr:`clip_id <.model.Clip.id>` from the :attr:`video`
        url (if it exists)
        """
        if self.video is None:
            return None
        clip_id = self.video.query.get('ID1')
        if clip_id is not None:
            clip_id = CLIP_ID(clip_id)
        return clip_id

    def __iter__(self) -> Iterator[tuple[LegistarFileKey, URL|None]]:
        for key in LegistarFileKeys:
            yield key, self[key]

    def __len__(self):
        i = 0
        for key, val in self:
            if val is not None:
                i += 1
        i += len(self.attachments)
        return i

    def __getitem__(self, key: LegistarFileKey) -> URL|None:
        return getattr(self, key)

    def __contains__(self, key: LegistarFileKey):
        return self[key] is not None

    def update(self, other: Self) -> UpdateResult:
        """Update *self* with any changes from *other*
        """
        changed = False
        changed_keys: list[LegistarFileKey] = []
        for key, val in self:
            oth_val = other[key]
            if oth_val == val:
                continue
            assert oth_val is not None
            # logger.debug(f'{key=}, {val=}, {oth_val=}')
            changed_keys.append(key)
            setattr(self, key, oth_val)
            changed = True
        changed_attachments: list[AttachmentName] = []
        if self.attachments != other.attachments:
            for att_key, att_val in self.attachments.items():
                oth_att_val = other.attachments.get(att_key)
                if oth_att_val == att_val:
                    continue
                changed_attachments.append(att_key)
            for oth_att_key, oth_att_val in other.attachments.items():
                self_att_val = self.attachments.get(oth_att_key)
                if self_att_val == oth_att_val:
                    continue
                changed_attachments.append(oth_att_key)
            self.attachments = other.attachments
            # logger.debug('link attachments updated')
            changed = True
        return UpdateResult(changed, changed_keys, changed_attachments)

    def serialize(self) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        for key, val in data.items():
            if key == 'attachments':
                val = {k:str(v) for k,v in val.items()}
                data[key] = val
            elif isinstance(val, URL):
                data[key] = str(val)
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        for key, val in kw.items():
            if key == 'attachments':
                val = {
                    AttachmentName(k): URL(v)
                    for k, v in data.get('attachments', {}).items()
                }
                kw[key] = val
                continue
            elif val is None:
                continue
            kw[key] = URL(val)
        return cls(**kw)


@dataclass
class DetailPageResult(Serializable):
    """Data gathered from a meeting detail (``/MeetingDetail.aspx``) page
    """
    page_url: URL
    """The detail page url (from :attr:`.rss_parser.FeedItem.link`)"""
    feed_guid: GUID
    """The :attr:`.rss_parser.FeedItem.guid`"""
    location: str
    """The meeting's location (where it takes place)"""
    links: DetailPageLinks
    """URL data"""
    agenda_status: AgendaStatus
    """Agenda status"""
    minutes_status: MinutesStatus
    """Minutes status"""
    feed_item: FeedItem
    """The :class:`~.rss_parser.FeedItem` associated with this instance"""

    @property
    def clip_id(self) -> CLIP_ID|None:
        """The :attr:`clip_id <.model.Clip.id>` parsed from
        :meth:`DetailPageLinks.get_clip_id_from_video`
        """
        return self.links.get_clip_id_from_video()

    @property
    def agenda_final(self) -> bool:
        """``True`` if :attr:`agenda_status` is final
        """
        return self.agenda_status == 'Final' or self.agenda_status == 'Final-Addendum'

    @property
    def minutes_final(self) -> bool:
        """``True`` if :attr:`minutes_status` is final
        """
        return self.minutes_status == 'Final' or self.minutes_status == 'Final-Addendum'

    @property
    def is_addendum(self) -> bool:
        """``True`` if :attr:`agenda_status` or :attr:`minutes_status` is
        ``"Final-Addendum"``
        """
        return (
            self.agenda_status == 'Final-Addendum' or
            self.minutes_status == 'Final-Addendum'
        )

    @property
    def item_status(self) -> ItemStatus:
        """Overall item status

        One of:

        - ``"final"``
        - ``"addendum"``
        - ``"draft"``
        - ``"hidden"``

        """
        if self.is_hidden:
            return 'hidden'
        elif self.is_addendum:
            return 'addendum'
        elif self.is_draft:
            return 'draft'
        assert self.agenda_final and self.minutes_final
        return 'final'

    @property
    def can_download(self) -> bool:
        """Whether this item may be safely downloaded
        """
        if self.agenda_final and self.minutes_final:
            return True
        if self.is_hidden or self.is_future:
            return False
        if self.is_in_past:
            if self.agenda_final or self.minutes_final:
                return True
            if len(self.links):
                return True
        return False

    @property
    def is_draft(self) -> bool:
        """``True`` if :attr:`agenda_status` or :attr:`minutes_status` are set
        to "Draft"
        """
        return self.agenda_status == 'Draft' or self.minutes_status == 'Draft'

    @property
    def is_hidden(self) -> bool:
        """Whether the item is hidden
        (if :attr:`agenda_status` is "Not Viewable by the Public")
        """
        return (
            self.agenda_status == 'Not Viewable by the Public' or
            self.minutes_status == 'Not Viewable by the Public'
        )

    @property
    def is_future(self) -> bool:
        """Alias for :attr:`.rss_parser.FeedItem.is_future`
        """
        return self.feed_item.is_future

    @property
    def is_in_past(self) -> bool:
        """Alias for :attr:`.rss_parser.FeedItem.is_in_past`
        """
        return self.feed_item.is_in_past

    @property
    def real_guid(self) -> REAL_GUID:
        """Alias for :attr:`.rss_parser.FeedItem.real_guid`
        """
        return self.feed_item.real_guid

    def get_unique_folder(self) -> Path:
        """Get a local path to store files for this item

        The structure will be:

        ``<category>/<year>/<datetime>_<title>_<status>``

        Where

        ``<category>``
            Is the :attr:`~.rss_parser.FeedItem.category` of the :attr:`feed_item`

        ``<year>``
            Is the 4-digit year of the :attr:`~.rss_parser.FeedItem.meeting_date`

        ``<datetime>``
            Is the :attr:`~.rss_parser.FeedItem.meeting_date`
            (formatted as ``"%Y%m%d-%H%M"``)

        ``<title>``
            Is the :attr:`~.rss_parser.FeedItem.title`

        ``<status>``
            Is the :attr:`item_status`

        This combination was chosen to ensure uniqueness.
        """
        def strip_bad_chars(s: str):
            s = s.replace(' / ', ' - ').replace('\n', ' ')
            for c in '/\\':
                s = s.replace(c, '-')
            return s
        item = self.feed_item
        dt = item.meeting_date.astimezone(item.get_timezone())
        dtstr = dt.strftime('%Y%m%d-%H%M')

        title_str = f'{dtstr}_{item.title}_{self.item_status}'
        parts = [item.category, dt.strftime('%Y'), title_str]
        parts = [strip_bad_chars(part) for part in parts]
        p = Path(*parts)
        return make_path_legal(p, is_dir=True)

    @classmethod
    def from_html(
        cls,
        html_str: str|bytes,
        feed_item: FeedItem
    ) -> Self:
        """Create an instance from the raw html from :attr:`page_url`
        """
        doc = PyQuery(html_str)
        if not isinstance(html_str, str):
            html_str = html_str.decode()    # type: ignore
        doc_title = html_str.split('<title>')[1].split('</title>')[0]
        if 'Error' in doc_title:
            raise ThisShouldBeA500ErrorButItsNot()
        dt_fmt = '%m/%d/%Y - %I:%M %p'
        agenda_status = get_elem_text(doc, 'agenda_status').strip(' ')
        assert agenda_status in AgendaStatusItems, f'{agenda_status=}'
        if agenda_status == 'Not Viewable by the Public':
            raise HiddenItemError()
        date_str, time_str = get_elem_text(doc, 'date'), get_elem_text(doc, 'time')
        if len(date_str.strip(' ')) and not len(time_str.strip(' ')):
            if not feed_item.is_in_past:
                raise NoMeetingTimeError()
            tmp_dt_str = ' - '.join([date_str, '12:00 pm'])
            tmp_dt = datetime.datetime.strptime(tmp_dt_str, dt_fmt)
            tmp_dt = tmp_dt.replace(tzinfo=feed_item.get_timezone())
            assert tmp_dt == feed_item.meeting_date
        else:
            dt_str = ' - '.join([date_str, time_str])
            dt = datetime.datetime.strptime(dt_str, dt_fmt)
            dt = dt.replace(tzinfo=feed_item.get_timezone())
            assert dt == feed_item.meeting_date
        assert get_elem_text(doc, 'title') == feed_item.title

        links = DetailPageLinks.from_html(doc, feed_item)
        minutes_status = get_elem_text(doc, 'minutes_status')
        assert minutes_status in MinutesStatusItems
        return cls(
            page_url=feed_item.link,
            feed_guid=feed_item.guid,
            location=get_elem_text(doc, 'location'),
            links=links,
            agenda_status=agenda_status,
            minutes_status=minutes_status,
            feed_item=feed_item,
        )

    def serialize(self) -> dict[str, Any]:
        return dict(
            page_url=str(self.page_url),
            feed_guid=self.feed_guid,
            location=self.location,
            links=self.links.serialize(),
            agenda_status=self.agenda_status,
            minutes_status=self.minutes_status,
            feed_item=self.feed_item.serialize(),
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        kw['page_url'] = URL(kw['page_url'])
        kw['links'] = DetailPageLinks.deserialize(kw['links'])
        kw['feed_item'] = FeedItem.deserialize(kw['feed_item'])
        obj = cls(**kw)
        assert isinstance(obj.links, DetailPageLinks)
        return obj

    def copy(self) -> Self:
        """Return a deep copy of this item and its children
        """
        return self.deserialize(self.serialize())

    def to_str(self) -> str:
        return self.feed_item.to_str()

    def update(self, other: Self) -> UpdateResult:
        """Update *self* with changed attributes in *other*
        """
        assert other.real_guid == self.real_guid
        assert other.page_url == self.page_url
        changed = False
        attrs = ['agenda_status', 'minutes_status', 'location']
        changed_attrs = {}
        for attr in attrs:
            self_val = getattr(self, attr)
            oth_val = getattr(other, attr)
            if self_val == oth_val:
                continue
            logger.debug(f'{attr=}, {self_val=}, {oth_val=}')
            setattr(self, attr, oth_val)
            changed_attrs[attr] = oth_val
            changed = True
        links_changed, file_keys, attachment_keys, _ = self.links.update(other.links)
        return UpdateResult(
            changed or links_changed,
            file_keys,
            attachment_keys,
            changed_attrs,
        )


@dataclass
class AbstractLegistarModel(Serializable, Generic[_GuidT, _ItemT]):
    root_dir: Path

    @abstractmethod
    def get_clip_id_for_guid(
        self,
        guid: _GuidT,
        use_overrides: bool = True
    ) -> CLIP_ID|None|NoClipT:
        raise NotImplementedError

    @abstractmethod
    def ensure_no_future_items(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def ensure_unique_item_folders(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def find_match_for_clip_id(
        self,
        clip_id: CLIP_ID
    ) -> _ItemT|None|NoClipT:
        raise NotImplementedError

    @abstractmethod
    def is_clip_id_available(self, clip_id: CLIP_ID) -> bool:
        raise NotImplementedError

    @abstractmethod
    def is_guid_matched(self, guid: _GuidT) -> bool:
        raise NotImplementedError

    @abstractmethod
    def add_guid_match(self, clip_id: CLIP_ID, guid: _GuidT) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_clip_match_override(
        self,
        real_guid: REAL_GUID,
        clip_id: CLIP_ID|None|NoClipT
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_detail_result(self, item: _ItemT) -> None:
        raise NotImplementedError

    @abstractmethod
    def iter_guid_matches(self) -> Iterator[tuple[CLIP_ID, _ItemT]]:
        raise NotImplementedError

    @abstractmethod
    def get_folder_for_item(self, item: _GuidT|_ItemT) -> Path:
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[_ItemT]:
        raise NotImplementedError

    @abstractmethod
    def __contains__(self, key: _GuidT|_ItemT) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, key: _GuidT) -> _ItemT:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: _GuidT) -> _ItemT|None:
        raise NotImplementedError

    @abstractmethod
    def keys(self) -> Iterator[_GuidT]:
        raise NotImplementedError

    @abstractmethod
    def items(self) -> Iterator[tuple[_GuidT, _ItemT]]:
        raise NotImplementedError

    @abstractmethod
    def item_dict(self) -> dict[_GuidT, _ItemT]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(
        cls,
        filename: PathLike,
        root_dir: Path|None = None,
    ) -> Self:
        raise NotImplementedError

    @abstractmethod
    def save(self, filename: PathLike, indent: int|None = 2) -> None:
        raise NotImplementedError


@dataclass
class LegistarData(AbstractLegistarModel[GUID, DetailPageResult]):
    """Container for data gathered from Legistar
    """
    root_dir: Path
    """Root filesystem path for downloading assets"""

    matched_guids: dict[CLIP_ID, GUID] = field(default_factory=dict)
    """:attr:`Clips <.model.Clip.id>` that have been matched to
    :attr:`FeedItems <.rss_parser.FeedItem.guid>`
    """
    matched_real_guids: dict[CLIP_ID, REAL_GUID] = field(default_factory=dict)
    """Similar to :attr:`matched_guids`, but uses :obj:`~.types.REAL_GUID`
    """
    detail_results: dict[GUID, DetailPageResult] = field(default_factory=dict)
    """Mapping of parsed :class:`DetailPageResult` items with their
    :attr:`~DetailPageResult.feed_guid` as keys
    """
    items_by_clip_id: dict[CLIP_ID, DetailPageResult] = field(default_factory=dict)
    """Mapping of items in :attr:`detail_results` with a valid
    :attr:`~DetailPageResult.clip_id`
    """

    files: dict[GUID, LegistarFiles] = field(default_factory=dict)
    """Mapping of downloaded :class:`LegistarFiles` with their
    :attr:`~LegistarFiles.guid` as keys
    """

    clip_id_overrides: dict[REAL_GUID, CLIP_ID|NoClipT] = field(default_factory=dict)
    """Mapping of items manually-linked to :class:`Clips <.model.Clip>`
    """

    clip_id_overrides_rev: dict[CLIP_ID, REAL_GUID] = field(init=False)
    real_guid_map: dict[REAL_GUID, GUID] = field(init=False)
    def __post_init__(self) -> None:
        self.matched_real_guids = {
            k: get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info(v)
            for k, v in self.matched_guids.items()
        }
        self.clip_id_overrides_rev = {
            v: k for k, v in self.clip_id_overrides.items() if v is not NoClip
        }
        self.real_guid_map = {item.real_guid: item.feed_guid for item in self}
        assert len(self.real_guid_map) == len(self.detail_results)
        for item in self.detail_results.values():
            clip_id = item.clip_id
            if clip_id is None:
                continue
            assert clip_id not in self.items_by_clip_id
            self.items_by_clip_id[clip_id] = item

    def get_clip_id_for_guid(
        self,
        guid: GUID,
        use_overrides: bool = True
    ) -> CLIP_ID|None|NoClipT:
        """Get the clip :attr:`~.model.Clip.id` linked to the given *guid*

        Arguments:
            guid: The item :attr:`~DetailPageResult.feed_guid`
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
        matched_rev = {v:k for k,v in self.matched_real_guids.items()}
        assert len(matched_rev) == len(self.matched_guids)
        return matched_rev.get(item.real_guid)

    def get_future_items(self) -> Iterator[DetailPageResult]:
        """Iterate over any items in :attr:`detail_results` that are in the
        :attr:`future <.rss_parser.FeedItem.is_future>`
        """
        for item in self:
            if item.is_future:
                yield item

    def ensure_no_future_items(self) -> None:
        """Ensure there are no items in :attr:`detail_results` that are in the
        :attr:`future <.rss_parser.FeedItem.is_future>`
        """
        for item in self:
            if item.is_future:
                raise ValueError(f'item is in the future: {item.feed_guid=}')

    def ensure_unique_item_folders(self) -> None:
        """Unsure paths generated by :meth:`DetailPageResult.get_folder_for_item`
        are unique among all items in :attr:`detail_results`
        """
        item_paths = [
            self.get_folder_for_item(item) for item in self if item.can_download
        ]
        s = set()
        for p in item_paths:
            if p in s:
                raise ValueError(f'folder collision for "{p}"')
            s.add(p)
        assert len(item_paths) == len(set(item_paths))

    def is_clip_id_available(self, clip_id: CLIP_ID) -> bool:
        """Check whether the given clip id is linked to an item (returns ``True``
        if there is no link)
        """
        if clip_id in self.clip_id_overrides_rev:
            return False
        if clip_id in self.matched_guids:
            return False
        return True

    def is_guid_matched(self, guid: GUID) -> bool:
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
        real_guid = get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info(guid)
        if real_guid in self.matched_real_guids.values():
            return True
        return False

    def get_by_real_guid(self, real_guid: REAL_GUID) -> DetailPageResult|None:
        """Get the :class:`DetailPageResult` matching the given
        :attr:`~.rss_parser.FeedItem.real_guid`

        If no match is found, ``None`` is returned.
        """
        d = self.real_guid_map
        if real_guid not in d:
            return None
        guid = d[real_guid]
        return self[guid]

    def find_match_for_clip_id(
        self,
        clip_id: CLIP_ID
    ) -> DetailPageResult|None|NoClipT:
        """Find a :class:`DetailPageResult` match for the given *clip_id*
        """
        if clip_id in self.clip_id_overrides_rev:
            real_guid = self.clip_id_overrides_rev[clip_id]
            return self.get_by_real_guid(real_guid)

        item = self.items_by_clip_id.get(clip_id)
        if item is not None and item.real_guid in self.clip_id_overrides:
            _clip_id = self.clip_id_overrides[item.real_guid]
            if _clip_id is NoClip:
                return NoClip
        return item

    def add_guid_match(self, clip_id: CLIP_ID, guid: GUID) -> None:
        """Add a ``Clip.id -> FeedItem`` match to :attr:`matched_guids`

        This may seem redunant considering the :meth:`find_match_for_clip_id`
        method, but is intended for adding matches for items without a
        :attr:`~DetailPageLinks.video` url to parse.
        """
        assert guid not in self.matched_guids.values()
        real_guid = get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info(guid)
        assert real_guid not in self.matched_real_guids.values()
        if clip_id in self.matched_guids:
            raise KeyError(f'clip id {clip_id} already matched')
        self.matched_guids[clip_id] = guid
        self.matched_real_guids[clip_id] = real_guid

    def add_clip_match_override(
        self,
        real_guid: REAL_GUID,
        clip_id: CLIP_ID|None|NoClipT
    ) -> None:
        """Add a manual override for the given :attr:`~DetailPageResult.real_guid`

        Arguments:
            real_guid: The :attr:`~DetailPageResult.real_guid` of the legistar
                item
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

    def add_detail_result(self, item: DetailPageResult) -> None:
        """Add a parsed :class:`DetailPageResult` to :attr:`detail_results`
        """
        assert not item.is_future
        assert item.feed_guid not in self.detail_results
        oth_guid = self.real_guid_map.get(item.real_guid)
        if oth_guid is not None:
            raise KeyError(f'real_guid exists: {item.to_str()=}, {oth_guid=}')
        self.detail_results[item.feed_guid] = item
        self.real_guid_map[item.real_guid] = item.feed_guid
        clip_id = item.links.get_clip_id_from_video()
        if clip_id is not None:
            self.items_by_clip_id[clip_id] = item

    def iter_guid_matches(self) -> Iterator[tuple[CLIP_ID, DetailPageResult]]:
        """Iterate over items added by the :meth:`add_guid_match`,
        :meth:`add_guid_match` and :meth:`add_clip_match_override` methods

        Results are tuples of :obj:`CLIP_ID` and :class:`DetailPageResult`
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
            item = self.get_by_real_guid(real_guid)
            assert item is not None
            yield clip_id, item

    def get_folder_for_item(self, item: GUID|DetailPageResult) -> Path:
        """Get a local path to store files for a :class:`DetailPageResult`

        See :meth:`DetailPageResult.get_folder_for_item` for more details.
        """
        if not isinstance(item, DetailPageResult):
            item = self[item]
        return self.root_dir / item.get_unique_folder()

    def get_or_create_files(self, guid: GUID) -> LegistarFiles:
        """Get a :class:`LegistarFiles` instance for *guid*, creating one if
        it does not exist
        """
        if guid in self.files:
            return self.files[guid]
        files = LegistarFiles(guid=guid)
        self.files[guid] = files
        return files

    def get_file_uid(self, guid: GUID, key: LegistarFileKey) -> LegistarFileUID:
        """Get a unique key for the given :obj:`~.types.GUID` and
        :obj:`~.types.LegistarFileKey`
        """
        files = self.get_or_create_files(guid)
        return files.get_file_uid(key)

    def get_attachment_uid(self, guid: GUID, name: AttachmentName) -> LegistarFileUID:
        """Get a unique key for the given :obj:`~.types.GUID` and
        :obj:`~.types.AttachmentName`
        """
        files = self.get_or_create_files(guid)
        return files.get_attachment_uid(name)

    def get_path_for_uid(self, guid: GUID, uid: LegistarFileUID) -> tuple[Path, FileMeta|None]:
        """Get filesystem path for the :obj:`~.types.GUID` and
        :obj:`~.types.LegistarFileUID`

        Returns:
            (tuple):
                - **filename** (:class:`~.pathlib.Path`): The local filename
                - **meta** (:class:`~.model.FileMeta`, *optional*): The file's
                  metadata (if it exists)

        """
        files = self.get_or_create_files(guid)
        key, is_attachment = files.resolve_file_uid(uid)
        if is_attachment:
            key = AttachmentName(key)
            att = files.attachments.get(key)
            meta = None if att is None else att.metadata
            return self.get_attachment_path(guid, key), meta
        assert key in LegistarFileKeys
        file_obj = files[key]
        meta = None if file_obj is None else file_obj.metadata
        return self.get_file_path(guid, key), meta

    def iter_files_for_upload(
        self,
        guid: GUID
    ) -> Iterator[tuple[LegistarFileUID, Path, FileMeta, bool]]:
        """Iterate over files present locally for the given :obj:`~.types.GUID`

        Yields:
            :
                - **uid** (:obj:`~.types.LegistarFileUID`): The uid for the file type
                - **filename** (:class:`~pathlib.Path`): The local file path
                - **meta** (:class:`~.model.FileMeta`): Local meta data for the file
                - **is_attachment** (:class:`bool`): ``True`` if the *uid* refers to an
                  :attr:`attachment <LegistarFiles.attachments>`, ``False`` otherwise

        """
        for key, filename, _ in self.iter_existing_url_paths(guid):
            assert filename.exists()
            uid = self.get_file_uid(guid, key)
            meta = self.files[guid].get_metadata(key)
            assert meta is not None
            yield uid, filename, meta, False
        for key, filename, _, is_complete in self.iter_attachments(guid):
            if not is_complete:
                continue
            assert filename.exists()
            uid = self.get_attachment_uid(guid, key)
            att = self.files[guid].attachments[key]
            assert att is not None
            yield uid, filename, att.metadata, True

    def get_file_path(
        self,
        guid: GUID,
        key: LegistarFileKey
    ) -> Path:
        """Get the local path for the :class:`LegistarFiles` object matching
        the given :attr:`guid <LegistarFiles.guid>` and :obj:`file key <LegistarFileKey>`
        """
        files = self.get_or_create_files(guid)
        base_dir = self.get_folder_for_item(guid)
        return base_dir / files.build_filename(key)

    def set_uid_complete(
        self,
        guid: GUID,
        uid: LegistarFileUID,
        meta: FileMeta,
        pdf_links_removed: bool = False
    ) -> LegistarFile|AttachmentFile:
        """Set the file or attachment for the given parameters as "complete"
        (after successful download)

        This calls either :meth:`set_file_complete` or :meth:`set_attachment_complete`
        depending on the *uid*.

        Arguments:
            guid: The :attr:`guid <LegistarFiles.guid>` of the :class:`LegistarFiles`
                object
            uid: The :obj:`file uid <.types.LegistarFileUID>`
            meta: Metadata from the download
            pdf_links_removed: Value to set for the file's
                :attr:`~AbstractFile.pdf_links_removed` attribute
        """
        if is_attachment_uid(uid):
            key = uid_to_attachment_name(uid)
            return self.set_attachment_complete(guid, key, meta, pdf_links_removed)
        key = uid_to_file_key(uid)
        return self.set_file_complete(guid, key, meta, pdf_links_removed)

    def set_file_complete(
        self,
        guid: GUID,
        key: LegistarFileKey,
        meta: FileMeta,
        pdf_links_removed: bool = False
    ) -> LegistarFile:
        """Set the file for the given parameters as "complete"
        (after successful download)

        Arguments:
            guid: The :attr:`guid <LegistarFiles.guid>` of the :class:`LegistarFiles`
                object
            key: The :obj:`file key <LegistarFileKey>`
            meta: Metadata from the download
            pdf_links_removed: Value to set for the file's
                :attr:`~AbstractFile.pdf_links_removed` attribute
        """
        files = self.get_or_create_files(guid)
        assert key not in files
        filename = self.get_file_path(guid, key)
        assert filename.exists()
        assert filename.stat().st_size == meta.content_length
        file_obj = LegistarFile(
            name=key,
            filename=filename,
            metadata=meta,
            pdf_links_removed=pdf_links_removed,
        )
        files[key] = file_obj
        return file_obj

    def get_attachment_path(self, guid: GUID, name: AttachmentName) -> Path:
        """Get the local path for an item in :attr:`LegistarFiles.attachments`
        """
        files = self.get_or_create_files(guid)
        base_dir = self.get_folder_for_item(guid)
        fn = base_dir / files.build_attachment_filename(name)
        att = files.attachments.get(name)
        if att is not None:
            assert att.filename == fn
        return fn

    def set_attachment_complete(
        self,
        guid: GUID,
        name: AttachmentName,
        meta: FileMeta,
        pdf_links_removed: bool = False
    ) -> AttachmentFile:
        """Set an item in :attr:`LegistarFiles.attachments` as "complete"
        (after successful download)

        Arguments:
            guid: The :attr:`guid <LegistarFiles.guid>` of the :class:`LegistarFiles`
                object
            name: The :obj:`AttachmentName` (key within
                :attr:`LegistarFiles.attachments`)
            meta: Metadata from the download
            pdf_links_removed: Value to set for the file's
                :attr:`~AbstractFile.pdf_links_removed` attribute
        """
        files = self.get_or_create_files(guid)
        assert files.attachments.get(name) is None
        detail_item = self[guid]
        assert name in detail_item.links.attachments
        filename = self.get_attachment_path(guid, name)
        assert filename.exists()
        assert filename.stat().st_size == meta.content_length
        return files.set_attachment(name, filename, meta, pdf_links_removed)

    def iter_url_paths_uid(
        self,
        guid: GUID
    ) -> Iterator[FilePathURLComplete[LegistarFileUID]]:
        """Iterate over all files and attachments for the given *guid*
        as :class:`FilePathURLComplete` tuples using the
        :obj:`uid <types.LegistarFileUID>` as the :attr:`~FilePathURLComplete.key`
        parameter
        """
        files = self.get_or_create_files(guid)
        yield from files.iter_url_paths_uid(self)

    def iter_attachments(
        self,
        guid: GUID
    ) -> Iterator[FilePathURLComplete[AttachmentName]]:
        """Iterate over any :attr:`LegistarFiles.attachments` for the given
        *guid* (as :class:`FilePathURLComplete` tuples)
        """
        files = self.get_or_create_files(guid)
        yield from files.iter_attachments(self)

    def iter_incomplete_attachments(
        self,
        guid: GUID
    ) -> Iterator[FilePathURL[AttachmentName]]:
        """Iterate over :attr:`LegistarFiles.attachments` which have not been
        downloaded (as :class:`FilePathURL` tuples)
        """
        for t in self.iter_attachments(guid):
            if t.complete:
                continue
            yield FilePathURL(t.key, t.filename, t.url)

    def iter_url_paths(
        self,
        guid: GUID
    ) -> Iterator[FilePathURLComplete[LegistarFileKey]]:
        """Iterate over items in a :class:`LegistarFiles` instance (as
        :class:`FilePathURLComplete` tuples)
        """
        files = self.get_or_create_files(guid)
        yield from files.iter_url_paths(self)

    def iter_incomplete_url_paths(
        self,
        guid: GUID
    ) -> Iterator[FilePathURL[LegistarFileKey]]:
        """Iterate over items in a :class:`LegistarFiles` instance which have
        not been downloaded (as :class:`FilePathURL` tuples)
        """
        files = self.get_or_create_files(guid)
        yield from files.iter_incomplete(self)

    def iter_existing_url_paths(
        self,
        guid: GUID
    ) -> Iterator[FilePathURL[LegistarFileKey]]:
        """Iterate over items in a :class:`LegistarFiles` instance which have
        been successfully downloaded (as :class:`FilePathURL` tuples)
        """
        files = self.get_or_create_files(guid)
        yield from files.iter_existing_url_paths(self)

    def __len__(self):
        return len(self.detail_results)

    def __iter__(self):
        yield from self.detail_results.values()

    def __contains__(self, key: GUID|DetailPageResult):
        if isinstance(key, DetailPageResult):
            key = key.feed_guid
        return key in self.detail_results

    def __getitem__(self, key: GUID) -> DetailPageResult:
        return self.detail_results[key]

    def get(self, key: GUID) -> DetailPageResult|None:
        return self.detail_results.get(key)

    def keys(self) -> Iterator[GUID]:
        yield from self.detail_results.keys()

    def items(self) -> Iterator[tuple[GUID, DetailPageResult]]:
        yield from self.detail_results.items()

    def item_dict(self) -> dict[GUID, DetailPageResult]:
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

    def serialize(self) -> dict[str, Any]:
        overrides: dict[REAL_GUID, CLIP_ID|Literal['NoClip']] = {}
        for key, val in self.clip_id_overrides.items():
            if val is NoClip:
                val = 'NoClip'
            overrides[key] = val
        return dict(
            root_dir=str(self.root_dir),
            matched_guids=self.matched_guids,
            detail_results={k:v.serialize() for k,v in self.detail_results.items()},
            clip_id_overrides=overrides,
            files={k:v.serialize() for k,v in self.files.items()},
        )

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
            matched_guids=data['matched_guids'],
            detail_results={
                k:DetailPageResult.deserialize(v)
                for k,v in data['detail_results'].items()
            },
            files={k:LegistarFiles.deserialize(v) for k,v in data['files'].items()},
            clip_id_overrides=overrides,
        )

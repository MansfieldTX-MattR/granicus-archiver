from __future__ import annotations
from typing import (
    TypeVar, NewType, Generic, NamedTuple, Self, Any,
    Iterator, Literal, Collection,
)
from pathlib import Path
from os import PathLike
import datetime
import dataclasses
from dataclasses import dataclass, field
import json
from loguru import logger

from pyquery.pyquery import PyQuery
from yarl import URL

from ..model import CLIP_ID, Serializable, FileMeta
from .rss_parser import GUID, REAL_GUID, FeedItem


class ThisShouldBeA500ErrorButItsNot(Exception):
    """Raised when a detail page request returns a ``200 - OK`` response,
    but with error information in the HTML content

    Yes, that really happens
    """
    def __str__(self):
        return "Yes, they really do that. Isn't it lovely?"


class IncompleteItemError(Exception):
    """Raised if a detail page is in an incomplete state

    This can be the case if the agenda status is not public or if no
    meeting time has been set.
    """


LegistarFileKey = Literal['agenda', 'minutes', 'agenda_packet', 'video']
"""Key name for legistar files"""

LegistarFileKeys: list[LegistarFileKey] = ['agenda', 'minutes', 'agenda_packet', 'video']

ElementKey = Literal[
    'title', 'date', 'time', 'agenda_status', 'minutes_status', 'agenda_packet',
    'agenda', 'minutes', 'video', 'location', 'attachments',
]
AgendaStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
MinutesStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
AgendaStatusItems: list[AgendaStatus] = ['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
MinutesStatusItems: list[MinutesStatus] = ['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']

KT = TypeVar('KT')
AttachmentName = NewType('AttachmentName', str)
"""Type variable to associate keys in :attr:`DetailPageLinks.attachments` with
:attr:`AttachmentFile.name`
"""


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
class AttachmentFile(Serializable):
    """Information for a downloaded attachment within
    :attr:`LegistarFiles.attachments`
    """
    name: AttachmentName    #: The attachment name
    filename: Path          #: Local file path for the file
    metadata: FileMeta      #: The file's metadata

    def serialize(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'filename': str(self.filename),
            'metadata': self.metadata.serialize(),
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            name=AttachmentName(data['name']),
            filename=Path(data['filename']),
            metadata=FileMeta.deserialize(data['metadata']),
        )


@dataclass
class LegistarFiles(Serializable):
    """Collection of files for a :class:`DetailPageResult`
    """
    guid: GUID                  #: The guid of the :class:`DetailPageResult`
    agenda: Path|None           #: Agenda file
    minutes: Path|None          #: Minutes file
    agenda_packet: Path|None    #: Agenda Packet file
    video: Path|None            #: Video file

    attachments: dict[AttachmentName, AttachmentFile|None] = field(default_factory=dict)
    """Additional file attachments"""

    metadata: dict[LegistarFileKey, FileMeta] = field(default_factory=dict)
    """Mapping of :class:`FileMeta` for downloaded file members (excluding
    :attr:`attachments`)
    """

    @classmethod
    def build_filename(cls, key: LegistarFileKey) -> Path:
        ext = 'mp4' if key == 'video' else 'pdf'
        return Path(f'{key}.{ext}')

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
        return self.metadata.get(key)

    def set_metadata(self, key: LegistarFileKey, meta: FileMeta) -> None:
        self.metadata[key] = meta

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
        meta: FileMeta
    ) -> AttachmentFile:
        assert self.attachments.get(name) is None
        a = AttachmentFile(
            name=name,
            filename=filename,
            metadata=meta,
        )
        self.attachments[name] = a
        return a

    def __getitem__(self, key: LegistarFileKey) -> Path|None:
        return getattr(self, key, None)

    def __setitem__(self, key: LegistarFileKey, value: Path|None) -> None:
        if value is not None:
            assert not value.is_absolute()
        setattr(self, key, value)

    def __contains__(self, key: LegistarFileKey):
        return self[key] is not None

    def __iter__(self) -> Iterator[tuple[LegistarFileKey, Path|None]]:
        for key in LegistarFileKeys:
            yield key, self[key]

    def serialize(self) -> dict[str, Any]:
        d: dict[str, object] = {k: None if v is None else str(v) or None for k,v in self}
        d['guid'] = self.guid
        d['metadata'] = {k: None if v is None else v.serialize() for k,v in self.metadata.items()}
        d['attachments'] = {
            k: None if v is None else v.serialize()
            for k, v in self.attachments.items()
        }
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        paths: dict[LegistarFileKey, Path|None] = {
            k: None if data[k] is None else Path(data[k])
            for k in LegistarFileKeys
        }
        meta: dict[LegistarFileKey, FileMeta] = {
            k: FileMeta.deserialize(v)
            for k,v in data['metadata'].items()
        }
        attachments: dict[AttachmentName, AttachmentFile|None] = {
            AttachmentName(k): None if v is None else AttachmentFile.deserialize(v)
            for k, v in data.get('attachments', {}).items()
        }
        return cls(
            guid=GUID(data['guid']),
            metadata=meta,
            attachments=attachments,
            **paths
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
    def is_final(self) -> bool:
        """``True`` if :attr:`agenda_status` and :attr:`minutes_status` are
        final
        """
        agenda = self.agenda_status == 'Final' or self.agenda_status == 'Final-Addendum'
        minutes = self.minutes_status == 'Final' or self.minutes_status == 'Final-Addendum'
        return agenda and minutes

    @property
    def is_draft(self) -> bool:
        """``True`` if :attr:`agenda_status` or :attr:`minutes_status` are set
        to "Draft"
        """
        return self.agenda_status == 'Draft' or self.minutes_status == 'Draft'

    @property
    def is_future(self) -> bool:
        """Alias for :attr:`.rss_parser.FeedItem.is_future`
        """
        return self.feed_item.is_future

    @property
    def real_guid(self) -> REAL_GUID:
        """Alias for :attr:`.rss_parser.FeedItem.real_guid`
        """
        return self.feed_item.real_guid

    def get_unique_folder(self) -> Path:
        """Get a local path to store files for this item

        The structure will be:

        ``<category>/<year>/<title>``

        Where ``<category>`` is the :attr:`~.rss_parser.FeedItem.category` of
        the :attr:`feed_item`, ``<year>`` is the 4-digit year of the
        :attr:`~.rss_parser.FeedItem.meeting_date` and ``<title>`` is a
        combination of the date, :attr:`~.rss_parser.FeedItem.title` and
        :attr:`location`.  This combination was chosen to ensure uniqueness.
        """
        item = self.feed_item
        dt = item.meeting_date.astimezone(item.get_timezone())
        dtstr = dt.strftime('%Y%m%d-%H%M')

        # location often has newlines and its length can cause issues with
        # directory name limitations.  Strip newlines and limit to 80 chars
        loc = self.location.replace('\n', ' ')[:80]
        title_str = f'{dtstr} - {item.title} - {loc}'
        p = Path(item.category) / dt.strftime('%Y') / title_str
        return p

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
            raise IncompleteItemError()
        date_str, time_str = get_elem_text(doc, 'date'), get_elem_text(doc, 'time')
        if len(date_str.strip(' ')) and not len(time_str.strip(' ')):
            raise IncompleteItemError()
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

    def update(self, other: Self) -> UpdateResult:
        """Update *self* with changed attributes in *other*
        """
        assert other.real_guid == self.real_guid
        assert other.page_url == self.page_url
        changed = False
        attrs = ['agenda_status', 'minutes_status', 'location']
        for attr in attrs:
            self_val = getattr(self, attr)
            oth_val = getattr(other, attr)
            if self_val == oth_val:
                continue
            logger.debug(f'{attr=}, {self_val=}, {oth_val=}')
            setattr(self, attr, oth_val)
            changed = True
        links_changed, file_keys, attachment_keys = self.links.update(other.links)
        return UpdateResult(changed or links_changed, file_keys, attachment_keys)


@dataclass
class LegistarData(Serializable):
    """Container for data gathered from Legistar
    """
    root_dir: Path
    """Root filesystem path for downloading assets"""

    matched_guids: dict[CLIP_ID, GUID] = field(default_factory=dict)
    """:attr:`Clips <.model.Clip.id>` that have been matched to
    :attr:`FeedItems <.rss_parser.FeedItem.guid>`
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

    def __post_init__(self) -> None:
        for item in self.detail_results.values():
            clip_id = item.clip_id
            if clip_id is None:
                continue
            assert clip_id not in self.items_by_clip_id
            self.items_by_clip_id[clip_id] = item

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
        item_paths = [self.get_folder_for_item(item) for item in self]
        s = set()
        for p in item_paths:
            if p in s:
                raise ValueError(f'folder collision for "{p}"')
            s.add(p)
        assert len(item_paths) == len(set(item_paths))

    def get_real_guids(self) -> set[REAL_GUID]:
        return set([item.real_guid for item in self])

    def get_by_real_guid(self, real_guid: REAL_GUID) -> DetailPageResult|None:
        """Get the :class:`DetailPageResult` matching the given
        :attr:`~.rss_parser.FeedItem.real_guid`

        If no match is found, ``None`` is returned.
        """
        d = {item.real_guid: item.feed_guid for item in self}
        if real_guid not in d:
            return None
        guid = d[real_guid]
        return self[guid]

    def find_match_for_clip_id(self, clip_id: CLIP_ID) -> DetailPageResult|None:
        """Find a :class:`DetailPageResult` match for the given *clip_id*
        """
        return self.items_by_clip_id.get(clip_id)

    def add_guid_match(self, clip_id: CLIP_ID, guid: GUID) -> None:
        """Add a ``Clip.id -> FeedItem`` match to :attr:`matched_guids`

        This may seem redunant considering the :meth:`find_match_for_clip_id`
        method, but is intended for adding matches for items without a
        :attr:`~DetailPageLinks.video` url to parse.
        """
        assert guid not in self.matched_guids.values()
        if clip_id in self.matched_guids:
            assert self.matched_guids[clip_id] == guid
            return
        self.matched_guids[clip_id] = guid

    def add_detail_result(self, item: DetailPageResult) -> None:
        """Add a parsed :class:`DetailPageResult` to :attr:`detail_results`
        """
        assert not item.is_future
        assert item.feed_guid not in self.detail_results
        if item.real_guid in self.get_real_guids():
            d = {item.real_guid: item.feed_guid for item in self}
            oth_guid = d[item.real_guid]
            raise KeyError(f'real_guid exists: {item.feed_guid=}, {oth_guid=}')
        self.detail_results[item.feed_guid] = item
        clip_id = item.links.get_clip_id_from_video()
        if clip_id is not None:
            self.items_by_clip_id[clip_id] = item

    def iter_guid_matches(self) -> Iterator[tuple[CLIP_ID, DetailPageResult]]:
        """Iterate over items added by the :meth:`add_guid_match` method as
        :obj:`CLIP_ID` and :class:`DetailPageResult` pairs
        """
        for clip_id, guid in self.matched_guids.items():
            yield clip_id, self.detail_results[guid]

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
        files = LegistarFiles(
            guid=guid,
            agenda=None,
            minutes=None,
            agenda_packet=None,
            video=None,
        )
        self.files[guid] = files
        return files

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

    def set_file_complete(
        self,
        guid: GUID,
        key: LegistarFileKey,
        meta: FileMeta
    ) -> None:
        """Set the file for the given parameters as "complete"
        (after successful download)

        Arguments:
            guid: The :attr:`guid <LegistarFiles.guid>` of the :class:`LegistarFiles`
                object
            key: The :obj:`file key <LegistarFileKey>`
            meta: Metadata from the download
        """
        files = self.get_or_create_files(guid)
        assert key not in files
        filename = self.get_file_path(guid, key)
        assert filename.exists()
        assert filename.stat().st_size == meta.content_length
        files[key] = filename
        files.set_metadata(key, meta)

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
        meta: FileMeta
    ) -> AttachmentFile:
        """Set an item in :attr:`LegistarFiles.attachments` as "complete"
        (after successful download)

        Arguments:
            guid: The :attr:`guid <LegistarFiles.guid>` of the :class:`LegistarFiles`
                object
            name: The :obj:`AttachmentName` (key within
                :attr:`LegistarFiles.attachments`)
            meta: Metadata from the download
        """
        files = self.get_or_create_files(guid)
        assert files.attachments.get(name) is None
        detail_item = self[guid]
        assert name in detail_item.links.attachments
        filename = self.get_attachment_path(guid, name)
        assert filename.exists()
        assert filename.stat().st_size == meta.content_length
        return files.set_attachment(name, filename, meta)

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
        return dict(
            root_dir=str(self.root_dir),
            matched_guids=self.matched_guids,
            detail_results={k:v.serialize() for k,v in self.detail_results.items()},
            files={k:v.serialize() for k,v in self.files.items()},
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            root_dir=Path(data['root_dir']),
            matched_guids=data['matched_guids'],
            detail_results={
                k:DetailPageResult.deserialize(v)
                for k,v in data['detail_results'].items()
            },
            files={k:LegistarFiles.deserialize(v) for k,v in data['files'].items()},
        )

from __future__ import annotations
from typing import NewType, Self, Any, Iterator, Literal
from pathlib import Path
from os import PathLike
import datetime
import dataclasses
from dataclasses import dataclass, field
import json

from pyquery.pyquery import PyQuery
from yarl import URL

from ..model import CLIP_ID, Serializable
from .rss_parser import GUID, REAL_GUID, FeedItem


class IncompleteItemError(Exception):
    """Raised if a detail page is in an incomplete state

    This can be the case if the agenda status is not public or if no
    meeting time has been set.
    """



ElementKey = Literal[
    'title', 'date', 'time', 'agenda_status', 'minutes_status', 'agenda_packet',
    'agenda', 'minutes', 'video', 'location', 'attachments',
]
AgendaStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
MinutesStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
AgendaStatusItems: list[AgendaStatus] = ['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
MinutesStatusItems: list[MinutesStatus] = ['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']

AttachmentName = NewType('AttachmentName', str)
"""Type variable to associate keys in :attr:`DetailPageLinks.attachments` with
:attr:`AttachmentFile.name`
"""


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

    @classmethod
    def from_html(
        cls,
        html_str: str|bytes,
        feed_item: FeedItem
    ) -> Self:
        """Create an instance from the raw html from :attr:`page_url`
        """
        doc = PyQuery(html_str)
        dt_fmt = '%m/%d/%Y - %I:%M %p'
        agenda_status = get_elem_text(doc, 'agenda_status').strip(' ')
        assert agenda_status in AgendaStatusItems
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

    def get_real_guids(self) -> set[REAL_GUID]:
        return set([item.real_guid for item in self])

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
        )

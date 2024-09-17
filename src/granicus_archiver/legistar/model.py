from __future__ import annotations
from typing import Self, Any, Iterator, Literal
from pathlib import Path
from os import PathLike
import datetime
import dataclasses
from dataclasses import dataclass, field
import json

from pyquery.pyquery import PyQuery
from yarl import URL

from ..model import CLIP_ID, Serializable, ClipCollection
from .rss_parser import ParseError, ParseErrorType, GUID, FeedItem


ElementKey = Literal[
    'title', 'date', 'time', 'agenda_status', 'minutes_status', 'agenda_packet'
]
AgendaStatus = str #Literal[...]
MinutesStatus = str #Literal[...]


ELEM_ID_PREFIX = 'ctl00_ContentPlaceHolder1_'
ELEM_IDS: dict[ElementKey, str] = {
    'title': 'hypName',
    'date': 'lblDate',
    'time': 'lblTime',
    'agenda_status': 'lblAgendaStatus',
    'minutes_status': 'lblMinutesStatus',
    'agenda_packet': 'hypAgendaPacket',
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

def get_elem_href(doc: PyQuery, key: ElementKey) -> URL|None:
    elem_id = build_elem_id(key)
    elem = doc(f'#{elem_id}').eq(0)
    href = elem.attr('href')
    if href is None:
        return None
    assert isinstance(href, str)
    return URL(href)

def url_with_origin(origin_url: URL, path_url: URL) -> URL:
    assert origin_url.is_absolute()
    return origin_url.origin().with_path(path_url.path).with_query(path_url.query)


@dataclass
class DetailPageResult(Serializable):
    """Data gathered from a meeting detail (``/MeetingDetail.aspx``) page
    """
    clip_id: CLIP_ID
    """The :attr:`.model.Clip.id` associated with the meeting"""
    page_url: URL
    """The detail page url (from :attr:`.rss_parser.FeedItem.link`)"""
    feed_guid: GUID
    """The :attr:`.rss_parser.FeedItem.guid`"""
    # agenda_status: AgendaStatus
    # """Agenda status"""
    # minutes_status: MinutesStatus
    # """Minutes status"""
    agenda_packet_url: URL|None
    """URL of the agenda packet (if it exists)"""

    @classmethod
    def from_html(
        cls,
        html_str: str|bytes,
        clip_id: CLIP_ID,
        feed_item: FeedItem
    ) -> Self:
        """Create an instance from the raw html from :attr:`page_url`
        """
        doc = PyQuery(html_str)
        dt_fmt = '%m/%d/%Y - %I:%M %p'
        dt_str = ' - '.join([
            get_elem_text(doc, 'date'),
            get_elem_text(doc, 'time'),
        ])
        dt = datetime.datetime.strptime(dt_str, dt_fmt)
        dt = dt.replace(tzinfo=feed_item.TZ)
        assert dt == feed_item.meeting_date
        assert get_elem_text(doc, 'title') == feed_item.title

        agenda_packet_url = get_elem_href(doc, 'agenda_packet')
        if agenda_packet_url is not None and not agenda_packet_url.is_absolute():
            agenda_packet_url = url_with_origin(feed_item.link, agenda_packet_url)
        return cls(
            clip_id=clip_id,
            page_url=feed_item.link,
            feed_guid=feed_item.guid,
            # agenda_status=get_elem_text(doc, 'agenda_status'),
            # minutes_status=get_elem_text(doc, 'minutes_status'),
            agenda_packet_url=agenda_packet_url,
        )

    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        for key, val in d.copy().items():
            if isinstance(val, URL):
                val = str(val)
                d[key] = val
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        for key in ['page_url', 'agenda_packet_url']:
            val = kw[key]
            if val is not None:
                val = URL(val)
            kw[key] = val
        return cls(**kw)


@dataclass
class LegistarData(Serializable):
    """Container for data gathered from Legistar
    """
    clips: ClipCollection
    """:class:`.model.ClipCollection` instance"""

    feed_url: URL
    """URL used to parse the :class:`.rss_parser.Feed`"""

    matched_guids: dict[CLIP_ID, GUID] = field(default_factory=dict)
    """:attr:`Clips <.model.Clip.id>` that have been matched to
    :attr:`FeedItems <.rss_parser.FeedItem.guid>`
    """

    matched_clips: dict[CLIP_ID, DetailPageResult] = field(default_factory=dict)
    """Mapping of parsed :class:`DetailPageResult` items with their
    :attr:`~DetailPageResult.clip_id` as keys
    """

    parse_error_clips: dict[CLIP_ID, ParseErrorType] = field(default_factory=dict)
    """Errors encountered when attempting to match clips to FeedItems.
    Values are :obj:`~.rss_parser.ParseErrorType`, using the clip ids as keys
    """

    def add_guid_match(self, clip_id: CLIP_ID, guid: GUID) -> None:
        """Add a ``Clip.id -> FeedItem`` match
        """
        if clip_id in self.matched_guids:
            assert self.matched_guids[clip_id] == guid
            return
        self.matched_guids[clip_id] = guid

    def add_match(self, item: DetailPageResult) -> None:
        """Add a parsed :class:`DetailPageResult` to :attr:`matched_clips`
        """
        assert item.clip_id not in self
        self.matched_clips[item.clip_id] = item
        if item.clip_id in self.parse_error_clips:
            del self.parse_error_clips[item.clip_id]

    def get_unmatched_clips(self) -> Iterator[CLIP_ID]:
        """Iterate over all clip ids that have not been added to
        :attr:`matched_clips`
        """
        matched = set(self.matched_clips.keys())
        all_clip_ids = set(self.clips.clips.keys())
        yield from all_clip_ids - matched

    def get_unmatched_clips_with_guids(self) -> Iterator[tuple[CLIP_ID, GUID]]:
        """Get all clip ids which have been matched to FeedItems, but have
        not yet been parsed into :attr:`matched_clips`
        """
        for clip_id in self.get_unmatched_clips():
            if clip_id in self.matched_guids:
                yield clip_id, self.matched_guids[clip_id]

    def add_parse_error(self, exc: ParseError) -> None:
        """Store error information encountered during parsing
        """
        self.parse_error_clips[exc.clip_id] = exc.error_type

    def __len__(self):
        return len(self.matched_clips)

    def __iter__(self):
        yield from self.matched_clips.values()

    def __contains__(self, key: CLIP_ID|DetailPageResult):
        if isinstance(key, DetailPageResult):
            key = key.clip_id
        return key in self.matched_clips

    def __getitem__(self, key: CLIP_ID) -> DetailPageResult:
        return self.matched_clips[key]

    @classmethod
    def load(cls, filename: PathLike, clips: ClipCollection) -> Self:
        """Loads an instance from previously saved data
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = json.loads(filename.read_text())
        return cls.deserialize(data, clips)

    def save(self, filename: PathLike, indent: int|None = 2) -> None:
        """Saves all clip data as JSON to the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = self.serialize()
        filename.write_text(json.dumps(data, indent=indent))

    def serialize(self) -> dict[str, Any]:
        d = {k:v.serialize() for k,v in self.matched_clips.items()}
        return dict(
            feed_url=str(self.feed_url),
            matched_guids=self.matched_guids,
            matched_clips=d,
            parse_error_clips=self.parse_error_clips,
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any], clips: ClipCollection) -> Self:
        matched_clips = data['matched_clips']
        return cls(
            clips=clips,
            feed_url=URL(data['feed_url']),
            matched_guids=data['matched_guids'],
            matched_clips={
                k:DetailPageResult.deserialize(v)
                for k,v in matched_clips.items()
            },
            parse_error_clips=data['parse_error_clips'],
        )

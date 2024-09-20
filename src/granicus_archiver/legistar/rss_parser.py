from __future__ import annotations
from typing import NewType, Self, Iterator, Iterable, Any, Literal, ClassVar

import dataclasses
from dataclasses import dataclass
import datetime
from zoneinfo import ZoneInfo

from loguru import logger
from yarl import URL
from pyquery.pyquery import PyQuery

from ..model import Serializable, Clip, CLIP_ID

GUID = NewType('GUID', str)
Category = str
ItemDict = dict[GUID, 'FeedItem']

UTC = ZoneInfo('UTC')


# Fri, 30 Aug 2024 15:22:36 GMT
PUBDATE_FMT = '%a, %d %b %Y %H:%M:%S GMT'

ParseErrorType = Literal['unknown', 'category', 'datetime']
"""Parse error names"""

class ParseError(Exception):
    """Exception raised during parsing
    """
    clip_id: CLIP_ID
    """The clip id associated with the error"""
    error_type: ClassVar[ParseErrorType] = 'unknown'
    """String indicator of the type of error"""
    def __init__(self, clip_id: CLIP_ID, msg: str) -> None:
        super().__init__()
        self.clip_id = clip_id
        self.msg = msg

    def __str__(self) -> str:
        return f'{self.msg} (clip_id={self.clip_id})'

class CategoryError(ParseError):
    """Exception raised when matching :attr:`FeedItem.category` to a clip
    location
    """
    error_type = 'category'

class DatetimeError(ParseError):
    """Exception raised when matching :attr:`FeedItem.meeting_date` to a clip
    datetime
    """
    error_type = 'datetime'


def set_timezone(tz: ZoneInfo) -> None:
    FeedItem.set_timezone(tz)

def parse_pubdate(dtstr: str) -> datetime.datetime:
    dt = datetime.datetime.strptime(dtstr, PUBDATE_FMT)
    return dt.replace(tzinfo=UTC)


def get_elem_text(elem: PyQuery, selector: str|None = None) -> str:
    if selector is not None:
        elem = elem(selector).eq(0)
    else:
        elem = elem.eq(0)
    txt = elem.text()
    assert isinstance(txt, str)
    return txt.strip(' ')


@dataclass
class FeedItem(Serializable):
    """An RSS feed item representing a meeting in the Legistar calendar

    A typical item representation would be

    .. code-block:: xml

        <item>
            <title>City Council - 9/9/2024 - 2:00 PM</title>
            <link>https://mansfield.legistar.com/Gateway.aspx?.......</link>
            <guid isPermaLink="false">...</guid>
            <description/>
            <category>City Council</category>
            <pubDate>Tue, 10 Sep 2024 16:52:26 GMT</pubDate>
        </item>

    Note the value for the ``<title>`` element.  It contains the title
    (or what *should* be the title) followed by a date and a time.  Note also
    that the ``<pubDate>`` field would appear as ``9/10/2024 - 11:52 AM``
    after time zone conversion (instead of ``9/9/2024 - 2:00 PM``).
    This is likely the last time the item was altered in Legistar
    (explaining the discrepancy).

    This makes the ``pubDate`` useless for determining the scheduled date/time
    for the event and we are forced instead to extract it from the ``title``
    and hope for the best.

    Since there is no timezone information available for it however, we're
    also forced to assume that the timezone is fixed as the municipality's
    local time (and we all know what assuming does).

    """
    title: str
    """The meeting title.  This is confusingly a combination of the meeting name,
    date and time in the RSS feed (see notes above).
    When parsed, the date and time are stripped, leaving only the title string
    """
    link: URL
    """URL for the meeting details page
    """
    guid: GUID
    """A globally-unique id for the item
    """
    category: Category
    """The meeting "category" (sometimes also referred to as "Department")
    Note that this may or may not match the value of
    :attr:`Clip.location <granicus_archiver.model.Clip.location>`,
    but that is the intent.
    """
    meeting_date: datetime.datetime
    """The scheduled date and time of the meeting, parsed from the original
    :attr:`title` and converted to the local :attr:`timezone <TZ>`
    """
    pub_date: datetime.datetime
    """Date and time the meeting was published (not the meeting date/time)
    """


    _TZ: ClassVar[ZoneInfo|None] = None
    """Local timezone used to parse :attr:`meeting_date`"""

    @classmethod
    def set_timezone(cls, tz: ZoneInfo) -> None:
        cls._TZ = tz

    @classmethod
    def get_timezone(cls) -> ZoneInfo:
        assert cls._TZ is not None
        return cls._TZ

    @classmethod
    def from_rss(cls, elem: PyQuery) -> Self:
        """Parse and create an item from its RSS data
        """
        title_and_dt = get_elem_text(elem, 'title')
        title, dt = cls.parse_dt_from_title_because_granicus_is_lazy_and_doesnt_include_the_event_datetime_in_their_rss_feeds(title_and_dt)

        return cls(
            title=title,
            link=URL(get_elem_text(elem, 'link')),
            guid=GUID(get_elem_text(elem, 'guid')),
            category=get_elem_text(elem, 'category'),
            meeting_date=dt,
            pub_date=parse_pubdate(get_elem_text(elem, 'pubDate')),
        )

    @classmethod
    def parse_dt_from_title_because_granicus_is_lazy_and_doesnt_include_the_event_datetime_in_their_rss_feeds(cls, title: str) -> tuple[str, datetime.datetime]:
        # seriously...
        # they put the meeting date and time in the title string like this:
        # "{RealMeetingTitle} - MM/DD/YYYY - HH:MM AM"
        # I mean, why not?  Cities just give them money regardless, right?
        title_spl = title.split(' - ')
        assert len(title_spl) >= 3
        real_title = ' - '.join(title_spl[:-2])
        dt_str = ' - '.join(title_spl[-2:])
        dt_fmt = '%m/%d/%Y - %I:%M %p'
        dt = datetime.datetime.strptime(dt_str, dt_fmt)
        tz = cls.get_timezone()
        dt = dt.replace(tzinfo=tz).astimezone(UTC)
        return real_title, dt


    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        for key, val in d.copy().items():
            if isinstance(val, datetime.datetime):
                val = val.astimezone(UTC)
                val = val.strftime(PUBDATE_FMT)
            elif isinstance(val, URL):
                val = str(val)
            else:
                continue
            d[key] = val
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        kw['link'] = URL(kw['link'])
        kw['meeting_date'] = parse_pubdate(kw['meeting_date'])
        kw['pub_date'] = parse_pubdate(kw['pub_date'])
        return cls(**kw)


class Feed(Serializable):
    """An representation of Legistar's calendar RSS feed

    The URL for this should have the options configured to show "All Years" and
    "All Departments" on the main ``/Calendar.aspx`` page.
    """
    item_list: list[FeedItem]
    """The feed items as :class:`FeedItem` instances"""
    items: ItemDict
    """Mapping of items using their :attr:`~FeedItem.guid` as keys"""
    # item_order: list[GUID]
    items_by_category: dict[Category, ItemDict]
    """Mapping of items by their :attr:`~FeedItem.category`"""

    category_maps: dict[str, Category]
    """A :class:`dict` of any custom mappings to match the
    :attr:`Clip.location <granicus_archiver.model.Clip.location>`
    fields to their appropriate :attr:`FeedItem.category`

    The keys for this should be the ``location`` with the values set to the
    ``category``.
    """
    def __init__(
        self,
        items: Iterable[FeedItem]|None = None,
        category_maps: dict[str, Category]|None = None
    ) -> None:
        if items is None:
            items = []
        if category_maps is None:
            category_maps = {}
        self.item_list = [item for item in items]
        self.items = {item.guid: item for item in self.item_list}
        assert len(self.items) == len(self.item_list)
        if len(self.items) == 100:
            logger.warning(f'Feed item count is exactly 100.  This may be missing items because Legistar thinks they can paginate RSS feeds!!!')
        self.items_by_category = self._get_items_by_category()
        self.category_maps = category_maps

    @classmethod
    def from_feed(
        cls,
        doc_str: str|bytes,
        category_maps: dict[str, Category]|None = None
    ) -> Self:
        """Create an instance by parsing the supplied RSS data
        """
        if isinstance(doc_str, str):
            doc_str = doc_str.encode()
        doc = PyQuery(doc_str, parser='xml')
        return cls(items=parse_feed_items(doc), category_maps=category_maps)

    def _get_items_by_category(self) -> dict[Category, ItemDict]:
        result: dict[Category, ItemDict] = {}
        for item in self.item_list:
            d = result.setdefault(item.category, {})
            assert item.guid not in d
            d[item.guid] = item
        return result

    def find_clip_match(self, clip: Clip) -> FeedItem:
        """Attempt to match the given clip to a :class:`FeedItem`

        The :attr:`Clip.location <granicus_archiver.model.Clip.location>` is
        first used to filter items by :attr:`~FeedItem.category` (using any
        custom overrides in :attr:`category_maps`).

        A match between the
        :attr:`Clip.datetime <granicus_archiver.model.Clip.datetime>` and
        :attr:`FeedItem.meeting_date` is then searched and the closest match
        is returned if within +/- four hours.

        Raises:
            CategoryError: If no category match was found
            DatetimeError: If a match could not be found for clip's the datetime

        """
        cat: Category = clip.location
        if cat in self.category_maps:
            cat = self.category_maps[cat]
        if cat not in self.items_by_category:
            raise CategoryError(clip.id, f'Category "{cat}" not found: {clip.name=}, {clip.datetime=}')
            # raise ValueError('Category not found')
        items = self.items_by_category[cat]
        items_by_dt = {item.meeting_date:item for item in items.values()}
        clip_dt = clip.datetime
        deltas = {clip_dt - dt: dt for dt in items_by_dt}
        min_delta = min([abs(delta) for delta in deltas])
        if min_delta not in deltas:
            min_delta = -min_delta
        dt_key = deltas[min_delta]
        if abs(min_delta) > datetime.timedelta(hours=4):
            _dt_key = dt_key.astimezone(clip.datetime.tzinfo)
            raise DatetimeError(clip.id, f'No datetime in range: {clip.name=}, {clip.datetime=}, {_dt_key=}')
            # raise ValueError('No datetime in range')


        return items_by_dt[dt_key]

    def __getitem__(self, key: GUID) -> FeedItem:
        return self.items[key]

    def __contains__(self, key: GUID):
        return key in self.items

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        yield from self.items.values()

    def serialize(self) -> dict[str, Any]:
        return {'item_list': [item.serialize() for item in self.item_list]}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        item_list = data['item_list']
        return cls(
            items=[FeedItem.deserialize(d) for d in item_list],
        )



def parse_feed_items(doc: PyQuery) -> Iterator[FeedItem]:
    for item_index, item_el in enumerate(doc('channel > item').items()):
        yield FeedItem.from_rss(item_el)

from __future__ import annotations
from typing import Self, Iterator, Iterable, Any, ClassVar, TYPE_CHECKING
if TYPE_CHECKING:
    try:
        from typing import TypeIs
    except ImportError:
        from typing_extensions import TypeIs    # type: ignore

import dataclasses
from dataclasses import dataclass
import datetime
from zoneinfo import ZoneInfo

from yarl import URL
from pyquery.pyquery import PyQuery

from ..types import Serializable
from ..clips.model import Clip, Location
from .types import GUID, REAL_GUID, Category
from .exceptions import (
    LegistarThinksRSSCanPaginateError, CategoryError, DatetimeError
)

ItemDict = dict[GUID, 'FeedItem']

UTC = ZoneInfo('UTC')


# Fri, 30 Aug 2024 15:22:36 GMT
PUBDATE_FMT = '%a, %d %b %Y %H:%M:%S GMT'
FUTURE_TIMEDELTA = datetime.timedelta(days=1)
"""Amount of time after a :attr:`~Item.meeting_date` that must pass before
it is no longer considered a future item
"""



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


def is_guid(item: str) -> TypeIs[GUID]:
    """Check whether the given value is a valid :obj:`~.types.GUID`
    """
    num_real_guid_segments = 5
    num_dt_segments = 6
    dt_segment_lengths = [4, 2, 2, 2, 2, 2]
    segments = item.split('-')
    if len(segments) != num_real_guid_segments + num_dt_segments:
        return False
    if not is_real_guid('-'.join(segments[:num_real_guid_segments])):
        return False
    dt_segments = segments[-num_dt_segments:]
    for segment, seg_len in zip(dt_segments, dt_segment_lengths):
        if len(segment) != seg_len:
            return False
        if not segment.isdigit():
            return False
    return True

def is_real_guid(item: str) -> TypeIs[REAL_GUID]:
    """Check whether the given value is a valid :obj:`~.types.REAL_GUID`
    """
    segment_lengths = [8, 4, 4, 4, 12]
    num_segments = 5
    segments = item.split('-')
    if len(segments) != num_segments:
        return False
    hex_str = f'0x{"".join(segments)}'
    try:
        _ = int(hex_str, 0)
    except ValueError:
        return False
    for segment, seg_len in zip(segments, segment_lengths):
        if len(segment) != seg_len:
            return False
    return True


def get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info(guid: GUID) -> REAL_GUID:
    segment_lengths = [8, 4, 4, 4, 12]
    num_segments = 5
    segments = guid.split('-')[:num_segments]
    for segment, seg_len in zip(segments, segment_lengths):
        assert len(segment) == seg_len
    # rg = '-'.join(guid.split('-')[:num_segments])
    return REAL_GUID('-'.join(segments))

def parse_dt_from_guid(guid: GUID) -> datetime.datetime:
    dt_str = '-'.join(guid.split('-')[-6:])
    dt_fmt = '%Y-%m-%d-%H-%M-%S'
    return datetime.datetime.strptime(dt_str, dt_fmt)


class GuidCompare:
    """Helper to compare :obj:`GUID's <.types.GUID>`

    Since the "GUID's" (loose term because they aren't really GUID's) contain
    date/time information, it can actually be useful to determine whether
    an update is needed from a feed item or not.

    Instances can be compared using the ``==``, ``!=``, ``>``, ``>=``,
    ``<`` and ``<=`` operators.

    Using the following GUID:

    >>> real_guid_a = 'F239FB22-A00A-6FF1-3E97-0F36043B96F6'
    >>> a = f'{real_guid_a}-2023-01-01-12-30-00'
    >>> b = f'{real_guid_a}-2024-01-01-12-30-00'

    Both ``a`` and ``b`` use the same GUID, but ``a`` is one year behind ``b``

    >>> GuidCompare(a) == a
    True
    >>> GuidCompare(a) != b
    True
    >>> GuidCompare(a) == b
    False
    >>> GuidCompare(a) > b
    False
    >>> GuidCompare(a) < b
    True
    >>> GuidCompare(b) > a
    True

    If the GUID portion does not match, equality checks will reflect that in
    equality checks:

    >>> real_guid_b = '8F6DD61F-3498-3FF1-12B9-38DBE1CA9B06'
    >>> c = f'{real_guid_b}-2024-01-01-12-30-00'
    >>> GuidCompare(a) == c
    False
    >>> GuidCompare(b) == c
    False
    >>> GuidCompare(c) == a
    False
    >>> GuidCompare(c) == b
    False

    ``<``, ``>`` checks however are not supported in this case:

    >>> GuidCompare(c) > a
    Traceback (most recent call last):
        ...
    TypeError: '>' not supported ...

    """
    __slots__ = ('guid', 'real_guid', 'dt')
    def __init__(self, guid: GUID) -> None:
        self.guid = guid
        self.real_guid = get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info(guid)
        self.dt = parse_dt_from_guid(guid)

    @classmethod
    def _coerce_other(cls, other: Self|GUID) -> Self:
        if not isinstance(other, GuidCompare):
            other = cls(other)
        return other

    def __eq__(self, other: Self|GUID):
        other = self._coerce_other(other)
        return self.guid == other.guid

    def __ne__(self, other: Self|GUID):
        other = self._coerce_other(other)
        return self.guid != other.guid

    def __gt__(self, other: Self|GUID):
        other = self._coerce_other(other)
        if self.real_guid != other.real_guid:
            return NotImplemented
        return self.dt > other.dt

    def __ge__(self, other: Self|GUID):
        other = self._coerce_other(other)
        if self.real_guid != other.real_guid:
            return NotImplemented
        return self.dt >= other.dt

    def __lt__(self, other: Self|GUID):
        other = self._coerce_other(other)
        if self.real_guid != other.real_guid:
            return NotImplemented
        return self.dt < other.dt

    def __le__(self, other: Self|GUID):
        other = self._coerce_other(other)
        if self.real_guid != other.real_guid:
            return NotImplemented
        return self.dt <= other.dt

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: {self}>'

    def __str__(self) -> str:
        return self.guid


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

    ITEM_IN_PAST_DELTA: ClassVar[datetime.timedelta] = datetime.timedelta(days=365)
    """Amount of time to consider an item as "in the past" (default is one year)
    """

    TZ: ClassVar[ZoneInfo|None] = None
    """Local timezone used to parse :attr:`meeting_date`"""

    @classmethod
    def set_timezone(cls, tz: ZoneInfo) -> None:
        cls.TZ = tz

    @classmethod
    def get_timezone(cls) -> ZoneInfo:
        assert cls.TZ is not None
        return cls.TZ

    @property
    def is_future(self) -> bool:
        """Whether the item is in the future
        """
        now = datetime.datetime.now(UTC)
        dt = self.meeting_date + FUTURE_TIMEDELTA
        return dt >= now

    @property
    def is_in_past(self) -> bool:
        """Whether the item is older than :attr:`ITEM_IN_PAST_DELTA`
        """
        now = datetime.datetime.now(UTC)
        td = now - self.meeting_date
        return td >= self.ITEM_IN_PAST_DELTA

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
            category=Category(get_elem_text(elem, 'category')),
            meeting_date=dt,
            pub_date=parse_pubdate(get_elem_text(elem, 'pubDate')),
        )

    @classmethod
    def to_csv(cls, *items: FeedItem) -> str:
        """Get a comma-separated representation for the given feed items

        The result will include a header followed by the results of
        :meth:`to_csv_line` for each item given.
        """
        header = 'Title, Date, Link'
        lines = [header]
        for item in items:
            lines.append(item.to_csv_line())
        return '\n'.join(lines)

    def to_csv_line(self) -> str:
        """Get the comma-separated values of this item

        The attributes returned will be

        - :attr:`title`
        - :attr:`meeting_date` (the :meth:`~datetime.datetime.date` portion only)
        - :attr:`link`

        """
        return f'{self.title}, {self.meeting_date.date()}, {self.link}'

    @property
    def real_guid(self) -> REAL_GUID:
        """The portion of :attr:`guid` that IS ACTUALLY A GUID
        (With the ridiculous date-time portion of it removed.. really, I'm not making this up)
        """
        return get_the_real_guid_part_of_their_guid_that_adds_pointless_datetime_info(self.guid)


    @classmethod
    def parse_dt_from_title_because_granicus_is_lazy_and_doesnt_include_the_event_datetime_in_their_rss_feeds(cls, title: str) -> tuple[str, datetime.datetime]:
        # seriously...
        # they put the meeting date and time in the title string like this:
        # "{RealMeetingTitle} - MM/DD/YYYY - HH:MM AM"
        # I mean, why not?  Cities just give them money regardless, right?
        dt_fmt = '%m/%d/%Y - %I:%M %p'
        tz = cls.get_timezone()
        title_spl = title.split(' - ')
        if len(title_spl) == 2:
            # This is for another stupid case where no meeting time was set
            # smh... I'm just chasing down years of incompetent programming
            #
            # Check to make sure there's at least a valid date string
            maybe_date_str = title_spl[1]
            assert len(maybe_date_str.split('/')) == 3
            maybe_date_str = f'{maybe_date_str} - 12:00 PM'
            maybe_datetime = datetime.datetime.strptime(maybe_date_str, dt_fmt).replace(tzinfo=cls.get_timezone())
            title_spl = [title_spl[0]]
            title_spl.extend(maybe_datetime.strftime(dt_fmt).split(' - '))
        assert len(title_spl) >= 3
        real_title = ' - '.join(title_spl[:-2])
        dt_str = ' - '.join(title_spl[-2:])
        dt = datetime.datetime.strptime(dt_str, dt_fmt)
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

    def to_str(self) -> str:
        dt = self.meeting_date.astimezone(self.get_timezone())
        return f'"{self.link}": {self.title} @ {dt.date()}'


class Feed(Serializable):
    """An representation of Legistar's calendar RSS feed

    The URL for this should have the options configured to show "All Years" and
    "All Departments" on the main ``/Calendar.aspx`` page.  That is, unless
    there are more than 100 meetings in your agenda history (which is *very*
    likely to be the case).

    The RSS feed that legistar generates, with all of their years of wisdom,
    limits the number of results to 100 items making it almost completely
    useless for archival purposes.

    The only known method to get around this is to parse separate feeds by
    choosing the "Departments" and sometimes each year individually.  This
    seems (and is!) a horribly laborious process, but it's definitely easier
    than manually downloading and naming over 4000 files for around 2000 meetings!
    """
    item_list: list[FeedItem]
    """The feed items as :class:`FeedItem` instances"""
    items: ItemDict
    """Mapping of items using their :attr:`~FeedItem.guid` as keys"""
    # item_order: list[GUID]
    items_by_category: dict[Category, ItemDict]
    """Mapping of items by their :attr:`~FeedItem.category`"""

    category_maps: dict[Location, Category]
    """A :class:`dict` of any custom mappings to match the
    :attr:`Clip.location <granicus_archiver.model.Clip.location>`
    fields to their appropriate :attr:`FeedItem.category`

    The keys for this should be the ``location`` with the values set to the
    ``category``.
    """
    def __init__(
        self,
        items: Iterable[FeedItem]|None = None,
        category_maps: dict[Location, Category]|None = None
    ) -> None:
        if items is None:
            items = []
        if category_maps is None:
            category_maps = {}
        self.item_list = [item for item in items]
        self.items = {item.guid: item for item in self.item_list}
        assert len(self.items) == len(self.item_list)
        self.items_by_category = self._get_items_by_category()
        self.category_maps = category_maps

    @classmethod
    def from_feed(
        cls,
        doc_str: str|bytes,
        category_maps: dict[Location, Category]|None = None,
        overflow_allowed: bool = False
    ) -> Self:
        """Create an instance by parsing the supplied RSS data

        Arguments:
            doc_str: The raw RSS/XML string
            category_maps: Value for the feed's :attr:`category_maps`
            overflow_allowed: If ``True`` disables raising
                :class:`LegistarThinksRSSCanPaginateError` if the feed's item
                count is 100.  The default (``False``) allows exception
                to be raised.

        Raises:
            LegistarThinksRSSCanPaginateError: If the feed's item count is 100
                and *overflow_allowed* is ``False``
        """
        if isinstance(doc_str, str):
            doc_str = doc_str.encode()
        doc = PyQuery(doc_str, parser='xml')
        feed = cls(items=parse_feed_items(doc), category_maps=category_maps)
        if len(feed.items) == 100 and not overflow_allowed:
            raise LegistarThinksRSSCanPaginateError()
        return feed

    def _get_items_by_category(self) -> dict[Category, ItemDict]:
        result: dict[Category, ItemDict] = {}
        for item in self.item_list:
            d = result.setdefault(item.category, {})
            assert item.guid not in d
            d[item.guid] = item
        return result

    def find_clip_match(
        self,
        clip: Clip,
        search_delta: datetime.timedelta = datetime.timedelta(hours=4)
    ) -> FeedItem:
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
        def _is_category(value: str) -> TypeIs[Category]:
            return (
                value in self.items_by_category or
                value in self.category_maps.values()
            )

        cat = clip.location
        if cat in self.category_maps:
            cat = self.category_maps[cat]
        if cat not in self.items_by_category:
            raise CategoryError(clip.id, f'Category "{cat}" not found: {clip.name=}, {clip.datetime=}')
            # raise ValueError('Category not found')
        assert _is_category(cat)
        items = self.items_by_category[cat]
        items_by_dt = {item.meeting_date:item for item in items.values()}
        clip_dt = clip.datetime
        deltas = {clip_dt - dt: dt for dt in items_by_dt}
        min_delta = min([abs(delta) for delta in deltas])
        if min_delta not in deltas:
            min_delta = -min_delta
        dt_key = deltas[min_delta]
        if abs(min_delta) > search_delta:
            _dt_key = dt_key.astimezone(clip.datetime.tzinfo)
            raise DatetimeError(clip.id, f'No datetime in range: {clip.name=}, {clip.datetime=}, {_dt_key=}')
            # raise ValueError('No datetime in range')


        item = items_by_dt[dt_key]
        assert item.meeting_date == dt_key
        assert abs(clip_dt - item.meeting_date) <= datetime.timedelta(hours=4)
        return item

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

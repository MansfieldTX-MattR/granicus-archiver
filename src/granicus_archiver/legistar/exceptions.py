from typing import ClassVar, Literal

from ..model import CLIP_ID


class ThisShouldBeA500ErrorButItsNot(Exception):
    """Raised when a detail page request returns a ``200 - OK`` response,
    but with error information in the HTML content

    Yes, that really happens
    """
    def __str__(self):
        return "Yes, they really do that. Isn't it lovely?"


class IncompleteItemError(Exception):
    """Raised in :meth:`.model.DetailPageResult.from_html` if a detail page is in an
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



RSSParseErrorType = Literal['unknown', 'category', 'datetime']
"""Parse error names"""


class RSSParseError(Exception):
    """Exception raised during parsing
    """
    clip_id: CLIP_ID
    """The clip id associated with the error"""
    error_type: ClassVar[RSSParseErrorType] = 'unknown'
    """String indicator of the type of error"""
    def __init__(self, clip_id: CLIP_ID, msg: str) -> None:
        super().__init__()
        self.clip_id = clip_id
        self.msg = msg

    def __str__(self) -> str:
        return f'{self.msg} (clip_id={self.clip_id})'


class CategoryError(RSSParseError):
    """Exception raised when matching :attr:`.rss_parser.FeedItem.category`
    to a clip location
    """
    error_type = 'category'

class DatetimeError(RSSParseError):
    """Exception raised when matching :attr:`.rss_parser.FeedItem.meeting_date`
    to a clip datetime
    """
    error_type = 'datetime'


class LegistarThinksRSSCanPaginateError(Exception):
    """Exception raised when an RSS feed contains exactly 100 items

    This *could* mean there are exactly 100 items available, but Legistar's
    feed generator limits the results for **any** RSS feed to 100 items
    **even if there are more available**!

    As a precaution, this is treated as an error so the feeds can be
    divided up as described in :class:`.rss_parser.Feed`.
    """
    def __str__(self) -> str:
        args = self.args
        arg_str = '' if not len(args) else f' {args!r}'
        msg = 'Feed item count is exactly 100.  This may be missing items because Legistar thinks they can paginate RSS feeds!!!'
        return f'{msg}{arg_str}'

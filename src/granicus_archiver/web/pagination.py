from __future__ import annotations
from typing import Iterable, Iterator, Sequence
import math

from aiohttp import web
from yarl import URL


class Paginator[ItemT]:
    """Helper class for paginating a sequence of items
    """

    request: web.Request
    """The request object"""

    items: Sequence[ItemT]
    """The items to paginate"""

    total_items: int
    """The total number of items"""

    current_index: int
    """The current page index"""

    per_page: int
    """The number of items per page"""

    page_query_param: str
    """The query parameter for the page number"""

    per_page_query_param: str
    """The query parameter for the number of items per page"""

    pages: list[Page[ItemT]]
    """A list of :class:`Page` objects"""

    def __init__(
        self,
        request: web.Request,
        items: Sequence[ItemT],
        total_items: int|None = None,
        current_index: int|None = None,
        per_page: int = 10,
        page_query_param: str = 'page',
        per_page_query_param: str = 'per_page',
    ) -> None:
        self.request = request
        self.items = items
        if total_items is None:
            total_items = len(items)
        if current_index is None:
            current_index = int(request.query.get(page_query_param, 0))
        if request.query.get(per_page_query_param):
            per_page = int(request.query[per_page_query_param])
        self.current_index = current_index
        self.total_items = total_items
        self.per_page = per_page
        self.page_query_param = page_query_param
        self.per_page_query_param = per_page_query_param
        self.pages: list[Page[ItemT]] = []
        for i in range(self.num_pages):
            self.pages.append(Page(self, i))

    @property
    def num_pages(self) -> int:
        """Number of pages
        """
        if self.total_items <= self.per_page:
            return 1
        return math.ceil(self.total_items / self.per_page)

    @property
    def current_page(self) -> Page[ItemT]:
        """The current :class:`Page` object
        """
        return self.pages[self.current_index]

    @property
    def paginating(self) -> bool:
        """Flag to indicate whether pagination is active
        """
        return len(self.pages) > 1

    def iter_pages(self, max_pages: int = 10) -> Iterable[Page[ItemT]]:
        """Iterate over :attr:`pages`

        Iteration will be centered around the current page index within the
        given *max_pages*.

        Arguments:
            max_pages: The maximum number of pages to iterate over

        """
        start = max(0, self.current_index - max_pages // 2)
        end = min(self.num_pages, start + max_pages)
        return self.pages[start:end]

    def __iter__(self) -> Iterator[ItemT]:
        yield from self.current_page

    def _build_url(self, page_num: int) -> URL:
        q = {k:v for k,v in self.request.query.items()}
        q[str(self.page_query_param)] = str(page_num)
        return self.request.url.with_query(q)


class Page[ItemT]:
    """A page of items within a :class:`Paginator`
    """

    paginator: Paginator[ItemT]
    page_num: int
    """The page number"""

    def __init__(
        self,
        paginator: Paginator[ItemT],
        page_num: int
    ) -> None:
        self.paginator = paginator
        self.page_num = page_num

    @property
    def active(self) -> bool:
        """Whether this page is the :attr:`~Paginator.current_page`"""
        return self.page_num == self.paginator.current_index

    @property
    def start_index(self) -> int:
        """The index of the first item within :attr:`~Paginator.items` for this page
        """
        return self.page_num * self.paginator.per_page

    @property
    def end_index(self) -> int:
        """The index of the last item within :attr:`~Paginator.items` for this page
        """
        return self.start_index + self.paginator.per_page

    @property
    def has_next(self) -> bool:
        """Whether there is a next page
        """
        return self.page_num < self.paginator.num_pages - 1

    @property
    def has_prev(self) -> bool:
        """Whether there is a previous page
        """
        return self.page_num > 0

    @property
    def prev_page_num(self) -> int:
        """The index of the previous page
        """
        return self.page_num - 1

    @property
    def next_page_num(self) -> int:
        """The index of the next page
        """
        return self.page_num + 1

    @property
    def next_page(self) -> Page[ItemT]:
        """The next :class:`Page`
        """
        return self.paginator.pages[self.next_page_num]

    @property
    def prev_page(self) -> Page[ItemT]:
        """The previous :class:`Page`
        """
        if self.page_num == 0:
            raise IndexError('No previous page')
        return self.paginator.pages[self.prev_page_num]

    @property
    def url(self) -> URL:
        """The URL for this page
        """
        return self.paginator._build_url(self.page_num)

    @property
    def slice(self) -> slice:
        """A :class:`slice` object for the :attr:`~Paginator.items` of this page
        """
        return slice(self.start_index, self.end_index)

    @property
    def length(self) -> int:
        """The number of items on this page
        """
        return len(self)

    def __iter__(self) -> Iterator[ItemT]:
        yield from self.paginator.items[self.slice]

    def __len__(self) -> int:
        return len(self.paginator.items[self.slice])

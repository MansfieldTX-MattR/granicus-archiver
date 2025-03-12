from __future__ import annotations
from typing import (
    TypeVar, Generic, Literal, ClassVar, TypedDict,
    Iterable, Sequence, Any, cast,
)

from abc import ABC, abstractmethod
import datetime
import dataclasses
from functools import wraps

from loguru import logger
from aiohttp import web
from multidict import MultiDict
import aiohttp_jinja2
from yarl import URL

from ..config import Config
from ..clips.model import CLIP_ID, Location, Clip, ClipCollection
from ..legistar.model import (
    LegistarData, DetailPageResult, LegistarFiles,
    AbstractLegistarModel, AbstractFile, LegistarFile, AttachmentFile,
    file_key_to_uid, attachment_name_to_uid,
)
from ..legistar.rss_parser import is_guid, is_real_guid
from ..legistar.guid_model import RGuidLegistarData, RGuidDetailResult
from ..legistar.types import Category, GUID, REAL_GUID, NoClip, NoClipT, LegistarFileUID
from .types import *
from .config import APP_CONF_KEY
from .s3client import S3ClientKey
from .pagination import Paginator


class GlobalContext(TypedDict):
    """Context data for all templated views
    """
    nav_links: Sequence[NavLink]
    """Navigation links"""
    page_title: str
    """Title of the page"""


ContextT = TypeVar('ContextT', bound=GlobalContext)
"""Type variable for template context data"""

_ID_Type = TypeVar('_ID_Type', GUID, REAL_GUID, CLIP_ID)

ClipIdOrNoneStr = CLIP_ID|Literal['None']|Literal['NoClip']
"""Type variable for a clip id"""
GuidOrNoneStr = GUID|Literal['None']|Literal['NoClip']
"""Type variable for a guid"""
RealGuidOrNoneStr = REAL_GUID|Literal['None']|Literal['NoClip']

routes = web.RouteTableDef()


def read_only_guard(func):
    """Decorator to prevent modification of data when in
    :attr:`~.config.AppConfig.read_only` mode
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        _self_or_request: web.View|web.Request = args[0]
        if isinstance(_self_or_request, web.View):
            request = _self_or_request.request
        elif isinstance(_self_or_request, web.Request):
            request = _self_or_request
        else:
            raise TypeError(f'invalid type: {type(_self_or_request)}')
        app_conf = request.app[APP_CONF_KEY]
        if app_conf.read_only:
            raise web.HTTPUnauthorized()
        return await func(*args, **kwargs)
    return wrapper


def with_data_file_lock(func):
    """Decorator to hold the :obj:`DataFileLock <.types.DataFileLockKey>`
    within wrapped the function
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        _self_or_request: web.View|web.Request = args[0]
        if isinstance(_self_or_request, web.View):
            request = _self_or_request.request
        elif isinstance(_self_or_request, web.Request):
            request = _self_or_request
        else:
            raise TypeError(f'invalid type: {type(_self_or_request)}')
        data_file_lock = request.app[DataFileLockKey]
        logger.debug(f'acquiring data file lock for {func}')
        async with data_file_lock:
            try:
                return await func(*args, **kwargs)
            finally:
                logger.debug(f'released data file lock for {func}')
        return r
    return wrapper


def clip_id_to_str(clip_id: CLIP_ID|Clip|None|NoClipT) -> ClipIdOrNoneStr:
    if clip_id is None:
        return 'None'
    if clip_id is NoClip:
        return 'NoClip'
    if isinstance(clip_id, Clip):
        clip_id = clip_id.id
    assert isinstance(clip_id, str)
    return clip_id

def clip_id_from_str(value: ClipIdOrNoneStr) -> CLIP_ID|None|NoClipT:
    if value == 'None':
        return None
    if value == 'NoClip':
        return NoClip
    return CLIP_ID(value)

def guid_to_str(guid: GUID|DetailPageResult|None|NoClipT) -> GuidOrNoneStr:
    if guid is None:
        return 'None'
    if guid is NoClip:
        return 'NoClip'
    if isinstance(guid, DetailPageResult):
        guid = guid.feed_guid
    return guid

def guid_from_str(value: GuidOrNoneStr) -> GUID|None|NoClipT:
    if value == 'None':
        return None
    if value == 'NoClip':
        return NoClip
    return GUID(value)

def real_guid_to_str(guid: REAL_GUID|RGuidDetailResult|None|NoClipT) -> RealGuidOrNoneStr:
    if guid is None:
        return 'None'
    if guid is NoClip:
        return 'NoClip'
    if isinstance(guid, RGuidDetailResult):
        guid = guid.real_guid
    return guid

def real_guid_from_str(value: RealGuidOrNoneStr) -> REAL_GUID|None|NoClipT:
    if value == 'None':
        return None
    if value == 'NoClip':
        return NoClip
    return REAL_GUID(value)

def id_equal(a: _ID_Type|None|NoClipT, b: _ID_Type|None|NoClipT) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if a is NoClip or b is NoClip:
        return a is NoClip and b is NoClip
    return a == b


@routes.get('/clips/webvtt/{clip_id}/', name='clip_webvtt')
async def clip_webvtt(request: web.Request) -> web.Response|web.StreamResponse:
    """View to display a webvtt file for a clip
    """
    app_conf = request.app[APP_CONF_KEY]
    clip_id = CLIP_ID(request.match_info['clip_id'])
    clips = request.app[ClipsKey]
    clip = clips[clip_id]
    if app_conf.use_s3:
        chunk_size = 4096
        s3_client = request.app[S3ClientKey]
        vtt_filename = clip.get_file_path('chapters', absolute=False)
        s3_prefix = s3_client.data_dirs['clips']
        s3_path = s3_prefix / vtt_filename
        # if not await s3_client.object_exists(str(s3_path)):
        #     raise web.HTTPNotFound()

        obj = await s3_client.s3_client.get_object(
            Bucket=s3_client.bucket.name, Key=str(s3_path),
        )

        obj_info = obj['ResponseMetadata']['HTTPHeaders']
        resp = web.StreamResponse(
            headers=MultiDict(
                {
                    "CONTENT-DISPOSITION": (
                        f"attachment; filename='{vtt_filename}'"
                    ),
                    "Content-Type": obj_info["content-type"],
                }
            )
        )
        resp.content_type = obj_info['content-type']
        resp.content_length = int(obj_info['content-length'])
        resp.etag = obj_info['etag'].strip('"')
        resp.last_modified = obj_info['last-modified']
        await resp.prepare(request)
        stream = obj['Body']
        async for chunk in stream.iter_chunks(chunk_size):
            await resp.write(chunk)
    else:
        vtt_filename = clip.get_file_path('chapters', absolute=True)
        webvtt = vtt_filename.read_text()
        resp = web.Response(
            text=webvtt,
            content_type='text/vtt',
        )
    return resp


class AbstractView(ABC, Generic[ContextT]):
    """Abstract base class for views that render templates
    """
    @classmethod
    @abstractmethod
    def get_template_name(cls) -> str:
        """Get the template name for this view
        """
        ...

    @abstractmethod
    async def get_context_data(self) -> ContextT:
        """Get the context data for this view
        """
        ...

    @abstractmethod
    async def render_to_response(self, context: ContextT) -> web.Response:
        """Render the response for this view
        """
        ...



class TemplatedView(web.View, Generic[ContextT], AbstractView[ContextT]):
    """Base class for views that render templates
    """
    template_name: ClassVar[str]

    def get_config(self) -> Config:
        """Get the :class:`granicus_archiver.config.Config` object
        """
        return self.request.app[ConfigKey]

    @classmethod
    def get_template_name(cls) -> str:
        return cls.template_name

    async def render_to_response(self, context: ContextT) -> web.Response:
        """Render the templated response
        """
        tmpl = self.get_template_name()
        return aiohttp_jinja2.render_template(tmpl, self.request, context)

    @with_data_file_lock
    async def get(self) -> web.Response:
        """Handler for GET requests
        """
        context = await self.get_context_data()
        return await self.render_to_response(context)

class ListFilterContext[CatT: (Location, Category)](TypedDict):
    """Context data for list views with filters
    """
    view_unassigned: bool
    """Whether to view unassigned items"""
    all_categories: list[CatT]
    """All possible categories"""
    current_category: CatT|None
    """The current category"""
    filter_by_date: bool
    """Whether to filter by date"""
    start_date: datetime.date|None
    """The start date for filtering"""
    end_date: datetime.date|None
    """The end date for filtering"""


class ClipListContext(GlobalContext):
    """Template context for :class:`ClipListView`
    """
    clip_dict: dict[CLIP_ID, Clip]
    """Mapping of :class:`~.model.Clip` instances by their :attr:`~.model.Clip.id`
    """
    clips: Iterable[Clip]
    """Iterable of :class:`~.model.Clip` instances"""
    clip_guids: dict[CLIP_ID, GuidOrNoneStr]
    """Mapping of clip ids to their associated legistar item guids. A value of
    ``None`` or ``NoClip`` indicates no associated legistar item.
    """
    paginator: Paginator[tuple[CLIP_ID, Clip]]
    """:class:`~.pagination.Paginator` instance for :attr:`clip_dict`"""
    filter_context: ListFilterContext[Location]
    """Filter context data"""


@routes.view('/clips/list/', name='clip_list')
class ClipListView(TemplatedView[ClipListContext]):
    """List view for :class:`granicus_archiver.model.Clip` objects
    """
    template_name = 'clips/clip-list.jinja2'

    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.filter_context = self.parse_filter_context()
        items = list(self.clips.clips.values())
        items = self.get_filtered_items(items)
        self.item_dict: dict[CLIP_ID, Clip] = {clip.id: clip for clip in items}
        self.items: list[Clip] = list(items)
        item_tuples = [(item.id, item) for item in self.items]
        self.paginator = Paginator[tuple[CLIP_ID, Clip]](self.request, item_tuples)

    @property
    def clips(self) -> ClipCollection:
        return self.request.app[ClipsKey]

    @property
    def legistar_data(self) -> LegistarData:
        return self.request.app[LegistarDataKey]

    def parse_filter_context(self) -> ListFilterContext[Location]:
        """Parse the filter context from the request's query parameters
        """
        view_unassigned = bool(self.request.query.get('unassigned'))
        filter_by_date = self.request.query.get('filter_by_date') is not None
        category = self.request.query.get('category')
        if not category:
            category = None
        if category is not None:
            category = Location(category)
        start_date = self.request.query.get('start_date')
        if not start_date:
            start_date = None
        if start_date is not None:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = self.request.query.get('end_date')
        if not end_date:
            end_date = None
        if end_date is not None:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        all_categories = set([clip.location for clip in self.clips])
        return {
            'view_unassigned': view_unassigned,
            'all_categories': list(sorted(all_categories)),
            'current_category': category,
            'filter_by_date': filter_by_date,
            'start_date': start_date,
            'end_date': end_date,
        }

    def get_filtered_items(self, clips: Sequence[Clip]) -> Sequence[Clip]:
        """Get items filtered by the :attr:`filter_context`
        """
        clips = self._filter_by_date_range(clips)
        clips = self._filter_by_category(clips)
        clips = self._filter_unassigned(clips)
        return clips

    def get_clip_guids(self) -> dict[CLIP_ID, GuidOrNoneStr]:
        legistar_data = self.legistar_data
        d: dict[CLIP_ID, GuidOrNoneStr] = {}
        for clip in self.clips:
            item = legistar_data.find_match_for_clip_id(clip.id)
            guid = guid_to_str(item)
            d[clip.id] = guid
        return d

    async def get_context_data(self) -> ClipListContext:
        """Get the context data for this view
        """
        context: ClipListContext = {
            'nav_links': self.request.app[NavLinksKey],
            'page_title': 'Clips',
            'clip_dict': self.item_dict,
            'clips': self.clips,
            'clip_guids': self.get_clip_guids(),
            'paginator': self.paginator,
            'filter_context': self.filter_context,
        }
        return context

    def _filter_by_date_range(self, clips: Sequence[Clip]) -> Sequence[Clip]:
        start_date, end_date = self.filter_context['start_date'], self.filter_context['end_date']
        if not self.filter_context['filter_by_date']:
            return clips
        if start_date is None or end_date is None:
            if self.filter_context['filter_by_date']:
                self.filter_context['filter_by_date'] = False
            return clips
        return [
            clip for clip in clips
            if start_date <= clip.datetime.date() <= end_date
        ]

    def _filter_by_category(self, clips: Sequence[Clip]) -> Sequence[Clip]:
        category = self.filter_context['current_category']
        if category is None:
            return clips
        return [clip for clip in clips if clip.location == category]

    def _filter_unassigned(self, clips: Sequence[Clip]) -> Sequence[Clip]:
        if not self.filter_context['view_unassigned']:
            return clips
        return [
            clip for clip in clips if self.legistar_data.is_clip_id_available(clip.id)
        ]


class ClipViewContext(GlobalContext):
    """Template context for :class:`ClipViewBase`
    """
    clip: Clip
    """The :class:`~.model.Clip` instance being viewed"""
    legistar_item: DetailPageResult|NoClipT|None
    """The associated legistar item, or ``None`` if no match is found"""
    legistar_guid: GUID|NoClipT|None
    """The associated legistar item guid, if one exists"""
    legistar_rguid_item: RGuidDetailResult|NoClipT|None
    """The associated real-guid legistar item, or ``None`` if no match is found"""
    legistar_rguid_item_id: REAL_GUID|NoClipT|None
    """Item id for :attr:`legistar_rguid_item`"""




ClipViewContextT = TypeVar('ClipViewContextT', bound=ClipViewContext)

class ClipViewBase(TemplatedView[ClipViewContextT]):
    """Base class for views that display a single :class:`~.model.Clip` instance
    """
    template_name = 'clips/clip.jinja2'
    legistar_item: DetailPageResult|NoClipT|None
    legistar_rguid_item: RGuidDetailResult|NoClipT|None

    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        clip_id = CLIP_ID(self.request.match_info['clip_id'])
        self.clip = self.clips[clip_id]
        self.legistar_item = self.legistar_data.find_match_for_clip_id(clip_id)
        self.legistar_rguid_item = self.legistar_data_rguid.find_match_for_clip_id(clip_id)

    @property
    def clips(self) -> ClipCollection:
        return self.request.app[ClipsKey]

    @property
    def legistar_data(self) -> LegistarData:
        return self.request.app[LegistarDataKey]

    @property
    def legistar_data_rguid(self) -> RGuidLegistarData:
        return self.request.app[RGuidLegistarDataKey]

    @property
    def legistar_guid(self) -> GUID|None|NoClipT:
        if self.legistar_item is None or self.legistar_item is NoClip:
            return self.legistar_item
        return self.legistar_item.feed_guid

    @property
    def legistar_rguid_item_id(self) -> REAL_GUID|NoClipT|None:
        if self.legistar_rguid_item is None or self.legistar_rguid_item is NoClip:
            return self.legistar_rguid_item
        return self.legistar_rguid_item.real_guid

    async def get_context_data(self) -> ClipViewContext:
        """Get the context data for this view
        """
        return {
            'page_title': self.clip.name,
            'nav_links': self.request.app[NavLinksKey],
            'clip': self.clip,
            'legistar_item': self.legistar_item,
            'legistar_guid': self.legistar_guid,
            'legistar_rguid_item': self.legistar_rguid_item,
            'legistar_rguid_item_id': self.legistar_rguid_item_id,
        }


@routes.view('/clips/clip/{clip_id}/', name='clip_item')
class ClipView(ClipViewBase):
    """View for a single :class:`~.model.Clip` instance
    """
    pass


class ClipEditForm(TypedDict):
    """Form data for :class:`ClipEditView`
    """
    guid: GuidOrNoneStr
    """The associated legistar item guid or ``None``.
    A special value of ``"NoClip"`` will specify :data:`~.types.NoClip`
    for the item.
    """
    real_guid: RealGuidOrNoneStr
    """The associated real-guid legistar item guid or ``None``.
    A special value of ``"NoClip"`` will specify :data:`~.types.NoClip`
    for the item.
    """


class ClipEditViewContext(ClipViewContext):
    """Template context for :class:`ClipEditView`
    """
    form_data: ClipEditForm
    """The :class:`form <ClipEditForm>` data"""
    item_options: Iterable[DetailPageResult]
    """An iterable of possible choices to assign to the clip"""
    rguid_item_options: Iterable[RGuidDetailResult]
    """An iterable of possible choices to assign to the clip (real-guid)"""


@routes.view('/clips/clip-edit/{clip_id}/', name='clip_item_change')
class ClipEditView(ClipViewBase[ClipEditViewContext]):
    """Edit view for a single :class:`~.model.Clip` instance
    """
    template_name = 'clips/clip-change.jinja2'

    def get_form_initial(self) -> ClipEditForm:
        return {
            'guid': guid_to_str(self.legistar_item),
            'real_guid': real_guid_to_str(self.legistar_rguid_item),
        }

    async def get_form_data(self) -> ClipEditForm:
        data: ClipEditForm
        if self.request.method == 'POST':
            raw = await self.request.post()
            _guid = raw['guid']
            assert isinstance(_guid, str)
            if _guid != 'None' and _guid != 'NoClip':
                _guid = GUID(_guid)
            _real_guid = raw['real_guid']
            assert isinstance(_real_guid, str)
            if _real_guid != 'None' and _real_guid != 'NoClip':
                _real_guid = REAL_GUID(_real_guid)
            data = {
                'guid': _guid,
                'real_guid': _real_guid,
            }
        else:
            data = self.get_form_initial()
        return data

    def get_item_options(self) -> Iterable[DetailPageResult]:
        return self._get_item_options(self.legistar_data, DetailPageResult)

    def get_rguid_item_options(self) -> Iterable[RGuidDetailResult]:
        return self._get_item_options(self.legistar_data_rguid, RGuidDetailResult)

    def _get_item_options[
        IdT: (GUID, REAL_GUID), ItemT: (DetailPageResult, RGuidDetailResult)
    ](
        self,
        legistar_data: AbstractLegistarModel[IdT, ItemT],
        item_type: type[ItemT]
    ) -> Iterable[ItemT]:
        items: list[ItemT] = []
        item_guids = set[IdT]()
        if item_type is DetailPageResult:
            legistar_item = self.legistar_item
        else:
            legistar_item = self.legistar_rguid_item
        legistar_item = cast(ItemT, legistar_item)
        if legistar_item is not None and legistar_item is not NoClip:
            items.append(legistar_item)
            item_guids.add(legistar_data.get_guid_for_detail_result(legistar_item))
        for item in legistar_data:
            item_id = legistar_data.get_guid_for_detail_result(item)
            if item_id in item_guids:
                continue
            if legistar_data.is_guid_matched(item_id):
                continue
            delta = self.clip.datetime - item.feed_item.meeting_date
            if abs(delta) > datetime.timedelta(days=90):
                continue
            items.append(item)
            item_guids.add(item_id)
        return items

    async def get_context_data(self) -> ClipEditViewContext:
        """Get the context data for this view
        """
        return {
            'page_title': self.clip.unique_name,
            'nav_links': self.request.app[NavLinksKey],
            'clip': self.clip,
            'legistar_item': self.legistar_item,
            'legistar_guid': self.legistar_guid,
            'legistar_rguid_item': self.legistar_rguid_item,
            'legistar_rguid_item_id': self.legistar_rguid_item_id,
            'form_data': await self.get_form_data(),
            'item_options': self.get_item_options(),
            'rguid_item_options': self.get_rguid_item_options(),
        }

    @with_data_file_lock
    @read_only_guard
    async def post(self) -> web.Response:
        next_url = self.request.app.router['clip_item_change'].url_for(clip_id=self.clip.id)
        context = await self.get_context_data()
        form_initial = self.get_form_initial()
        form_data = context['form_data']
        guid = guid_from_str(form_data['guid'])
        guid_changed = id_equal(guid, self.legistar_guid)
        logger.info(f'{guid=}, {self.legistar_guid=}, {guid_changed=}')
        real_guid = real_guid_from_str(form_data['real_guid'])
        real_guid_changed = id_equal(real_guid, self.legistar_rguid_item_id)
        logger.info(f'{real_guid=}, {self.legistar_rguid_item_id=}, {real_guid_changed=}')
        # raise web.HTTPFound(next_url)
        if not guid_changed and not real_guid_changed:
            raise web.HTTPFound(next_url)
        if guid_changed:
            if guid is None or guid is NoClip:
                current_guid = self.legistar_guid
                assert isinstance(current_guid, str)
                item = self.legistar_data[current_guid]
                logger.debug(f'unlinking item: {item.real_guid=}')
                # self.legistar_data.add_clip_match_override(item.real_guid, guid)
            else:
                item = self.legistar_data[guid]
                logger.debug(f'setting clip match: {item.real_guid=}')
                # self.legistar_data.add_clip_match_override(item.real_guid, self.clip.id)
        if real_guid_changed:
            if real_guid is None or real_guid is NoClip:
                current_guid = self.legistar_rguid_item_id
                assert isinstance(current_guid, str)
                item = self.legistar_data_rguid[current_guid]
                logger.debug(f'unlinking item: {item.real_guid=}')
                # self.legistar_data_rguid.add_clip_match_override(item.real_guid, real_guid)
            else:
                item = self.legistar_data_rguid[real_guid]
                logger.debug(f'setting clip match: {item.real_guid=}')
                # self.legistar_data_rguid.add_clip_match_override(item.real_guid, self.clip.id)
        conf_file = self.get_config()
        # self.legistar_data.save(conf_file.legistar.data_file)
        raise web.HTTPFound(next_url)



class LegistarItemsContext[
    IdT: (GUID, REAL_GUID), ItemT: (DetailPageResult, RGuidDetailResult)
](GlobalContext):
    """Template context for :class:`LegistarItemsViewBase`
    """
    items: Iterable[ItemT]
    """The items to display"""
    item_dict: dict[IdT, ItemT]
    """Mapping of items by their id"""
    item_clip_ids: dict[IdT, ClipIdOrNoneStr]
    """Mapping of item ids to their associated clip ids"""
    item_view_name: str
    """The name of the view to display an individual item"""
    item_edit_view_name: str
    """The name of the view to edit an individual item"""
    paginator: Paginator[tuple[IdT, ItemT]]
    """:class:`~.pagination.Paginator` instance for :attr:`item_dict`"""
    filter_context: ListFilterContext[Category]
    """Filter context data"""


class LegistarItemsViewBase[
    IdT: (GUID, REAL_GUID),
    ItemT: (DetailPageResult, RGuidDetailResult),
](TemplatedView[LegistarItemsContext[IdT, ItemT]]):
    """Base class for views that display a list of legistar items
    """
    template_name = 'legistar/items.jinja2'

    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.filter_context = self.parse_filter_context()
        self.item_dict, self.items = self.get_items()
        item_tuples = [(self._id_for_item(item), item) for item in self.items]
        self.paginator = Paginator[tuple[IdT, ItemT]](self.request, item_tuples)

    @property
    @abstractmethod
    def item_view_name(self) -> str: ...

    @property
    @abstractmethod
    def item_edit_view_name(self) -> str: ...

    @property
    @abstractmethod
    def legistar_data(self) -> AbstractLegistarModel[IdT, ItemT]: ...

    @abstractmethod
    def _id_for_item(self, item: ItemT) -> IdT: ...

    def parse_filter_context(self) -> ListFilterContext[Category]:
        view_unassigned = bool(self.request.query.get('unassigned'))
        category = self.request.query.get('category')
        if not category:
            category = None
        if category is not None:
            category = Category(category)
        return {
            'view_unassigned': view_unassigned,
            'all_categories': list(sorted(self.get_all_categories())),
            'current_category': category,
            'filter_by_date': False,
            'start_date': None,
            'end_date': None,
        }

    def get_items(self) -> tuple[dict[IdT, ItemT], Sequence[ItemT]]:
        legistar_data = self.legistar_data
        item_clip_ids: dict[IdT, ClipIdOrNoneStr] = {}
        for guid, item in legistar_data.items():
            clip_id = legistar_data.get_clip_id_for_guid(guid)
            item_clip_ids[guid] = clip_id_to_str(clip_id)
        item_dict = dict(legistar_data.items())
        item_dict = self.get_filtered_items(item_dict)
        item_dict = self._sort_items(item_dict)
        return item_dict, list(item_dict.values())

    def get_filtered_items(self, item_dict: dict[IdT, ItemT]) -> dict[IdT, ItemT]:
        """Get items filtered by the :attr:`filter_context`
        """
        item_dict = self._filter_by_category(item_dict)
        item_dict = self._filter_by_date(item_dict)
        item_dict = self._filter_unassigned_clips(item_dict)
        return item_dict

    def get_all_categories(self) -> set[Category]:
        return set([item.feed_item.category for item in self.legistar_data])

    async def get_context_data(self) -> LegistarItemsContext[IdT, ItemT]:
        """Get the context data for this view
        """
        legistar_data = self.legistar_data
        item_clip_ids: dict[IdT, ClipIdOrNoneStr] = {}
        for guid, item in legistar_data.items():
            clip_id = legistar_data.get_clip_id_for_guid(guid)
            item_clip_ids[guid] = clip_id_to_str(clip_id)

        context: LegistarItemsContext[IdT, ItemT] = {
            'page_title': 'Legistar Items',
            'nav_links': self.request.app[NavLinksKey],
            'items': self.items,
            'item_dict': self.item_dict,
            'item_clip_ids': item_clip_ids,
            'item_view_name': self.item_view_name,
            'item_edit_view_name': self.item_edit_view_name,
            'paginator': self.paginator,
            'filter_context': self.filter_context,
        }
        return context

    def _filter_by_category(
        self,
        item_dict: dict[IdT, ItemT],
    ) -> dict[IdT, ItemT]:
        category = self.filter_context['current_category']
        if category is None:
            return item_dict
        return self.legistar_data.filter_by_category(category, items=item_dict)

    def _filter_by_date(
        self,
        item_dict: dict[IdT, ItemT],
    ) -> dict[IdT, ItemT]:
        if not self.filter_context['filter_by_date']:
            return item_dict
        start_date, end_date = self.filter_context['start_date'], self.filter_context['end_date']
        if start_date is None or end_date is None:
            self.filter_context['filter_by_date'] = False
            return item_dict
        return self.legistar_data.filter_by_dt_range(
            start_dt=datetime.datetime.combine(start_date, datetime.time.min),
            end_dt=datetime.datetime.combine(end_date, datetime.time.max),
            items=item_dict
        )

    def _filter_unassigned_clips(
        self,
        item_dict: dict[IdT, ItemT]
    ) -> dict[IdT, ItemT]:
        if not self.filter_context['view_unassigned']:
            return item_dict
        return {
            guid: item for guid, item in item_dict.items()
            if self.legistar_data.get_clip_id_for_guid(guid) is None
        }

    def _sort_items(
        self,
        item_dict: dict[IdT, ItemT],
        order: SortOrder = 'dsc'
    ) -> dict[IdT, ItemT]:
        by_dt: dict[datetime.datetime, list[ItemT]] = {}
        for item in item_dict.values():
            dt = item.feed_item.meeting_date
            l = by_dt.setdefault(dt, [])
            l.append(item)
        sorted_items: list[ItemT] = []
        for dt in sorted(by_dt.keys()):
            sorted_items.extend(by_dt[dt])
        if order == 'dsc':
            sorted_items.reverse()
        return {self._id_for_item(item):item for item in sorted_items}



@routes.view('/legistar/items/', name='legistar_items')
class LegistarItemsView(LegistarItemsViewBase[GUID, DetailPageResult]):
    """A list view of :class:`~.legistar.model.DetailPageResult` instances
    """
    @property
    def legistar_data(self) -> LegistarData:
        return self.request.app[LegistarDataKey]

    def _id_for_item(self, item: DetailPageResult) -> GUID:
        return item.feed_guid

    @property
    def item_view_name(self) -> str:
        return 'legistar_item'

    @property
    def item_edit_view_name(self) -> str:
        return 'legistar_item_change'



@routes.view('/legistar-rguid/items/', name='rguid_legistar_items')
class RGuidLegistarItemsView(LegistarItemsViewBase[REAL_GUID, RGuidDetailResult]):
    """A list view of :class:`~.legistar.guid_model.RGuidDetailResult` instances
    """
    @property
    def legistar_data(self) -> RGuidLegistarData:
        return self.request.app[RGuidLegistarDataKey]

    def _id_for_item(self, item: RGuidDetailResult) -> REAL_GUID:
        return item.real_guid

    @property
    def item_view_name(self) -> str:
        return 'rguid_legistar_item'

    @property
    def item_edit_view_name(self) -> str:
        return 'rguid_legistar_item_change'



class LegistarItemFormData(TypedDict):
    """Form data for :class:`LItemChangeView`
    """
    clip_id: ClipIdOrNoneStr
    """The associated clip id or ``None``"""
    next_url: URL|str
    """The URL to redirect to after submitting the form"""


class _ItemContext[
    IdT: (GUID, REAL_GUID), ItemT: (DetailPageResult, RGuidDetailResult)
](GlobalContext):
    """Template context for :class:`LItemViewBase`

    :meta public:
    """
    legistar_type: Literal['legistar', 'legistar_rguid']
    """The type of legistar data"""
    item: ItemT
    """The item being viewed"""
    item_id: IdT
    """The id of the :attr:`item`"""
    files: LegistarFiles|None
    """The files associated with the item"""
    file_iter: Iterable[tuple[LegistarFileUID, AbstractFile]]
    """An iterable of :class:`~.legistar.model.AbstractFile` instances"""
    file_static_key: StaticRootName
    """The key for the item assets in :class:`~.web.types.StaticUrlRoots`"""
    clip_id: ClipIdOrNoneStr
    """The associated clip id or ``None``"""
    clip: Clip|None
    """The associated :class:`~.model.Clip` or ``None``"""
    change_view_name: str
    """The name of the view to edit the item"""


class _ItemChangeContext[
    IdT: (GUID, REAL_GUID), ItemT: (DetailPageResult, RGuidDetailResult)
](_ItemContext[IdT, ItemT]):
    """Template context for :class:`LItemChangeView`

    :meta public:
    """
    form_data: LegistarItemFormData
    """The form data"""
    clip_options: Iterable[Clip]
    """An iterable of possible clips to assign to the item"""


type ItemContextT[
    IdT: (GUID, REAL_GUID), ItemT: (DetailPageResult, RGuidDetailResult)
] =  _ItemContext[IdT, ItemT]|_ItemChangeContext[IdT, ItemT]
"""Type alias for the template context for :class:`LItemViewBase` and :class:`LItemChangeView`"""


type ItemContext[
    X: ItemContextT,
    A: (GUID, REAL_GUID),
    B: (DetailPageResult, RGuidDetailResult)
] = ItemContextT[A, B]
"""Type alias for the template context for :class:`LItemViewBase` and :class:`LItemChangeView`"""


class LItemViewBase[
    X: (_ItemContext, _ItemChangeContext),
    IdT: (GUID, REAL_GUID),
    ItemT: (DetailPageResult, RGuidDetailResult),
](TemplatedView[ItemContext[X, IdT, ItemT]]):
    """Base class for views that display a single :class:`~.legistar.model.DetailPageResult`
    or :class:`~.legistar.guid_model.RGuidDetailResult` instance
    """
    template_name = 'legistar/item.jinja2'
    legistar_type: Literal['legistar', 'legistar_rguid']
    guid: IdT
    clip_id: CLIP_ID|None|NoClipT
    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.guid = self.get_guid()
        self.clip_id = self.legistar_data.get_clip_id_for_guid(self.guid)

    @property
    @abstractmethod
    def legistar_data(self) -> AbstractLegistarModel[IdT, ItemT]:
        raise NotImplementedError

    @property
    @abstractmethod
    def file_static_key(self) -> StaticRootName: ...

    @abstractmethod
    def get_guid(self) -> IdT:
        raise NotImplementedError

    @abstractmethod
    def get_files(self) -> LegistarFiles|None:
        raise NotImplementedError

    @abstractmethod
    def iter_files(self) -> Iterable[tuple[LegistarFileUID, AbstractFile]]: ...

    @abstractmethod
    def get_change_view_name(self) -> str:
        raise NotImplementedError

    @property
    def clips(self) -> ClipCollection:
        return self.request.app[ClipsKey]

    @property
    def clip(self) -> Clip|None:
        if self.clip_id is None or self.clip_id is NoClip:
            return None
        return self.clips[self.clip_id]

    @property
    def item(self) -> ItemT:
        return self.legistar_data[self.guid]

    @abstractmethod
    async def get_context_data(self) -> ItemContext[X, IdT, ItemT]:
        """Get the context data for this view
        """
        raise NotImplementedError

    async def get_base_context_data(self) -> _ItemContext[IdT, ItemT]:
        return {
            'page_title': self.item.feed_item.title,
            'nav_links': self.request.app[NavLinksKey],
            'legistar_type': self.legistar_type,
            'item': self.item,
            'item_id': self.guid,
            'files': self.get_files(),
            'file_iter': self.iter_files(),
            'clip_id': clip_id_to_str(self.clip_id),
            'file_static_key': self.file_static_key,
            'clip': self.clip,
            'change_view_name': self.get_change_view_name(),
        }


class LItemMixin:
    """Mixin for views that display a single :class:`~.legistar.model.DetailPageResult`
    """
    request: web.Request
    item: DetailPageResult
    guid: GUID
    @property
    def legistar_data(self) -> LegistarData:
        return self.request.app[LegistarDataKey]

    @property
    def file_static_key(self) -> StaticRootName:
        return 'legistar'

    def get_guid(self) -> GUID:
        guid = self.request.match_info['guid']
        assert is_guid(guid)
        return guid

    def get_files(self) -> LegistarFiles | None:
        return self.legistar_data.files.get(self.item.feed_guid)

    def iter_files(self) -> Iterable[tuple[LegistarFileUID, AbstractFile]]:
        files = self.get_files()
        if files is None:
            return []
        result: list[tuple[LegistarFileUID, LegistarFile|AttachmentFile]] = []
        for f_key, f_file in files:
            if f_file is None:
                continue
            uid = file_key_to_uid(f_key)
            full_p = self.legistar_data.get_file_path(self.guid, f_key)
            full_p = full_p.relative_to(self.legistar_data.root_dir)
            result.append((uid, dataclasses.replace(f_file, filename=full_p)))
        for att_key, att_file in files.attachments.items():
            if att_file is None:
                continue
            uid = attachment_name_to_uid(att_key)
            full_p = self.legistar_data.get_attachment_path(self.guid, att_key)
            full_p = full_p.relative_to(self.legistar_data.root_dir)
            result.append((uid, dataclasses.replace(att_file, filename=full_p)))
        return result


    def get_change_view_name(self) -> str:
        return 'legistar_item_change'

    def _get_next_url(self) -> URL:
        return self.request.app.router['legistar_item'].url_for(guid=self.guid)



@routes.view('/legistar/items/view/{guid}/', name='legistar_item')
class LItemView(LItemMixin, LItemViewBase[_ItemContext[GUID, DetailPageResult], GUID, DetailPageResult]):
    """View for a single :class:`~.legistar.model.DetailPageResult` instance
    """
    legistar_type = 'legistar'
    async def get_context_data(self) -> _ItemContext[GUID, DetailPageResult]:
        """Get the context data for this view
        """
        return await self.get_base_context_data()


class RGItemMixin:
    """Mixin for views that display a single :class:`~.legistar.guid_model.RGuidDetailResult`
    """
    guid: REAL_GUID
    request: web.Request
    item: RGuidDetailResult

    @property
    def legistar_data(self) -> RGuidLegistarData:
        return self.request.app[RGuidLegistarDataKey]

    @property
    def file_static_key(self) -> StaticRootName:
        return 'legistar_rguid'

    def get_guid(self) -> REAL_GUID:
        guid = self.request.match_info['guid']
        assert is_real_guid(guid)
        return guid

    def get_files(self) -> LegistarFiles | None:
        return None

    def iter_files(self) -> Iterable[tuple[LegistarFileUID, AbstractFile]]:
        for f in self.item.files:
            full_p = self.item.files.get_file_path(f.uid, absolute=True)
            full_p = full_p.relative_to(self.legistar_data.root_dir)
            yield f.uid, dataclasses.replace(f, filename=full_p)

    def get_change_view_name(self) -> str:
        return 'rguid_legistar_item_change'

    def _get_next_url(self) -> URL:
        return self.request.app.router['rguid_legistar_item'].url_for(guid=self.guid)



@routes.view('/legistar-rguid/items/view/{guid}/', name='rguid_legistar_item')
class RGItemView(RGItemMixin, LItemViewBase[_ItemContext[REAL_GUID, RGuidDetailResult], REAL_GUID, RGuidDetailResult]):
    """View for a single :class:`~.legistar.guid_model.RGuidDetailResult` instance
    """
    legistar_type = 'legistar_rguid'
    async def get_context_data(self) -> _ItemContext[REAL_GUID, RGuidDetailResult]:
        """Get the context data for this view
        """
        return await self.get_base_context_data()



class LItemChangeViewBase[
    IdT: (GUID, REAL_GUID),
    ItemT: (DetailPageResult, RGuidDetailResult),
](LItemViewBase[_ItemChangeContext[IdT, ItemT], IdT, ItemT]):
    """Base class for edit views of legistar items
    """
    template_name = 'legistar/item-change.jinja2'
    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.form_initial = self.get_form_initial()

    def get_next_url(self) -> URL:
        prev = self.request.query.get('prev')
        if prev is not None:
            assert isinstance(prev, str)
            return URL(prev)
        return self._get_next_url()

    @abstractmethod
    def _get_next_url(self) -> URL:
        raise NotImplementedError

    def get_clip_options(self):
        legistar_data = self.legistar_data
        category_maps = self.get_config().legistar.category_maps
        category_maps_rev = {v:k for k,v in category_maps.items()}
        item = self.legistar_data[self.guid]
        location = category_maps_rev.get(item.feed_item.category)

        clips = []
        for clip in self.clips:
            if not legistar_data.is_clip_id_available(clip.id) and clip.id != self.clip_id:
                continue
            if location is not None and clip.location != location:
                continue
            delta = clip.datetime - item.feed_item.meeting_date
            if abs(delta) > datetime.timedelta(days=90):
                continue
            clips.append(clip)
        return clips

    def get_form_initial(self) -> LegistarItemFormData:
        return {
            'clip_id': clip_id_to_str(self.clip_id),
            'next_url': self.get_next_url(),
        }

    async def get_form_data(self) -> LegistarItemFormData:
        data: LegistarItemFormData
        if self.request.method == 'POST':
            raw = await self.request.post()
            _clip_id = raw['clip_id']
            next_url = raw['next_url']

            assert isinstance(_clip_id, str)
            if _clip_id != 'None' and _clip_id != 'NoClip':
                _clip_id = CLIP_ID(_clip_id)

            assert isinstance(next_url, str)
            next_url = URL(next_url)

            data = {
                'clip_id': _clip_id,
                'next_url': next_url,
            }
        else:
            data = self.get_form_initial()
        return data

    async def get_context_data(self) -> _ItemChangeContext[IdT, ItemT]:
        """Get the context data for this view
        """
        sc: _ItemContext[IdT, ItemT] = await super().get_base_context_data()
        c = cast(_ItemChangeContext[IdT, ItemT], sc)
        c['form_data'] = await self.get_form_data()
        c['clip_options'] = self.get_clip_options()
        return c

    @with_data_file_lock
    @read_only_guard
    async def post(self) -> web.Response:
        context = await self.get_context_data()
        item = context['item']
        form_initial = self.get_form_initial()
        form_data = context['form_data']
        next_url = form_data['next_url']
        clip_id = clip_id_from_str(form_data['clip_id'])
        changed = not id_equal(clip_id, self.clip_id)
        logger.info(f'{clip_id=}, {self.clip_id=}, {changed=}')
        if not changed:
            raise web.HTTPFound(next_url)
        self.legistar_data.add_clip_match_override(item.real_guid, clip_id)
        conf = self.get_config()
        if isinstance(self.legistar_data, LegistarData):
            self.legistar_data.save(conf.legistar.data_file)
        else:
            self.legistar_data.save(RGuidLegistarData._get_data_file(conf))
        raise web.HTTPFound(next_url)


@routes.view('/legistar/items/change/{guid}/', name='legistar_item_change')
class LItemChangeView(LItemMixin, LItemChangeViewBase[GUID, DetailPageResult]):
    """Edit view for a single :class:`~.legistar.model.DetailPageResult` instance
    """
    legistar_type = 'legistar'


@routes.view('/legistar-rguid/items/change/{guid}/', name='rguid_legistar_item_change')
class RGLItemChangeView(RGItemMixin, LItemChangeViewBase[REAL_GUID, RGuidDetailResult]):
    """Edit view for a single :class:`~.legistar.guid_model.RGuidDetailResult` instance
    """
    legistar_type = 'legistar_rguid'

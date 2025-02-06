from __future__ import annotations
from typing import TypedDict, Literal
from pathlib import Path
import datetime

from aiohttp import web
import jinja2
from yarl import URL

from ..model import CLIP_ID
from ..legistar.types import GUID, REAL_GUID, LegistarFileUID
from ..legistar.rss_parser import is_guid, is_real_guid
from .types import *
from .config import APP_CONF_KEY


class Context(TypedDict):
    """Filter context
    """
    app: web.Application
    """The current application"""
    request: web.Request
    """The current request"""


@jinja2.pass_context
def local_tz(ctx: Context, dt: datetime.datetime) -> datetime.datetime:
    """Convert a datetime to the app's local timezone (stored in :data:`.types.TimezoneKey`)
    """
    app = ctx['app']
    tz = app[TimezoneKey]
    if dt.tzinfo is not None:
        return dt.astimezone(tz)
    return dt.replace(tzinfo=tz)

@jinja2.pass_context
def datetime_format(ctx: Context, dt: datetime.datetime, fmt: str = '%m/%d/%Y %H:%M') -> str:
    """Format a datetime object using :func:`datetime.datetime.strftime`

    The timezone of the datetime object will first be converted using
    :func:`local_tz`.

    Arguments:
        dt: The datetime object to format
        fmt: The format string to use

    """
    dt = local_tz(ctx, dt)
    return dt.strftime(fmt)

@jinja2.pass_context
def date_format(ctx: Context, dt: datetime.datetime, fmt: str = '%x') -> str:
    """Format a datetime object as a date using :func:`datetime.datetime.strftime`

    The timezone of the datetime object will first be converted using
    :func:`local_tz`.

    Arguments:
        dt: The datetime object to format
        fmt: The format string to use

    """
    dt = local_tz(ctx, dt)
    return dt.strftime(fmt)

@jinja2.pass_context
def time_format(ctx: Context, dt: datetime.datetime, fmt: str = '%X') -> str:
    """Format a datetime object as a time using :func:`datetime.datetime.strftime`

    The timezone of the datetime object will first be converted using
    :func:`local_tz`.

    Arguments:
        dt: The datetime object to format
        fmt: The format string to use

    """
    dt = local_tz(ctx, dt)
    return dt.strftime(fmt)

def duration_format(td: datetime.timedelta|float) -> str:
    """Format a timedelta or number of seconds as ``HH:MM:SS``
    """
    if not isinstance(td, datetime.timedelta):
        td = datetime.timedelta(seconds=td)
    seconds = int(round(td.total_seconds()))
    h = seconds // 3600
    m = (seconds - h * 3600) // 60
    s = seconds % 60
    return f'{h:02d}:{m:02d}:{s:02d}'


def snake_case_to_title(s: str) -> str:
    """Convert a snake_case string to title case
    """
    return ' '.join(s.split('_')).title()


@jinja2.pass_context
def url_query(
    ctx: Context,
    query_: dict[str, str]|None = None,
    merge: bool = True
) -> URL:
    """Add query parameters to the current request URL

    Arguments:
        query_: The query parameters to add
        merge: If ``True``, merge the new query parameters with any ewxisting
            query parameters.  Otherwise, replace the existing query parameters.

    """
    cur_url = ctx['request'].url
    cur_query = cur_url.query
    if merge and query_ is not None:
        new_query = {k: cur_query[k] for k in cur_query}
        new_query.update(query_)
    else:
        new_query = query_
    return cur_url.with_query(new_query)

@jinja2.pass_context
def static_path(ctx: Context, static_name: StaticRootName, filename: Path|str) -> URL:
    """Get a URL for a static file

    Arguments:
        static_name: The name of the static root to use
            (a member of :class:`.types.StaticUrlRoots`).
        filename: The path to the file, relative to the static root

    """
    # root = ctx['app'][StaticRootsKey][static_name]
    url_root = ctx['app'][StaticUrlRootsKey][static_name]
    assert not Path(filename).is_absolute()
    return url_root.joinpath(str(filename))
    # p = root / filename
    # assert not p.is_absolute()
    # return URL(f'/{p}')

@jinja2.pass_context
def clip_url(
    ctx: Context,
    clip_id: CLIP_ID,
    file_type: Literal['video', 'chapters'],
) -> URL:
    """Get the s3 URL for a clip file

    .. note::

        For the ``chapters`` file type, the URL will be for a local view
        of the chapters file (:func:`.views.clip_webvtt`).
        This is to prevent issues with CORS.

    """
    app_conf = ctx['app'][APP_CONF_KEY]
    if not app_conf.use_s3:
        raise web.HTTPInternalServerError(reason='Clips are not available')
    if file_type == 'chapters':
        return ctx['app'].router['clip_webvtt'].url_for(clip_id=clip_id)
    s3_client = ctx['app'][S3ClientKey]
    clips = ctx['app'][ClipsKey]
    clip = clips[clip_id]
    rel_filename = clip.get_file_path(file_type, absolute=False)
    s3_prefix = s3_client.data_dirs['clips']
    s3_path = s3_prefix / rel_filename
    return s3_client.url_for_key(str(s3_path))



@jinja2.pass_context
def legistar_url(
    ctx: Context,
    key: tuple[Literal['legistar'], GUID, LegistarFileUID]|tuple[Literal['legistar_rguid'], REAL_GUID, LegistarFileUID],
) -> URL:
    """Get the s3 URL for a legistar file
    """
    app_conf = ctx['app'][APP_CONF_KEY]
    if not app_conf.use_s3:
        raise web.HTTPInternalServerError(reason='Legistar files are not available')
    model_type, guid, uid = key
    s3_client = ctx['app'][S3ClientKey]
    if model_type == 'legistar':
        m = ctx['app'][LegistarDataKey]
        assert is_guid(guid)
        filename, _ = m.get_path_for_uid(guid, uid)
    else:
        m = ctx['app'][RGuidLegistarDataKey]
        assert is_real_guid(guid)
        filename, _ = m.get_path_for_uid(guid, uid)
    data_root = m.root_dir
    filename = filename.relative_to(data_root)
    s3_prefix = s3_client.data_dirs[model_type]
    s3_path = s3_prefix / filename
    return s3_client.url_for_key(str(s3_path), scheme=ctx['request'].scheme)

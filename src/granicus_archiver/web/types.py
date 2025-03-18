from __future__ import annotations
from typing import Literal, TypedDict, NamedTuple, Sequence, Any, Self
from pathlib import Path
from zoneinfo import ZoneInfo
import asyncio

from aiohttp import web
from yarl import URL


from ..config import Config
from ..clips.model import ClipCollection
from ..legistar.model import LegistarData
from ..legistar.guid_model import RGuidLegistarData


__all__ = (
    'ConfigKey', 'DataFileType', 'DataFiles', 'DataFileLockKey',
    'ClipsKey', 'LegistarDataKey', 'RGuidLegistarDataKey',
    'SortOrder', 'TimezoneKey', 'StaticRootName', 'StaticRoots', 'StaticRootsKey',
    'StaticUrlRoots', 'StaticUrlRootsKey', 'NavLink', 'NavLinksKey',
)

ConfigKey = web.AppKey('Config', Config)
"""App key for the :class:`granicus_archiver.config.Config` instance"""

DataFileType = Literal['clips', 'legistar', 'legistar_rguid']
""""""
DataFiles = dict[DataFileType, Path]
""""""

DataFileLockKey = web.AppKey('DataFileLock', asyncio.Lock)
"""App key for the :class:`asyncio.Lock` used to protect data files"""

ClipsKey = web.AppKey('Clips', ClipCollection)
"""App key for the :class:`granicus_archiver.model.ClipCollection` instance"""

LegistarDataKey = web.AppKey('LegistarData', LegistarData)
"""App key for the :class:`granicus_archiver.legistar.model.LegistarData` instance"""

RGuidLegistarDataKey = web.AppKey('RGuidLegistarDataKey', RGuidLegistarData)
"""App key for the :class:`granicus_archiver.legistar.guid_model.RGuidLegistarData` instance"""

class NavLink(NamedTuple):
    """A navigation link
    """
    name: str
    """Name of the link"""
    title: str
    """Title of the link"""
    url: str|URL
    """URL or view name for the link"""
    view_kwargs: dict[str, Any]|None = None
    """Keyword arguments for the view, if any"""

    def get_url(self, app: web.Application) -> URL:
        """Get the URL for the link
        """
        if isinstance(self.url, URL):
            return self.url
        return app.router[self.url].url_for(**(self.view_kwargs or {}))

    def serialize(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'title': self.title,
            'url': str(self.url),
            'view_kwargs': self.view_kwargs,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            name=data['name'],
            title=data['title'],
            url=URL(data['url']) if '://' in data['url'] else data['url'],
            view_kwargs=data['view_kwargs'],
        )


NavLinksKey = web.AppKey('NavLinks', Sequence[NavLink])
"""App key for :class:`NavLink` instances"""


SortOrder = Literal['asc', 'dsc']

TimezoneKey = web.AppKey('Timezone', ZoneInfo)
"""App key for the server's default timezone

(Set from :attr:`granicus_archiver.config.Config.local_timezone_name`)
"""

StaticRootName = Literal['assets', 'granicus', 'legistar', 'legistar_rguid']

class StaticRoots(TypedDict):
    """Filesystem paths for static files if serving locally
    """
    assets: Path
    """Files specific to the web app"""
    granicus: Path
    """Granicus clips"""
    legistar: Path
    """Legistar data"""
    legistar_rguid: Path
    """Legistar data with RGUIDs"""

class StaticUrlRoots(TypedDict):
    """Root URLs for static files
    """
    assets: URL
    """Files specific to the web app"""
    granicus: URL
    """Granicus clips"""
    legistar: URL
    """Legistar data"""
    legistar_rguid: URL
    """Legistar data with RGUIDs"""

StaticRootsKey = web.AppKey('StaticRootsKey', StaticRoots)
StaticUrlRootsKey = web.AppKey('StaticUrlRootsKey', StaticUrlRoots)

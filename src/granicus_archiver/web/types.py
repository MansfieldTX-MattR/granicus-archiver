from __future__ import annotations
from typing import Literal, TypedDict
from pathlib import Path
from zoneinfo import ZoneInfo

from aiohttp import web
from yarl import URL


from ..config import Config
from ..model import ClipCollection
from ..legistar.model import LegistarData
from ..legistar.guid_model import RGuidLegistarData

__all__ = (
    'ConfigKey', 'ClipsKey', 'LegistarDataKey', 'RGuidLegistarDataKey',
    'SortOrder', 'TimezoneKey', 'StaticRootName', 'StaticRoots', 'StaticRootsKey',
    'StaticUrlRoots', 'StaticUrlRootsKey',
)

ConfigKey = web.AppKey('Config', Config)
"""App key for the :class:`granicus_archiver.config.Config` instance"""

ClipsKey = web.AppKey('Clips', ClipCollection)
"""App key for the :class:`granicus_archiver.model.ClipCollection` instance"""

LegistarDataKey = web.AppKey('LegistarData', LegistarData)
"""App key for the :class:`granicus_archiver.legistar.model.LegistarData` instance"""

RGuidLegistarDataKey = web.AppKey('RGuidLegistarDataKey', RGuidLegistarData)
"""App key for the :class:`granicus_archiver.legistar.guid_model.RGuidLegistarData` instance"""

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

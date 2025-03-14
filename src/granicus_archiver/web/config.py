from __future__ import annotations
from typing import NamedTuple, Sequence
from pathlib import Path

from aiohttp import web
from yarl import URL

from .types import NavLink

__all__ = ('AppConfig', 'APP_CONF_KEY')


class AppConfig(NamedTuple):
    """Web app configuration
    """
    hostname: str = 'localhost'
    """Hostname to bind to"""
    port: int = 8080
    """Port to bind to"""
    sockfile: Path|None = None
    """UNIX socket file to bind to

    If ``None``, the :attr:`hostname` and :attr:`port` will be used instead.
    """
    serve_static: bool = True
    """Whether to serve static files directly from aiohttp"""
    read_only: bool = True
    """If ``True``, the app will not allow modification of any data files"""
    static_url: URL = URL('/')
    """Root URL to serve static files from

    If :attr:`serve_static` is ``True``, this should be ``"/"``. Otherwise, it
    should be the URL path to the static files.
    """
    use_s3: bool = False
    """If ``True``, the app will use S3 to for data files and assets"""
    s3_data_dir: Path|None = None
    """Root directory to store local data files from s3"""
    nav_links: Sequence[NavLink] = (
        NavLink(name='home', title='Home', url='home'),
        NavLink(name='clips', title='Clips', url='clip_list'),
        NavLink(name='legistar', title='Legistar', url='legistar_items'),
        NavLink(name='legistar_rguid', title='Legistar (Real Guid)', url='rguid_legistar_items'),
    )
    """Navigation links for the app"""
    site_name: str = 'Granicus Archive'
    """Name of the site"""


APP_CONF_KEY = web.AppKey('AppConfig', AppConfig)
"""App key for the :class:`AppConfig` instance"""

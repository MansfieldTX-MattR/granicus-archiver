from __future__ import annotations
from typing import NamedTuple
from pathlib import Path

from aiohttp import web
from yarl import URL

from .types import *

__all__ = ('AppConfig', 'APP_CONF_KEY')


class AppConfig(NamedTuple):
    """Web app configuration
    """
    hostname: str = 'localhost'
    """Hostname to bind to"""
    port: int = 8080
    """Port to bind to"""
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


APP_CONF_KEY = web.AppKey('AppConfig', AppConfig)
"""App key for the :class:`AppConfig` instance"""

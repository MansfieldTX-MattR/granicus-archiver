from __future__ import annotations
from typing import Sequence, Self, Any
from pathlib import Path
from dataclasses import dataclass, field
from os import PathLike

from aiohttp import web
from yarl import URL
from yaml import (
    load as yaml_load,
    dump as yaml_dump,
    CLoader as YamlLoader,
    CDumper as YamlDumper,
)

from ..clips.model import Location
from .types import NavLink
from ..config import BaseConfig

__all__ = ('AppConfig', 'APP_CONF_KEY')


@dataclass
class AppConfig(BaseConfig):
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

    hidden_clip_categories: Sequence[Location] = field(default_factory=list)
    """List of clip categories to hide in the UI"""

    @classmethod
    def load(cls, filename: PathLike) -> Self:
        """Load the configuration from a file
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        if not filename.exists():
            return cls.build_defaults()
        data = yaml_load(filename.read_text(), Loader=YamlLoader)
        return cls.deserialize(data)

    def save(self, filename: PathLike) -> None:
        """Save the configuration to a file
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        filename.parent.mkdir(parents=True, exist_ok=True)
        filename.write_text(yaml_dump(self.serialize(), Dumper=YamlDumper))

    def update(self, **kwargs) -> bool:
        updated = False
        for k, v in kwargs.items():
            if getattr(self, k) == v:
                continue
            setattr(self, k, v)
            updated = True
        return updated

    @classmethod
    def build_defaults(cls, **kwargs) -> Self:
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        return {
            'hostname': self.hostname,
            'port': self.port,
            'sockfile': self.sockfile,
            'serve_static': self.serve_static,
            'read_only': self.read_only,
            'static_url': str(self.static_url),
            'use_s3': self.use_s3,
            's3_data_dir': self.s3_data_dir,
            'nav_links': [nl.serialize() for nl in self.nav_links],
            'hidden_clip_categories': self.hidden_clip_categories,
            'site_name': self.site_name,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            hostname=data['hostname'],
            port=data['port'],
            sockfile=data['sockfile'],
            serve_static=data['serve_static'],
            read_only=data['read_only'],
            static_url=URL(data['static_url']),
            use_s3=data['use_s3'],
            s3_data_dir=data['s3_data_dir'],
            nav_links=[NavLink.deserialize(nl) for nl in data['nav_links']],
            hidden_clip_categories=[
                Location(c) for c in data.get('hidden_clip_categories', [])
            ],
            site_name=data['site_name'],
        )

    @classmethod
    def load_from_env(cls) -> Self:
        raise NotImplementedError


APP_CONF_KEY = web.AppKey('AppConfig', AppConfig)
"""App key for the :class:`AppConfig` instance"""

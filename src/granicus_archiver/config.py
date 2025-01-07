from __future__ import annotations
from typing import ClassVar, Any, Self, TYPE_CHECKING
from abc import ABC, abstractmethod
from pathlib import Path
from os import PathLike
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

from yarl import URL

from yaml import (
    load as yaml_load,
    dump as yaml_dump,
    CLoader as YamlLoader,
    CDumper as YamlDumper,
)

from .model import Serializable
if TYPE_CHECKING:
    from .model import Location
    from .legistar.rss_parser import Category



class BaseConfig(Serializable):

    @abstractmethod
    def update(self, **kwargs) -> bool:
        """Update the config from keyword arguments
        """

    @classmethod
    @abstractmethod
    def build_defaults(cls, **kwargs) -> Self:
        """Create the config using defaults

        Any provided keyword arguments will override the default
        """


@dataclass
class GoogleConfig(BaseConfig):
    """Google config
    """
    user_credentials_filename: Path
    """Path to store OAuth credentials"""

    drive_folder: Path
    """Root folder name to upload within Drive"""

    legistar_drive_folder: Path
    """Root folder name to upload legistar items within Drive"""

    rguid_legistar_drive_folder: Path
    """Root folder name to upload rguid legistar items within Drive"""

    def update(self, **kwargs) -> bool:
        changed = False
        keys = [
            'user_credentials_filename', 'drive_folder',
            'legistar_drive_folder', 'rguid_legistar_drive_folder',
        ]
        for key in keys:
            if key not in kwargs:
                continue
            val = kwargs[key]
            if val == getattr(self, key):
                continue
            assert isinstance(val, Path)
            setattr(self, key, val)
            changed = True
        return changed

    @classmethod
    def build_defaults(cls, **kwargs) -> Self:
        default_kw = dict(
            user_credentials_filename=Path.home() / '.granicus-oauth-user.json',
            drive_folder=Path('granicus-archive/data/granicus'),
            legistar_drive_folder=Path('granicus-archive/data/legistar'),
            rguid_legistar_drive_folder=Path('granicus-archive/data/legistar-rguid'),
        )
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        return dict(
            user_credentials_filename=str(self.user_credentials_filename),
            drive_folder=str(self.drive_folder),
            legistar_drive_folder=str(self.legistar_drive_folder),
            rguid_legistar_drive_folder=str(self.rguid_legistar_drive_folder),
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        rg_folder = data.get(
            'rguid_legistar_drive_folder', 'granicus-archive/data/legistar-rguid'
        )
        return cls(
            user_credentials_filename=Path(data['user_credentials_filename']),
            drive_folder=Path(data['drive_folder']),
            legistar_drive_folder=Path(data['legistar_drive_folder']),
            rguid_legistar_drive_folder=Path(rg_folder),
        )

@dataclass
class LegistarConfig(BaseConfig):
    """Legistar Config
    """
    out_dir: Path
    """Root directory to store downloaded files (relatve to the current
    working directory).  Defaults to ``data/legistar``
    """
    out_dir_abs: Path
    """Root directory to store downloaded files (absolute path)
    """
    data_file: Path
    """Filename to store parsed data.  Defaults to "<out-dir>/data.json"
    """
    feed_urls: dict[str, URL] = field(default_factory=dict)
    """Mapping of calendar RSS feed urls with user-defined names as keys
    """
    feed_overflows_allowed: list[str] = field(default_factory=list)
    """A list of feed names (keys of :attr:`feed_urls`) that are allowed to
    reach the 100 item limit described in
    :meth:`.legistar.rss_parser.Feed.from_feed`
    """
    category_maps: dict[Location, Category] = field(default_factory=dict)
    """A :class:`dict` of any custom mappings to match the
    :attr:`Clip.location <.model.Clip.location>` fields to their appropriate
    :attr:`.legistar.rss_parser.FeedItem.category`

    The keys for this should be the ``location`` with the values set to the
    ``category``.
    """

    def is_feed_overflow_allowed(self, feed: str|URL) -> bool:
        """Check whether the given feed name or url is allowed to overflow

        If a string is supplied this returns whether it is present in
        :attr:`feed_overflows_allowed`.

        If a :class:`~yarl.URL` is supplied, :attr:`feed_urls` will be searched
        and the matching key (if any) will be checked.
        """
        if isinstance(feed, URL):
            urls_rev = {v:k for k,v in self.feed_urls.items()}
            key = urls_rev.get(feed)
        else:
            key = feed
        return key in self.feed_overflows_allowed


    def update(self, **kwargs) -> bool:
        changed = False
        out_dir = kwargs.get('out_dir')
        if out_dir is not None and out_dir != self.out_dir:
            assert isinstance(out_dir, Path)
            assert not out_dir.is_absolute()
            self.out_dir = out_dir
            self.out_dir_abs = out_dir.resolve()
            changed = True
        out_dir_abs = kwargs.get('out_dir_abs')
        if out_dir_abs is not None and out_dir_abs != self.out_dir_abs:
            assert isinstance(out_dir_abs, Path)
            assert out_dir_abs.is_absolute()
            self.out_dir_abs = out_dir_abs
            self.out_dir = out_dir_abs.relative_to(Path.cwd())
            changed = True
        data_file = kwargs.get('data_file')
        if data_file is not None:
            assert isinstance(data_file, Path)
            self.data_file = data_file
            changed = True
        for key in ['feed_urls', 'category_maps']:
            if key not in kwargs:
                continue
            cur_val = getattr(self, key)
            cur_val.update(kwargs[key])
            changed = True
        feed_overflows = kwargs.get('feed_overflows_allowed')
        if feed_overflows is not None:
            cur_val = self.feed_overflows_allowed
            s = set(cur_val) | set(feed_overflows)
            if s != set(cur_val):
                self.feed_overflows_allowed = list(s)
                changed = True
        return changed

    @classmethod
    def build_defaults(cls, **kwargs) -> Self:
        out_dir = Path('data') / 'legistar'
        out_dir_abs = out_dir.resolve()
        default_kw = dict(
            out_dir=out_dir,
            out_dir_abs=out_dir_abs,
            data_file=out_dir / 'data.json',
            feed_urls={},
            feed_overflows_allowed=[],
            category_maps={},
        )
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file']
        d: dict[str, object] = {k: str(getattr(self, k)) for k in path_attrs}
        d.update(dict(
            feed_urls={k:str(v) for k,v in self.feed_urls.items()},
            feed_overflows_allowed=self.feed_overflows_allowed,
            category_maps=self.category_maps,
        ))
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            out_dir=Path(data['out_dir']),
            out_dir_abs=Path(data['out_dir_abs']),
            data_file=Path(data['data_file']),
            feed_urls={k:URL(v) for k,v in data['feed_urls'].items()},
            feed_overflows_allowed=data.get('feed_overflows_allowed', []),
            category_maps=data['category_maps'],
        )



@dataclass
class Config(BaseConfig):
    out_dir: Path
    """Root directory to store downloaded files (relatve to the current
    working directory)
    """
    out_dir_abs: Path
    """Root directory to store downloaded files (absolute path)
    """
    data_file: Path
    """Filename to store download information. Defaults to "<out-dir>/data.json"
    """
    timestamp_file: Path
    """Filename to store clip timestamp information. Defaults to "<out-dir>/timestamp-data.yaml"
    """
    granicus_data_url: URL|None
    """URL for granicus clip data
    """

    legistar: LegistarConfig
    """:class:`LegistarConfig` instance
    """

    google: GoogleConfig
    """:class:`GoogleConfig` instance
    """

    local_timezone_name: str|None
    default_filename: ClassVar[Path] = Path.home() / '.granicus.conf.yaml'

    def __post_init__(self) -> None:
        assert self.out_dir != self.legistar.out_dir
        assert self.out_dir_abs != self.legistar.out_dir_abs
        assert self.data_file != self.legistar.data_file
        assert self.data_file.resolve() != self.legistar.data_file.resolve()

    @property
    def local_timezone(self) -> ZoneInfo:
        if self.local_timezone_name is None:
            raise ValueError('local timezone not set')
        return ZoneInfo(self.local_timezone_name)

    @classmethod
    def load(cls, filename: PathLike) -> Self:
        """Load the config from the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = yaml_load(filename.read_text(), Loader=YamlLoader)
        return cls.deserialize(data)

    def save(self, filename: PathLike) -> None:
        """Save the config to the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        s = yaml_dump(self.serialize(), Dumper=YamlDumper)
        filename.write_text(s)

    def update(self, **kwargs) -> bool:
        changed = False
        out_dir = kwargs.get('out_dir')
        if out_dir is not None and out_dir != self.out_dir:
            assert isinstance(out_dir, Path)
            assert not out_dir.is_absolute()
            self.out_dir = out_dir
            self.out_dir_abs = out_dir.resolve()
            changed = True
        out_dir_abs = kwargs.get('out_dir_abs')
        if out_dir_abs is not None and out_dir_abs != self.out_dir_abs:
            assert isinstance(out_dir_abs, Path)
            assert out_dir_abs.is_absolute()
            self.out_dir_abs = out_dir_abs
            self.out_dir = out_dir_abs.relative_to(Path.cwd())
            changed = True

        path_attrs = ['data_file', 'timestamp_file']

        for key, val in kwargs.items():
            if val is None:
                continue
            if key in path_attrs and val != getattr(self, key):
                assert isinstance(val, Path)
                setattr(self, key, val)
                changed = True
            elif key == 'granicus_data_url':
                val = URL(val)
                if self.granicus_data_url == val:
                    continue
                self.granicus_data_url = val
                changed = True
            elif key == 'legistar':
                if self.legistar.update(**val):
                    changed = True
            elif key == 'google':
                if self.google.update(**val):
                    changed = True
            elif key == 'local_timezone_name':
                if val == self.local_timezone_name:
                    continue
                self.local_timezone_name = val
                changed = True
        return changed

    @classmethod
    def build_defaults(cls, **kwargs) -> Self:
        out_dir = Path('data/granicus')
        out_dir_abs = out_dir.resolve()
        # out_dir_abs: Path = kwargs.get('out_dir', Path.cwd() / 'data')
        # out_dir = out_dir_abs.relative_to(Path.cwd())
        data_url = kwargs.get('data_url')
        if data_url is not None:
            data_url = URL(data_url)
        feed_url = kwargs.get('legistar_feed_url')
        if feed_url is not None:
            feed_url = URL(feed_url)
        default_kw = dict(
            out_dir=out_dir,
            out_dir_abs=out_dir_abs,
            data_file=out_dir / 'data.json',
            timestamp_file=out_dir / 'timestamp-data.yaml',
            granicus_data_url=data_url,
            local_timezone_name=None,
            legistar=LegistarConfig.build_defaults(**kwargs.get('legistar', {})),
            google=GoogleConfig.build_defaults(**kwargs.get('google', {}))
        )
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file', 'timestamp_file']
        d: dict[str, object] = {k: str(getattr(self, k)) for k in path_attrs}
        data_url = self.granicus_data_url
        d['granicus_data_url'] = None if data_url is None else str(data_url)
        d['google'] = self.google.serialize()
        d['legistar'] = self.legistar.serialize()
        d['local_timezone_name'] = self.local_timezone_name
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file', 'timestamp_file']
        kw = {k: Path(data[k]) for k in path_attrs}
        data_url = data.get('granicus_data_url')
        if data_url is not None:
            data_url = URL(data_url)
        legistar: LegistarConfig
        if 'legistar' in data:
            legistar = LegistarConfig.deserialize(data['legistar'])
        else:
            legistar = LegistarConfig.build_defaults()
        return cls(
            granicus_data_url=data_url,
            google=GoogleConfig.deserialize(data['google']),
            legistar=legistar,
            local_timezone_name=data.get('local_timezone_name'),
            **kw
        )

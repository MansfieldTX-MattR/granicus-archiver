from __future__ import annotations
from typing import (
    ClassVar, Literal, Iterator, Any, Self, get_type_hints, TYPE_CHECKING,
)
from abc import abstractmethod
from pathlib import Path
import os
from os import PathLike
import json
import dataclasses
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

from yarl import URL

from yaml import (
    load as yaml_load,
    dump as yaml_dump,
    CLoader as YamlLoader,
    CDumper as YamlDumper,
)
from loguru import logger
from appdirs import AppDirs

from .types import Serializable
if TYPE_CHECKING:
    from .clips.model import Location
    from .legistar.types import Category
    from .aws.client import Key
else:
    Location = str
    Category = str

GroupKey = Literal['root', 'google', 'aws', 'legistar']
""""""


APP_NAME = 'granicus-archiver'
APP_AUTHOR = 'granicus-archiver'

APP_DIRS = AppDirs(APP_NAME, APP_AUTHOR)


def get_app_config(*parts: Path|str) -> Path:
    """Get the user config directory for the app

    Any arguments (if provided) will be joined to the path
    """
    return Path(APP_DIRS.user_config_dir, *parts)

def get_app_cache(*parts: Path|str) -> Path:
    """Get the user cache directory for the app

    Any arguments (if provided) will be joined to the path
    """
    return Path(APP_DIRS.user_cache_dir, *parts)


@dataclass
class BaseConfig(Serializable):
    group_key: ClassVar[GroupKey]
    """Unique key for :class:`BaseConfig` subclasses"""
    _env_prefix: ClassVar[str] = 'GRANICUS_ARCHIVER'

    @classmethod
    def _get_env_key(cls, key: str) -> str:
        return f'{cls._env_prefix}_{cls.group_key.upper()}_{key.upper()}'

    @classmethod
    def iter_child_config_classes(cls) -> Iterator[tuple[str, type[BaseConfig]]]:
        """Iterate over child config classes
        """
        for attr, val_type in get_type_hints(cls).items():
            if not isinstance(val_type, type):
                continue
            if issubclass(val_type, BaseConfig):
                yield attr, val_type

    @property
    def child_configs(self) -> dict[str, BaseConfig]:
        """Mapping of child config instances by their attribute name
        """
        return {
            k:getattr(self, k)
            for k, _ in self.iter_child_config_classes()
        }

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

    @classmethod
    @abstractmethod
    def load_from_env(cls) -> Self:
        """Load the config from environment variables
        """

    def as_dotenv(self) -> str:
        """Convert the config to a dotenv string
        """
        data = self.serialize()
        child_configs = self.child_configs
        lines = []
        for key, val in data.items():
            if key in child_configs:
                child_conf = child_configs[key]
                lines.extend(child_conf.as_dotenv().splitlines())
            else:
                if isinstance(val, (list, dict)):
                    val = json.dumps(val)
                lines.append(f'{self._get_env_key(key)}={val}')
        return '\n'.join(lines)

    @classmethod
    def _get_env_var[Vt: (str, Path, URL)](
        cls, key: str, val_type: type[Vt]
    ) -> Vt|None:
        env_key = cls._get_env_key(key)
        if env_key not in os.environ:
            return None
        val = os.environ[env_key].strip()
        return val_type(val)

    @classmethod
    def _get_env_var_list[Vt: (str, Path, URL)](
        cls, key: str, val_type: type[Vt]
    ) -> list[Vt]|None:
        val = cls._get_env_var(key, str)
        if val is None:
            return None
        val_list = json.loads(val)
        return [val_type(v) for v in val_list]

    @classmethod
    def _get_env_var_dict[Kt: (str), Vt: (str, Path, URL)](
        cls, key: str, key_type: type[Kt], val_type: type[Vt]
    ) -> dict[Kt, Vt]|None:
        val = cls._get_env_var(key, str)
        if val is None:
            return None
        d = json.loads(val)
        return {key_type(k):val_type(v) for k,v in d.items()}


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

    folder_cache_file: Path
    """Path to store folder cache"""

    meta_cache_file: Path
    """Path to store metadata cache"""

    group_key: ClassVar[GroupKey] = 'google'

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
            folder_cache_file=get_app_cache('google-folder-cache.json'),
            meta_cache_file=get_app_cache('google-meta-cache.json'),
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
            folder_cache_file=str(self.folder_cache_file),
            meta_cache_file=str(self.meta_cache_file),
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        rg_folder = data.get(
            'rguid_legistar_drive_folder', 'granicus-archive/data/legistar-rguid'
        )
        for key in ['folder_cache_file', 'meta_cache_file']:
            if key not in data:
                obj = cls.build_defaults()
                data[key] = getattr(obj, key)
        return cls(
            user_credentials_filename=Path(data['user_credentials_filename']),
            drive_folder=Path(data['drive_folder']),
            legistar_drive_folder=Path(data['legistar_drive_folder']),
            rguid_legistar_drive_folder=Path(rg_folder),
            folder_cache_file=Path(data['folder_cache_file']),
            meta_cache_file=Path(data['meta_cache_file']),
        )

    @classmethod
    def load_from_env(cls) -> Self:
        kw = dict(
            user_credentials_filename=cls._get_env_var('user_credentials_filename', Path),
            drive_folder=cls._get_env_var('drive_folder', Path),
            legistar_drive_folder=cls._get_env_var('legistar_drive_folder', Path),
            rguid_legistar_drive_folder=cls._get_env_var('rguid_legistar_drive_folder', Path),
            folder_cache_file=cls._get_env_var('folder_cache_file', Path),
            meta_cache_file=cls._get_env_var('meta_cache_file', Path),
        )
        kw = {k:v for k,v in kw.items() if v is not None}
        return cls.build_defaults(**kw)

@dataclass
class AWSConfig(BaseConfig):
    """AWS Config
    """
    default_object_url_fmt: ClassVar[str] = 'https://s3.amazonaws.com/{bucket_name}/{key}'
    """Default :attr:`object_url_format`"""

    bucket_name: str
    """The bucket to use for the archive"""
    clips_prefix: Path
    """Prefix for clips"""
    legistar_prefix: Path
    """Prefix for legistar items"""
    legistar_rguid_prefix: Path
    """Prefix for rguid legistar items"""
    region_name: str|None = None
    """AWS region name.  If not set, the default region will be used"""
    s3_endpoint_url: URL|None = None
    """AWS S3 endpoint URL.  If not set, the default endpoint will be used"""
    credentials_profile: str = 'default'
    """The AWS credentials profile to use (from ``~/.aws/credentials``)"""
    access_key_id: str|None = None
    """AWS Access Key ID.  If not set, the default credentials provider chain will be used"""
    secret_access_key: str|None = None
    """AWS Secret Access Key.  If not set, the default credentials provider chain will be used"""
    object_url_format: str = default_object_url_fmt
    """Format string for generating object URLs.

    Required fields are

    - :attr:`bucket_name`
    - ``key`` (the S3 object key name)
    """

    group_key: ClassVar[GroupKey] = 'aws'

    @classmethod
    def _validate_object_url_format(cls, fmt: str) -> bool:
        try:
            fmt.format(bucket_name='test-bucket', key='test/key')
        except KeyError as e:
            raise ValueError(f'Invalid object_url_format, missing key: {e}') from e
        return True

    def update(self, **kwargs) -> bool:
        changed = False
        not_required_keys = [
            'region_name', 's3_endpoint_url', 'credentials_profile',
            'access_key_id', 'secret_access_key',
        ]
        path_keys = ['clips_prefix', 'legistar_prefix', 'legistar_rguid_prefix']
        required_keys = ['bucket_name'] + path_keys
        keys = required_keys + not_required_keys
        for key in keys:
            if key not in kwargs:
                continue
            val = kwargs[key]
            if val == getattr(self, key):
                continue
            if key in path_keys:
                assert isinstance(val, Path)
                assert not val.is_absolute()
            setattr(self, key, val)
            changed = True
        if 'object_url_format' in kwargs:
            val = kwargs['object_url_format']
            if val != self.object_url_format:
                self._validate_object_url_format(val)
                self.object_url_format = val
                changed = True
        return changed

    def get_object_url(self, key: Key, scheme: str|None = None) -> URL:
        """Get a URL for an S3 key within :attr:`bucket_name`
        """
        fmt_kwargs = dict(
            bucket_name=self.bucket_name,
            key=key,
        )
        if self.region_name is not None:
            fmt_kwargs['region_name'] = self.region_name
        if self.s3_endpoint_url is not None:
            fmt_kwargs['s3_endpoint_url'] = str(self.s3_endpoint_url)
        url_str = self.object_url_format.format(**fmt_kwargs)
        url = URL(url_str)
        if scheme is not None:
            url = url.with_scheme(scheme)
        return url

    @classmethod
    def build_defaults(cls, **kwargs) -> Self:
        default_kw = dict(
            bucket_name='',
            clips_prefix=Path('clips'),
            legistar_prefix=Path('legistar'),
            legistar_rguid_prefix=Path('legistar-rguid'),
            region_name=None,
            s3_endpoint_url=None,
            credentials_profile='default',
            access_key_id=None,
            secret_access_key=None,
            object_url_format=cls.default_object_url_fmt,
        )
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        return dict(
            bucket_name=self.bucket_name,
            clips_prefix=str(self.clips_prefix),
            legistar_prefix=str(self.legistar_prefix),
            legistar_rguid_prefix=str(self.legistar_rguid_prefix),
            region_name=self.region_name,
            s3_endpoint_url=str(self.s3_endpoint_url) if self.s3_endpoint_url else None,
            credentials_profile=self.credentials_profile,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            object_url_format=self.object_url_format,
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            bucket_name=data['bucket_name'],
            clips_prefix=Path(data['clips_prefix']),
            legistar_prefix=Path(data['legistar_prefix']),
            legistar_rguid_prefix=Path(data['legistar_rguid_prefix']),
            region_name=data.get('region_name'),
            s3_endpoint_url=URL(data['s3_endpoint_url']) if data.get('s3_endpoint_url') else None,
            access_key_id=data.get('access_key_id'),
            secret_access_key=data.get('secret_access_key'),
            credentials_profile=data.get('credentials_profile', 'default'),
            object_url_format=data.get('object_url_format', cls.default_object_url_fmt),
        )

    @classmethod
    def load_from_env(cls) -> Self:
        credentials_profile = cls._get_env_var('credentials_profile', str)
        if credentials_profile is None or not len(credentials_profile):
            credentials_profile = 'default'
        kw = dict(
            bucket_name=cls._get_env_var('bucket_name', str),
            clips_prefix=cls._get_env_var('clips_prefix', Path),
            legistar_prefix=cls._get_env_var('legistar_prefix', Path),
            legistar_rguid_prefix=cls._get_env_var('legistar_rguid_prefix', Path),
            region_name=cls._get_env_var('region_name', str),
            s3_endpoint_url=cls._get_env_var('s3_endpoint_url', URL),
            credentials_profile=credentials_profile,
            access_key_id=cls._get_env_var('access_key_id', str),
            secret_access_key=cls._get_env_var('secret_access_key', str),
            object_url_format=cls._get_env_var('object_url_format', str),
        )
        kw = {k:v for k,v in kw.items() if v is not None}
        return cls.build_defaults(**kw)


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

    group_key: ClassVar[GroupKey] = 'legistar'

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

    @classmethod
    def load_from_env(cls) -> Self:
        kw = dict(
            out_dir=cls._get_env_var('out_dir', Path),
            out_dir_abs=cls._get_env_var('out_dir_abs', Path),
            data_file=cls._get_env_var('data_file', Path),
            feed_urls=cls._get_env_var_dict('feed_urls', str, URL),
            feed_overflows_allowed=cls._get_env_var_list('feed_overflows_allowed', str),
            category_maps=cls._get_env_var_dict('category_maps', str, str),
        )
        kw = {k:v for k,v in kw.items() if v is not None}
        return cls.build_defaults(**kw)


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

    aws: AWSConfig
    """:class:`AWSConfig` instance
    """

    local_timezone_name: str|None
    """Local timezone name for all granicus / legistar items"""
    default_filename: ClassVar[Path] = get_app_config('config.yaml')

    group_key: ClassVar[GroupKey] = 'root'

    app_dirs: ClassVar[AppDirs] = AppDirs(APP_NAME, APP_AUTHOR)
    _read_only: ClassVar[bool] = True

    def __post_init__(self) -> None:
        assert self.out_dir != self.legistar.out_dir
        assert self.out_dir_abs != self.legistar.out_dir_abs
        assert self.data_file != self.legistar.data_file
        assert self.data_file.resolve() != self.legistar.data_file.resolve()

    @classmethod
    def get_app_config(cls, *parts: Path|str) -> Path:
        """Get the user config directory for the app

        Any arguments (if provided) will be joined to the path
        """
        return get_app_config(*parts)

    @classmethod
    def get_app_cache(cls, *parts: Path|str) -> Path:
        """Get the user cache directory for the app

        Any arguments (if provided) will be joined to the path
        """
        return get_app_cache(*parts)

    @property
    def local_timezone(self) -> ZoneInfo:
        if self.local_timezone_name is None:
            raise ValueError('local timezone not set')
        return ZoneInfo(self.local_timezone_name)

    def get_group(self, key: GroupKey) -> BaseConfig:
        """Get a :class:`BaseConfig` instance by its :attr:`~BaseConfig.group_key`
        """
        if key == 'root':
            return self
        obj = getattr(self, key)
        return obj

    @classmethod
    def load(cls, filename: PathLike) -> Self:
        """Load the config from the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        if not filename.exists():
            logger.warning(f'Config file {filename} does not exist. Using defaults')
            return cls.build_defaults()
        data = yaml_load(filename.read_text(), Loader=YamlLoader)
        return cls.deserialize(data)

    def save(self, filename: PathLike) -> None:
        """Save the config to the given filename
        """
        if self._read_only:
            raise ValueError('Config is read-only')
        if not isinstance(filename, Path):
            filename = Path(filename)
        filename.parent.mkdir(parents=True, exist_ok=True)
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
        child_configs = self.child_configs

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
            elif key in child_configs:
                if child_configs[key].update(**val):
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
        )
        for attr, conf_cls in cls.iter_child_config_classes():
            if attr in kwargs and isinstance(kwargs[attr], conf_cls):
                continue
            val = conf_cls.build_defaults(**kwargs.get(attr, {}))
            kwargs[attr] = val
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file', 'timestamp_file']
        d: dict[str, object] = {k: str(getattr(self, k)) for k in path_attrs}
        data_url = self.granicus_data_url
        d['granicus_data_url'] = None if data_url is None else str(data_url)
        for key, child_conf in self.child_configs.items():
            d[key] = child_conf.serialize()
        d['local_timezone_name'] = self.local_timezone_name
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file', 'timestamp_file']
        kw: dict[str, Any] = {k: Path(data[k]) for k in path_attrs}
        data_url = data.get('granicus_data_url')
        if data_url is not None:
            data_url = URL(data_url)
        for key, val_type in cls.iter_child_config_classes():
            if key not in data:
                child_conf = val_type.build_defaults()
            else:
                child_conf = val_type.deserialize(data[key])
            kw[key] = child_conf
        return cls(
            granicus_data_url=data_url,
            local_timezone_name=data.get('local_timezone_name'),
            **kw
        )

    @classmethod
    def load_from_env(cls) -> Self:
        kw: dict[str, Any] = dict(
            out_dir=cls._get_env_var('out_dir', Path),
            out_dir_abs=cls._get_env_var('out_dir_abs', Path),
            data_file=cls._get_env_var('data_file', Path),
            timestamp_file=cls._get_env_var('timestamp_file', Path),
            granicus_data_url=cls._get_env_var('granicus_data_url', URL),
            local_timezone_name=cls._get_env_var('local_timezone_name', str),
        )
        kw = {k:v for k,v in kw.items() if v is not None}
        for key, val_type in cls.iter_child_config_classes():
            kw[key] = val_type.load_from_env()
        return cls.build_defaults(**kw)

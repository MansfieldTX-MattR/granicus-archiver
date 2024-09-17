from __future__ import annotations
from typing import ClassVar, Any, Self
from abc import ABC, abstractmethod
from pathlib import Path
from os import PathLike
from dataclasses import dataclass, field

from yarl import URL

from yaml import (
    load as yaml_load,
    dump as yaml_dump,
    CLoader as YamlLoader,
    CDumper as YamlDumper,
)

from .model import Serializable



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

    def update(self, **kwargs) -> bool:
        changed = False
        for key in ['user_credentials_filename', 'drive_folder']:
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
            drive_folder=Path('granicus-archive/data'),
        )
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        return dict(
            user_credentials_filename=str(self.user_credentials_filename),
            drive_folder=str(self.drive_folder)
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            user_credentials_filename=Path(data['user_credentials_filename']),
            drive_folder=Path(data['drive_folder']),
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
    legistar_data_file: Path
    legistar_feed_url: URL|None

    google: GoogleConfig
    """:class:`GoogleConfig` instance
    """

    legistar_category_maps: dict[str,str] = field(default_factory=dict)
    default_filename: ClassVar[Path] = Path.home() / '.granicus.conf.yaml'

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

        path_attrs = ['data_file', 'legistar_data_file', 'timestamp_file']

        for key, val in kwargs.items():
            if val is None:
                continue
            if key in path_attrs and val != getattr(self, key):
                assert isinstance(val, Path)
                setattr(self, key, val)
                changed = True
            elif key == 'legistar_feed_url':
                if not isinstance(val, URL):
                    val = URL(val)
                setattr(self, key, val)
                changed = True
            elif key == 'legistar_category_maps':
                self.legistar_category_maps.update(val)
            elif key == 'google':
                if self.google.update(**val):
                    changed = True
        return changed

    @classmethod
    def build_defaults(cls, **kwargs) -> Self:
        out_dir = Path('data')
        out_dir_abs = out_dir.resolve()
        # out_dir_abs: Path = kwargs.get('out_dir', Path.cwd() / 'data')
        # out_dir = out_dir_abs.relative_to(Path.cwd())
        feed_url = kwargs.get('legistar_feed_url')
        if feed_url is not None:
            feed_url = URL(feed_url)
        default_kw = dict(
            out_dir=out_dir,
            out_dir_abs=out_dir_abs,
            data_file=out_dir / 'data.json',
            legistar_data_file=out_dir / 'legistar-data.json',
            legistar_feed_url=feed_url,
            legistar_category_maps={},
            timestamp_file=out_dir / 'timestamp-data.yaml',
            google=GoogleConfig.build_defaults(**kwargs.get('google', {}))
        )
        for key, val in default_kw.items():
            kwargs.setdefault(key, val)
        return cls(**kwargs)

    def serialize(self) -> dict[str, Any]:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file', 'legistar_data_file', 'timestamp_file']
        d: dict[str, object] = {k: str(getattr(self, k)) for k in path_attrs}
        d['google'] = self.google.serialize()
        d['legistar_feed_url'] = None if self.legistar_feed_url is None else str(self.legistar_feed_url)
        d['legistar_category_maps'] = self.legistar_category_maps
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        path_attrs = ['out_dir', 'out_dir_abs', 'data_file', 'timestamp_file']
        kw = {k: Path(data[k]) for k in path_attrs}
        legistar_data_file = data.get('legistar_data_file')
        if legistar_data_file is not None:
            legistar_data_file = Path(legistar_data_file)
        else:
            def_obj = cls.build_defaults(**kw)
            legistar_data_file = def_obj.legistar_data_file
        kw['legistar_data_file'] = legistar_data_file
        feed_url = data.get('legistar_feed_url')
        if feed_url is not None:
            feed_url = URL(feed_url)
        return cls(
            google=GoogleConfig.deserialize(data['google']),
            legistar_feed_url=feed_url,
            legistar_category_maps=data.get('legistar_category_maps', {}),
            **kw
        )

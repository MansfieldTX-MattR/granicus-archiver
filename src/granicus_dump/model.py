from __future__ import annotations

from typing import Self, ClassVar, Literal, Iterator, Any
from abc import ABC, abstractmethod

from pathlib import Path
import dataclasses
from dataclasses import dataclass, field
import datetime
import json

from yarl import URL

# __all__ = ('CLIP_ID', 'ParseClipData', 'ClipCollection')

CLIP_ID = str
ClipFileKey = Literal['agenda', 'minutes', 'audio', 'video']


class Serializable(ABC):

    @abstractmethod
    def serialize(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError


@dataclass
class ParseClipLinks(Serializable):
    agenda: URL|None = None
    minutes: URL|None = None
    audio: URL|None = None
    video: URL|None = None

    link_attrs: ClassVar[list[ClipFileKey]] = ['agenda', 'minutes', 'audio', 'video']

    @property
    def links(self) -> dict[ClipFileKey, URL|None]:
        return {k:v for k,v in self}

    @property
    def existing_links(self) -> dict[ClipFileKey, URL]:
        return {k:v for k,v in self.iter_existing()}

    def __getitem__(self, key: ClipFileKey) -> URL|None:
        assert key in self.link_attrs
        return getattr(self, key)

    def __iter__(self) -> Iterator[tuple[ClipFileKey, URL|None]]:
        for attr in self.link_attrs:
            yield attr, self[attr]

    def iter_existing(self) -> Iterator[tuple[ClipFileKey, URL]]:
        for attr, val in self:
            if val is not None:
                yield attr, val

    def serialize(self) -> dict[str, Any]:
        return {k: str(v) if v else v for k,v in self}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = {k: v if not v else URL(v) for k,v in data.items()}
        return cls(**kw)


@dataclass
class ParseClipData(Serializable):
    id: CLIP_ID
    location: str
    name: str
    date: int
    duration: int
    original_links: ParseClipLinks
    actual_links: ParseClipLinks|None = None

    date_fmt: ClassVar[str] = '%Y-%m-%d'

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.date)

    @property
    def title_name(self) -> str:
        dt_str = self.datetime.strftime(self.date_fmt)
        return f'{dt_str}_{self.name}_{self.id}'

    @property
    def unique_name(self) -> str:
        return f'{self.id}_{self.title_name}'

    def build_fs_path(self, root_dir: Path) -> Path:
        return root_dir / self.location / self.title_name

    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d['original_links'] = self.original_links.serialize()
        if self.actual_links is not None:
            d['actual_links'] = self.actual_links.serialize()
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        for key in ['original_links', 'actual_links']:
            if kw[key] is None:
                continue
            kw[key] = ParseClipLinks.deserialize(kw[key])
        return cls(**kw)


@dataclass
class ClipFiles(Serializable):
    agenda: Path|None
    minutes: Path|None
    audio: Path|None
    video: Path|None

    path_attrs: ClassVar[list[ClipFileKey]] = ['agenda', 'minutes', 'audio', 'video']

    @classmethod
    def from_parse_data(cls, parse_data: ParseClipData, root_dir: Path) -> Self:
        links = parse_data.original_links
        kw = {k: v if not v else cls.build_path(root_dir, k) for k,v in links}
        return cls(**kw)

    @classmethod
    def build_path(cls, root_dir: Path, key: ClipFileKey) -> Path:
        suffixes: dict[ClipFileKey, str] = {
            'agenda':'.pdf',
            'minutes':'.pdf',
            'audio':'.mp3',
            'video':'.mp4',
        }
        filename = f'{key}{suffixes[key]}'
        return root_dir / filename

    @property
    def paths(self) -> dict[ClipFileKey, Path|None]:
        return {k:v for k,v in self}

    @property
    def existing_paths(self) -> dict[ClipFileKey, Path]:
        return {k:v for k,v in self.iter_existing()}

    @property
    def complete(self) -> bool:
        paths = self.existing_paths
        return all([p.exists() for p in paths.values()])

    def relative_to(self, rel_dir: Path) -> Self:
        # paths: dict[ClipFileKey, Path] = {
        #     k:v.relative_to(rel_dir) for k,v in self.existing_paths.items()
        # }
        # missing_keys = set(self.path_attrs) - set(paths.keys())
        paths = {k:v.relative_to(rel_dir) for k,v in self.existing_paths.items()}
        return dataclasses.replace(self, **paths)
        # kw = self.serialize()
        # kw.update(paths)
        # return self.__class__

    def __getitem__(self, key: ClipFileKey) -> Path|None:
        assert key in self.path_attrs
        return getattr(self, key)

    def __iter__(self) -> Iterator[tuple[ClipFileKey, Path|None]]:
        for attr in self.path_attrs:
            yield attr, self[attr]

    def iter_existing(self) -> Iterator[tuple[ClipFileKey, Path]]:
        for attr, p in self:
            if p is not None:
                yield attr, p

    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        for key, val in d.copy().items():
            if val is None:
                continue
            d[key] = str(val)
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = {}
        for key, val in data.items():
            if val is not None:
                val = Path(val)
            kw[key] = val
        return cls(**kw)


@dataclass
class Clip(Serializable):
    parse_data: ParseClipData
    root_dir: Path
    files: ClipFiles

    @property
    def id(self) -> CLIP_ID: return self.parse_data.id

    @property
    def unique_name(self) -> str: return self.parse_data.unique_name

    @property
    def location(self) -> str: return self.parse_data.location

    @property
    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.parse_data.date)

    @property
    def duration(self):
        return datetime.timedelta(seconds=self.parse_data.duration)

    @property
    def complete(self) -> bool: return self.files.complete

    @classmethod
    def from_parse_data(cls, parse_data: ParseClipData, base_dir: Path) -> Self:
        root_dir = parse_data.build_fs_path(base_dir)
        return cls(
            parse_data=parse_data,
            root_dir=root_dir,
            files=ClipFiles.from_parse_data(
                parse_data=parse_data, root_dir=root_dir,
            )
        )

    def iter_url_paths(self, actual: bool = True) -> Iterator[tuple[URL, Path]]:
        if actual:
            assert self.parse_data.actual_links is not None
            it = self.parse_data.actual_links.iter_existing()
        else:
            it = self.parse_data.original_links.iter_existing()
        for urlkey, url in it:
            filename = self.files.build_path(self.root_dir, urlkey)
            yield url, filename

    def relative_to(self, base_dir: Path) -> Self:
        new_root_dir = self.parse_data.build_fs_path(base_dir)
        new_files = self.files.relative_to(new_root_dir)
        return dataclasses.replace(self, root_dir=new_root_dir, files=new_files)

    def serialize(self) -> dict[str, Any]:
        return dict(
            parse_data=self.parse_data.serialize(),
            root_dir=str(self.root_dir),
            files=self.files.serialize(),
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(
            parse_data=ParseClipData.deserialize(data['parse_data']),
            root_dir=Path(data['root_dir']),
            files=ClipFiles.deserialize(data['files']),
        )


@dataclass
class ClipCollection(Serializable):
    base_dir: Path
    clips: dict[CLIP_ID, Clip] = field(default_factory=dict)

    @classmethod
    def load(cls, filename: Path) -> Self:
        data = json.loads(filename.read_text())
        return cls.deserialize(data)

    def save(self, filename: Path, indent: int|None = 2) -> None:
        data = self.serialize()
        filename.write_text(json.dumps(data, indent=indent))

    def add_clip(self, parse_data: ParseClipData) -> Clip:
        clip = Clip.from_parse_data(parse_data=parse_data, base_dir=self.base_dir)
        if clip.id in self.clips:
            raise KeyError(f'Clip with id "{clip.id}" exists')
        self.clips[clip.id] = clip
        return clip

    def __contains__(self, key):
        return key in self.clips

    def __len__(self):
        return len(self.clips)

    def __iter__(self):
        yield from self.clips.values()

    def relative_to(self, base_dir: Path) -> Self:
        clips = {k:v.relative_to(base_dir) for k,v in self.clips.items()}
        return dataclasses.replace(
            self,
            base_dir=self.base_dir.relative_to(base_dir), clips=clips,
        )

    def serialize(self) -> dict[str, Any]:
        clips = {k: v.serialize() for k,v in self.clips.items()}
        return dict(base_dir=str(self.base_dir), clips=clips)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        clips: dict[CLIP_ID, Clip] = {}
        for key, val in data['clips'].items():
            clip = Clip.deserialize(val)
            assert clip.id == key
            clips[key] = clip
        return cls(base_dir=Path(data['base_dir']), clips=clips)

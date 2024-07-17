from typing import Self, ClassVar, Literal, Iterator, Any
from abc import ABC, abstractmethod

from pathlib import Path
import dataclasses
from dataclasses import dataclass, field
import datetime

from yarl import URL

# __all__ = ('CLIP_ID', 'ParseClipData', 'ClipCollection')

CLIP_ID = str

class Serializable(ABC):

    @abstractmethod
    def serialize(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError

# @dataclass
# class ParseClipLinks:
#     agenda: URL|None
#     minutes: URL|None
#     audio: URL|None
#     video: URL|None

ParseClipUrlKey = Literal['agenda_link', 'minutes_link', 'audio_link', 'video_link']

@dataclass
class ParseClipData(Serializable):
    id: CLIP_ID
    location: str
    name: str
    date: int
    duration: int
    # links: ParseClipLinks
    agenda_link: URL|None
    minutes_link: URL|None
    audio_link: URL|None
    video_link: URL|None

    date_fmt: ClassVar[str] = '%Y-%m-%d'
    url_keys: ClassVar[list[ParseClipUrlKey]] = [
        'agenda_link', 'minutes_link', 'audio_link', 'video_link'
    ]

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

    @property
    def links(self) -> dict[ParseClipUrlKey, URL|None]:
        return {k:getattr(self, k) for k in self.url_keys}

    @property
    def existing_links(self) -> dict[ParseClipUrlKey, URL]:
        return {k:v for k,v in self.links.items() if v is not None}

    def build_fs_path(self, root_dir: Path) -> Path:
        return root_dir / self.location / self.title_name

    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d.update({k:str(v) for k,v in self.links.items()})
        # for key in ['agenda_link', 'minutes_link', 'audio_link', 'video_link']:
        #     val = d[key]
        #     if isinstance(val, URL):
        #         d[key] = str(val)
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        for key in ['agenda_link', 'minutes_link', 'audio_link', 'video_link']:
            if kw[key] is None:
                continue
            kw[key] = URL(kw[key])
        return cls(**kw)


ClipFileKey = Literal['agenda', 'minutes', 'audio', 'video']

ClipFileUrlMap: dict[ClipFileKey, ParseClipUrlKey] = {
    'agenda': 'agenda_link',
    'minutes': 'minutes_link',
    'audio': 'audio_link',
    'video': 'video_link',
}

ClipUrlFileMap: dict[ParseClipUrlKey, ClipFileKey] = {#{v:k for k,v in ClipFileUrlMap.items()}
    'agenda_link':'agenda',
    'minutes_link':'minutes',
    'audio_link':'audio',
    'video_link':'video',
}

@dataclass
class ClipFiles(Serializable):
    agenda: Path|None
    minutes: Path|None
    audio: Path|None
    video: Path|None

    path_attrs: ClassVar[list[ClipFileKey]] = ['agenda', 'minutes', 'audio', 'video']

    @classmethod
    def from_parse_data(cls, parse_data: ParseClipData, root_dir: Path) -> Self:
        # agenda=root_dir / 'agenda.pdf' if parse_data.agenda_link else None
        return cls(
            agenda=root_dir / 'agenda.pdf' if parse_data.agenda_link else None,
            minutes=root_dir / 'minutes.pdf' if parse_data.minutes_link else None,
            audio=root_dir / 'audio.mp3' if parse_data.video_link else None,
            video=root_dir / 'video.mp4' if parse_data.video_link else None,
        )

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
        return {k:getattr(self, k) for k in self.path_attrs}

    @property
    def existing_paths(self) -> dict[ClipFileKey, Path]:
        return {k:v for k,v in self.paths.items() if v is not None}

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
    # agenda: Path|None
    # minutes: Path|None
    # audio: Path|None
    # video: Path|None

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

    def iter_url_paths(self) -> Iterator[tuple[URL, Path]]:
        urls = self.parse_data.existing_links
        for urlkey, url in urls.items():
            filekey = ClipUrlFileMap[urlkey]
            filename = self.files.build_path(self.root_dir, filekey)
            yield url, filename
        # paths = self.files.paths
        # for filekey, filename in paths.items():
        #     urlkey = ClipFileUrlMap[filekey]
        #     url = urls[urlkey]
        #     yield url, filename


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

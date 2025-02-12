from __future__ import annotations

from typing import NewType, Self, ClassVar, Literal, Iterator, Any, overload
from abc import ABC, abstractmethod

from os import PathLike
from pathlib import Path
import dataclasses
from dataclasses import dataclass, field
import datetime
import zoneinfo
import json
import mimetypes
from loguru import logger

from yaml import (
    load as yaml_load,
    dump as yaml_dump,
    CLoader as YamlLoader,
    CDumper as YamlDumper,
)
from yarl import URL
from multidict import MultiMapping

from .utils import (
    SHA1Hash, seconds_to_time_str, get_file_hash, HashMismatchError,
)

mimetypes.add_type('audio/mp3', '.mp3')

# __all__ = ('CLIP_ID', 'ParseClipData', 'ClipCollection')

UTC = datetime.timezone.utc

CLIP_ID = NewType('CLIP_ID', str)
"""Unique id for a :class:`Clip`"""
ClipFileKey = Literal['agenda', 'minutes', 'audio', 'video']
"""Key to for file types in :class:`ParseClipLinks` and :class:`ClipFiles`"""
ClipFileUploadKey = ClipFileKey | Literal['chapters', 'agenda_packet']
"""Key for file types in :class:`ClipFiles`"""

Headers = MultiMapping[str]|dict[str, str]

Location = NewType('Location', str)
"""The "Location" (or folder) of a clip"""


def set_timezone(tz: zoneinfo.ZoneInfo) -> None:
    ParseClipData.set_timezone(tz)


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
    """Links for clip assets
    """
    agenda: URL|None = None     #: Agenda link
    minutes: URL|None = None    #: Minutes link
    audio: URL|None = None      #: MP3 link
    video: URL|None = None      #: MP4 link

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

    def __setitem__(self, key: ClipFileKey, val: URL|None) -> None:
        assert key in self.link_attrs
        setattr(self, key, val)

    def __contains__(self, key: ClipFileKey):
        return self[key] is not None

    def __iter__(self) -> Iterator[tuple[ClipFileKey, URL|None]]:
        for attr in self.link_attrs:
            yield attr, self[attr]

    def iter_existing(self) -> Iterator[tuple[ClipFileKey, URL]]:
        for attr, val in self:
            if val is not None:
                yield attr, val

    def merge(self, other: ParseClipLinks) -> bool:
        """Merge any data missing in *self* from *other*
        """
        changed = False
        for key, oth_val in other.iter_existing():
            if self[key] is None:
                setattr(self, key, oth_val)
                changed = True
        return changed

    def serialize(self) -> dict[str, Any]:
        return {k: str(v) if v else v for k,v in self}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = {k: v if not v else URL(v) for k,v in data.items()}
        return cls(**kw)


@dataclass
class ParseClipData(Serializable):
    """Data for a clip parsed from granicus
    """
    id: CLIP_ID         #: The (assumingly) primary key of the clip
    location: Location  #: The "Location" (category or folder would be better terms)
    name: str           #: The clip name
    date: int           #: POSIX timestamp of the clip
    duration: int       #: Duration of the clip (in seconds)
    original_links: ParseClipLinks
    """The asset links as reported by granicus.  Some will be actually be
    redirects to a PDF viewer which will need to be resolved
    """

    actual_links: ParseClipLinks|None = None
    """The :attr:`original_links` after the redirects have been resolved
    """

    player_link: URL|None = None
    """URL for the popup video player with agenda view"""

    date_fmt: ClassVar[str] = '%Y-%m-%d'

    _timezone: ClassVar[zoneinfo.ZoneInfo|None] = None

    @classmethod
    def set_timezone(cls, tz: zoneinfo.ZoneInfo) -> None:
        cls._timezone = tz

    @classmethod
    def get_zimezone(cls) -> zoneinfo.ZoneInfo:
        assert cls._timezone is not None
        return cls._timezone

    @property
    def timezone(self) -> zoneinfo.ZoneInfo:
        return self.get_zimezone()

    @property
    def datetime(self) -> datetime.datetime:
        """The clip's datetime (derived from the :attr:`date`)
        """
        dt = datetime.datetime.fromtimestamp(self.date)
        return dt.replace(tzinfo=self.timezone)

    @property
    def title_name(self) -> str:
        """Combination of the clip's formatted :attr:`datetime`,
        :attr:`name` and :attr:`id`
        """
        dt_str = self.datetime.strftime(self.date_fmt)
        return f'{dt_str}_{self.name}_{self.id}'

    @property
    def unique_name(self) -> str:
        """A unique name for the clip
        """
        return f'{self.id}_{self.title_name}'

    def iter_incomplete_links(self) -> Iterator[tuple[ClipFileKey, URL]]:
        """Iterate over links existing in :attr:`original_links` but missing
        from :attr:`actual_links`
        """
        for key, url in self.original_links.iter_existing():
            if self.actual_links is None:
                yield key, url
            elif key not in self.actual_links:
                yield key, url

    def has_incomplete_links(self):
        """Check if any links in :attr:`original_links` are missing from
        :attr:`actual_links`
        """
        return len([key for key in self.iter_incomplete_links()]) > 0

    def build_fs_dir(self, root_dir: Path|None, replace_invalid: bool = True) -> Path:
        """Create a path for the clip within the given *root_dir*

        If *replace_invalid* is True, forward slashes ("/") will be replaced
        with colons (":") to prevent invalid and unexpected path names
        """
        year = self.datetime.strftime('%Y')
        title_name = self.title_name
        if replace_invalid:
            if '/' in title_name:
                title_name = title_name.replace('/', ':')
        stem = Path(year) / self.location / title_name
        if root_dir is None:
            return stem
        return root_dir / stem

    def check(self, other: Self) -> None:
        """Check *other* for any missing data in self
        """
        self.original_links.merge(other.original_links)
        if self.player_link is None and other.player_link is not None:
            self.player_link = other.player_link

    def update(self, other: Self) -> None:
        if self.location != other.location:
            self.location = other.location

    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d['original_links'] = self.original_links.serialize()
        if self.actual_links is not None:
            d['actual_links'] = self.actual_links.serialize()
        if self.player_link is not None:
            d['player_link'] = str(self.player_link)
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        for key in ['original_links', 'actual_links']:
            if kw[key] is None:
                continue
            kw[key] = ParseClipLinks.deserialize(kw[key])
        p_link = kw.get('player_link')
        if p_link is not None:
            kw['player_link'] = URL(p_link)
        return cls(**kw)


@dataclass
class FileMeta(Serializable):
    """Metadata for a file
    """
    content_length: int                     #: File size (in bytes)
    content_type: str                       #: The file's mime type
    last_modified: datetime.datetime|None   #: Last modified datetime
    etag: str|None                          #: The etag value (if available)
    sha1: SHA1Hash|None = None              #: SHA1 hash of the file

    # Tue, 04 Jun 2024 00:22:54 GMT
    dt_fmt: ClassVar[str] = '%a, %d %b %Y %H:%M:%S GMT'

    @classmethod
    def from_headers(cls, headers: Headers) -> Self:
        """Create an instance from http headers
        """
        dt_str = headers.get('Last-Modified')
        if dt_str is not None:
            dt = datetime.datetime.strptime(dt_str, cls.dt_fmt).replace(tzinfo=UTC)
        else:
            dt = None
        etag = headers.get('Etag')
        if etag is not None:
            etag = etag.strip('"')
        return cls(
            content_length=int(headers.get('Content-Length', '0')),
            content_type=headers['Content-Type'],
            last_modified=dt,
            etag=etag,
        )

    @classmethod
    def create_zero_length(cls) -> Self:
        """Create an instance to indicate that the file has a reported length
        of zero

        This may be used to indicate that a file is malformed or no longer
        exists on the server.

        Zero-length :class:`FileMeta` instances can be detected from their
        :attr:`is_zero_length` attribute.
        """
        return cls(
            content_length=-1,
            content_type='__none__',
            last_modified=None,
            etag=None,
        )

    @property
    def is_pdf(self) -> bool:
        """Whether this is a pdf file
        """
        return self.content_type == 'application/pdf'

    @property
    def is_zero_length(self) -> bool:
        """Whether this instance represents a zero-length file (created by
        :meth:`create_zero_length`)
        """
        return self.content_length == -1 and self.content_type == '__none__'

    def serialize(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        if self.last_modified is not None:
            dt = self.last_modified.astimezone(UTC)
            d['last_modified'] = dt.strftime(self.dt_fmt)
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = data.copy()
        if kw['last_modified'] is not None:
            dt = datetime.datetime.strptime(kw['last_modified'], cls.dt_fmt)
            kw['last_modified'] = dt.replace(tzinfo=UTC)
        return cls(**kw)


class CheckError(Exception):
    """Base exception for :meth:`ClipFiles.check`
    """
    key: ClipFileUploadKey
    def __init__(
        self,
        clip: Clip,
        key: ClipFileUploadKey,
        msg: str|None = None
    ) -> None:
        super().__init__()
        self.clip = clip
        self.key = key
        self.msg = msg

    def __str__(self) -> str:
        msg = '' if self.msg is None else f': {self.msg}'
        return f'{self.clip.unique_name} ({self.key}){msg}'

class NoMetaError(CheckError):
    """Raised when a file exists locally with no stored :class:`FileMeta`
    """

class ContentLengthError(CheckError):
    """Raised when the stored :attr:`~FileMeta.content_length` does not match
    the local filesize
    """

class ContentTypeError(CheckError):
    """Raised when the :attr:`~FileMeta.content_type` disagrees with the
    file extension
    """
    @overload
    @classmethod
    def check(
        cls,
        meta: FileMeta,
        filename: PathLike,
        return_mtypes: Literal[False]
    ) -> bool: ...
    @overload
    @classmethod
    def check(
        cls,
        meta: FileMeta,
        filename: PathLike,
        return_mtypes: Literal[True]
    ) -> tuple[bool, str|None, str|None]: ...
    @classmethod
    def check(
        cls,
        meta: FileMeta,
        filename: PathLike,
        return_mtypes: bool = False
    ) -> bool|tuple[bool, str|None, str|None]:
        mtype, _ = mimetypes.guess_type(filename)
        ext = mimetypes.guess_extension(meta.content_type)
        if mtype == meta.content_type:
            if return_mtypes:
                return True, mtype, ext
            return True
        filename = Path(filename)
        if filename.suffix == ext:
            if return_mtypes:
                return True, mtype, ext
            return True
        return False, mtype, ext

    @classmethod
    def check_and_raise(
        cls,
        clip: Clip,
        key: ClipFileUploadKey,
        meta: FileMeta,
        filename: PathLike,
    ) -> None:
        """Check the metadata against the file extension using :mod:`mimetypes`
        and raise this exception if they do not match
        """
        matched, mtype, ext = cls.check(meta, filename, return_mtypes=True)
        if matched:
            return
        filename = Path(filename)
        raise cls(
            clip,
            key,
            msg=f'{meta.content_type=}, mimetype={mtype}, {ext=}, {filename.suffix=}',
        )


class NoFileError(CheckError):
    """Raised when there is :class:`metadata <FileMeta>` for a file that
    does not exist locally
    """

class FileShouldNotExist(CheckError):
    """Raised when the stored metadata for a file is
    :attr:`zero-length <FileMeta.is_zero_length>` but the file exists locally
    """


class FilesizeMagicNumber(CheckError):
    """Raised when the filesize is exactly ``1245`` bytes

    Don't ask me why, but this happens with some pdf downloads.  It usually
    means the file can be re-downloaded because the Content-Length in the
    header shows a different value.

    (I stopped asking why their systems are the way they are a long time ago)
    """

    @classmethod
    def is_magic_number(cls, item: Path|FileMeta|int) -> bool:
        """Check if the value equals ``1245``

        This seriously feels a bit like the `is-thirteen`_ package.

        .. _is-thirteen: https://github.com/jezen/is-thirteen
        """
        if isinstance(item, FileMeta):
            content_length = item.content_length
        elif isinstance(item, Path):
            content_length = item.stat().st_size
        else:
            content_length = item
        return content_length == 1245

    @classmethod
    def check_and_raise(
        cls,
        clip: Clip,
        key: ClipFileUploadKey,
        item: Path|FileMeta|int,
        msg: str|None = None
    ) -> None:
        """Check the given *item* against :meth:`is_magic_number` and raise
        this exception if true
        """
        if not cls.is_magic_number(item):
            return
        extra_msg = 'Filesize is exactly 1245 bytes. Corruption likely'
        if msg is None:
            msg = ''
        else:
            msg = f' - "{msg}"'
        raise cls(clip, key, msg=f'{extra_msg}{msg}')


@dataclass
class ClipFiles(Serializable):
    """File information for a :class:`Clip`
    """
    clip: Clip = field(repr=False)  #: The parent :class:`Clip`
    agenda: Path|None               #: Agenda filename
    minutes: Path|None              #: Minutes filename
    audio: Path|None                #: MP3 filename
    video: Path|None                #: MP4 filename
    chapters: Path|None = None
    """WebVTT chapters filename (built by :meth:`AgendaTimestamps.build_vtt`)
    """
    agenda_packet: Path|None = None
    """Agenda packet (parsed from :class:`.legistar.detail_page.DetailPageResult`)"""
    metadata: dict[ClipFileUploadKey, FileMeta] = field(default_factory=dict)
    """:class:`FileMeta` for each file (if available)"""

    path_attrs: ClassVar[list[ClipFileKey]] = ['agenda', 'minutes', 'audio', 'video']
    all_path_attrs: ClassVar[list[ClipFileUploadKey]] = [
        'agenda', 'minutes', 'audio', 'video', 'chapters', 'agenda_packet',
    ]

    @classmethod
    def from_parse_data(cls, clip: Clip, parse_data: ParseClipData) -> Self:
        """Create an instance from a :class:`ParseClipData` instance
        """
        root_dir = clip.root_dir
        links = parse_data.original_links
        kw: dict[str, Any] = {k: v if not v else cls.build_path(root_dir, k) for k,v in links}
        return cls(clip=clip, metadata={}, **kw)

    @classmethod
    def build_path(cls, root_dir: Path, key: ClipFileUploadKey) -> Path:
        """Build the filename for the given file type with *root_dir* prepended
        """
        if key == 'chapters':
            return root_dir / 'chapters.vtt'
        elif key == 'agenda_packet':
            return root_dir / 'agenda_packet.pdf'
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
        for key, u, p in self.clip.iter_url_paths():
            if not p.exists():
                meta = self.get_metadata(key)
                if meta is not None and meta.is_zero_length:
                    continue
                return False
        return True

    def ensure_path(self, key: ClipFileUploadKey) -> None:
        """Ensure path for *key* is set on the instance

        Raises:
            ValueError: If the file does not exist
        """
        if key in self:
            return
        rel_p = self.clip.get_file_path(key, absolute=False)
        abs_p = self.clip.get_file_path(key, absolute=True)
        if not abs_p.exists():
            raise ValueError(f'File does not exist for "{key}": {abs_p}')
        self[key] = rel_p

    def get_metadata(self, key: ClipFileUploadKey) -> FileMeta|None:
        """Get the :class:`FileMeta` for the given file type (if available)
        """
        return self.metadata.get(key)

    def set_metadata(self, key: ClipFileUploadKey, meta: FileMeta|Headers) -> FileMeta:
        """Set the :class:`FileMeta` for the given file type from request headers
        """
        if not isinstance(meta, FileMeta):
            meta = FileMeta.from_headers(meta)
        self.metadata[key] = meta
        return meta

    def check_agenda_packet_file(self, meta: FileMeta|None = None) -> bool:
        if self.agenda_packet is not None:
            return False
        full_filename = self.clip.get_file_path('agenda_packet', absolute=True)
        if not full_filename.exists():
            return False
        self.agenda_packet = self.build_path(self.clip.root_dir, 'agenda_packet')
        if meta is None:
            st = full_filename.stat()
            dt = datetime.datetime.fromtimestamp(st.st_mtime).astimezone(UTC)
            meta = FileMeta(
                content_length=st.st_size,
                content_type='application/pdf',
                last_modified=dt,
                etag=None,
            )
        self.metadata['agenda_packet'] = meta
        return True

    def check_chapters_file(self) -> bool:
        """Check for an existing :attr:`chapters` file

        If :attr:`chapters` is not set and the expected filename for it exists,
        the filename and its :attr:`metadata` will be added.
        """
        if self.chapters is not None:
            return False
        full_filename = self.clip.get_file_path('chapters', absolute=True)
        if not full_filename.exists():
            return False
        self.chapters = self.build_path(self.clip.root_dir, 'chapters')
        st = full_filename.stat()
        dt = datetime.datetime.fromtimestamp(st.st_mtime).astimezone(UTC)
        meta = FileMeta(
            content_length=st.st_size,
            content_type='text/vtt',
            last_modified=dt,
            etag=None,
            sha1=get_file_hash(full_filename, 'sha1'),
        )
        self.metadata['chapters'] = meta
        return True

    def check(self):
        """Check local files against the stored :attr:`metadata`

        Raises:
            CheckError: A subclass of :class:`CheckError` if any errors are found
        """
        keys_checked: set[ClipFileUploadKey] = set()
        for key, p in self.iter_existing(for_download=False):
            full_p = self.clip.get_file_path(key, absolute=True)
            if not full_p.exists():
                continue
            st = full_p.stat()
            meta = self.get_metadata(key)
            if meta is None:
                raise NoMetaError(self.clip, key)
            if meta.is_zero_length:
                continue
            ContentTypeError.check_and_raise(self.clip, key, meta, full_p)
            if meta.content_length != st.st_size:
                raise ContentLengthError(self.clip, key)
            if meta.is_pdf:
                FilesizeMagicNumber.check_and_raise(
                    self.clip, key, st.st_size, msg='filesize',
                )
                FilesizeMagicNumber.check_and_raise(
                    self.clip, key, meta, msg='local meta',
                )
            keys_checked.add(key)
        for key in self.metadata.keys():
            if key in keys_checked:
                continue
            meta = self.metadata[key]
            full_p = self.clip.get_file_path(key, absolute=True)
            if meta.is_zero_length:
                if full_p.exists():
                    raise FileShouldNotExist(self.clip, key)
                continue
            if not full_p.exists():
                raise NoFileError(self.clip, key)
            ContentTypeError.check_and_raise(self.clip, key, meta, full_p)
            st = full_p.stat()
            if st.st_size != meta.content_length:
                raise ContentLengthError(self.clip, key)
            if meta.is_pdf:
                FilesizeMagicNumber.check_and_raise(
                    self.clip, key, full_p, msg='filesize',
                )
                FilesizeMagicNumber.check_and_raise(
                    self.clip, key, meta, msg='local meta',
                )

    def ensure_local_hashes(self, check_existing: bool = False) -> bool:
        """Ensure that all local files have an :attr:`~FileMeta.sha1` hash
        stored in :attr:`metadata`

        Arguments:
            check_existing: If ``True``, the hash of the local file will be
                checked against the stored hash

        Returns:
            ``True`` if any hashes were generated or updated
        """
        changed = False
        for key, p in self.iter_existing(for_download=False):
            meta = self.get_metadata(key)
            assert meta is not None
            full_p = self.clip.get_file_path(key, absolute=True)
            if not full_p.exists():
                continue
            if meta.sha1 is None:
                sha1 = get_file_hash(full_p, 'sha1')
                meta.sha1 = sha1
                changed = True
            elif check_existing:
                sha1 = get_file_hash(full_p, 'sha1')
                if sha1 != meta.sha1:
                    raise HashMismatchError(f'{full_p=}')
        return changed

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

    def __getitem__(self, key: ClipFileUploadKey) -> Path|None:
        assert key in self.all_path_attrs
        return getattr(self, key)

    def __setitem__(self, key: ClipFileUploadKey, value: Path) -> None:
        assert key in self.all_path_attrs
        assert self[key] is None
        setattr(self, key, value)

    def __contains__(self, key: ClipFileUploadKey):
        return self[key] is not None

    def __iter__(self) -> Iterator[tuple[ClipFileKey, Path|None]]:
        for attr in self.path_attrs:
            yield attr, self[attr]

    @overload
    def iter_existing(
        self,
        for_download: bool = True
    ) -> Iterator[tuple[ClipFileKey, Path]]: ...
    @overload
    def iter_existing(
        self,
        for_download: bool = False
    ) -> Iterator[tuple[ClipFileUploadKey, Path]]: ...
    def iter_existing(
        self,
        for_download: bool = True
    ) -> Iterator[tuple[ClipFileKey|ClipFileUploadKey, Path]]:
        """Iterate over existing filenames

        Arguments:
            for_download: If ``True`` (the default), only the filenames expected
                to be on the Granicus server will be yielded.  If ``False``,
                locally-generated files will be included (such as :attr:`chapters`).

        Yields:
            :
                - **key**:  The file key as :obj:`ClipFileKey` (or :obj:`ClipFileUploadKey`
                    if *for_download* it True)
                - **filename**: The relative :class:`~pathlib.Path` for the file

        """
        for attr, p in self:
            if p is not None:
                yield attr, p
        if not for_download:
            if self.chapters is not None:
                yield 'chapters', self.chapters
            if self.agenda_packet is not None:
                yield 'agenda_packet', self.agenda_packet

    def serialize(self) -> dict[str, Any]:
        d = {}
        for attr in self.path_attrs:
            val = self[attr]
            if val is not None:
                val = str(val)
            d[attr] = val
        d['chapters'] = str(self.chapters) if self.chapters else None
        d['agenda_packet'] = str(self.agenda_packet) if self.agenda_packet else None
        d['metadata'] = {mkey: mval.serialize() for mkey, mval in self.metadata.items()}
        return d

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        kw = {}
        for key, val in data.items():
            if val is not None and key != 'clip':
                if key == 'metadata':
                    val = {mkey: FileMeta.deserialize(mval) for mkey, mval in val.items()}
                else:
                    val = Path(val)
            kw[key] = val
        return cls(**kw)


@dataclass
class AgendaTimestamp(Serializable):
    """A timestamped agenda item
    """
    seconds: int    #: The timestamp in seconds
    text: str       #: Agenda item text

    @property
    def time_str(self) -> str:
        """:attr:`seconds` formatted as ``HH:MM:SS``
        """
        return seconds_to_time_str(self.seconds)

    def serialize(self) -> dict[str, Any]:
        return {'seconds': self.seconds, 'text': self.text}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        return cls(**data)


@dataclass
class AgendaTimestamps(Serializable):
    """Collection of :class:`AgendaTimestamp` for a :class:`Clip`
    """
    clip_id: CLIP_ID                #: The associated clip's :attr:`~Clip.id`
    items: list[AgendaTimestamp]    #: Timestamps for the clip

    def build_vtt(self, clip: Clip) -> str:
        """Generate WebVTT-formatted chapters with timestamp/text data

        Arguments:
            clip: The associated :class:`Clip` instance. This is needed for
                the end timestamp of last cue in the VTT track.

        """
        def iter_pairs() -> Iterator[tuple[int, AgendaTimestamp, AgendaTimestamp|None]]:
            if not len(self.items):
                raise IndexError('Item list is empty')
            for i, item in enumerate(self.items):
                try:
                    next_item = self.items[i+1]
                except IndexError:
                    next_item = None
                yield i, item, next_item

        assert clip.id == self.clip_id
        lines: list[str] = ['WEBVTT', '']
        for i, cur_item, next_item in iter_pairs():
            cur_ts = cur_item.time_str
            if next_item is not None:
                next_ts = next_item.time_str
            else:
                next_ts = seconds_to_time_str(int(clip.duration.total_seconds()))
            cue_timing = f'{cur_ts}.000 --> {next_ts}.000'
            lines.extend([
                f'{i+1}',
                cue_timing,
                cur_item.text,
                '',
            ])
        return '\n'.join(lines)

    def __iter__(self) -> Iterator[AgendaTimestamp]:
        yield from self.items

    def __len__(self):
        return len(self.items)

    def serialize(self) -> dict[str, Any]:
        items = [item.serialize() for item in self.items]
        return {'clip_id': self.clip_id, 'items': items}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        items = [AgendaTimestamp.deserialize(item) for item in data['items']]
        return cls(clip_id=data['clip_id'], items=items)


@dataclass
class AgendaTimestampCollection(Serializable):
    """Container for :class:`AgendaTimestamps`
    """

    clips: dict[CLIP_ID, AgendaTimestamps] = field(default_factory=dict)
    """
    """

    @classmethod
    def load(cls, filename: PathLike) -> Self:
        """Loads an instance from previously saved data
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = yaml_load(filename.read_text(), Loader=YamlLoader)
        return cls.deserialize(data)

    def save(self, filename: PathLike, indent: int|None = 2) -> None:
        """Saves all data as JSON to the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = self.serialize()
        s = yaml_dump(data, Dumper=YamlDumper)
        filename.write_text(s)

    def add(self, item: AgendaTimestamps) -> None:
        """Add an :class:`AgendaTimestamps` instance
        """
        assert item.clip_id not in self
        self.clips[item.clip_id] = item

    def get(self, key: CLIP_ID|Clip) -> AgendaTimestamps|None:
        """Get an :class:`AgendaTimestamps` object if it exists

        The *key* can be a :class:`Clip` instance or the clip's :attr:`~Clip.id`
        """
        if isinstance(key, Clip):
            key = key.id
        return self.clips.get(key)

    def __getitem__(self, key: CLIP_ID|Clip) -> AgendaTimestamps:
        if isinstance(key, Clip):
            key = key.id
        return self.clips[key]

    def __contains__(self, key: CLIP_ID|Clip):
        if isinstance(key, Clip):
            key = key.id
        return key in self.clips

    def __len__(self):
        return len(self.clips)

    def __iter__(self):
        yield from self.clips.values()

    def serialize(self) -> dict[str, Any]:
        clips = {key: val.serialize() for key, val in self.clips.items()}
        return {'clips': clips}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        clips = {
            key: AgendaTimestamps.deserialize(val)
            for key, val in data['clips'].items()
        }
        return cls(clips=clips)


@dataclass
class Clip(Serializable):
    """Stores all information for a single clip
    """
    parse_data: ParseClipData = field(repr=False)

    root_dir: Path
    """Path for the clip (relative to its :attr:`parent`)"""

    files: ClipFiles = field(init=False)
    """The clip's file information"""

    parent: ClipCollection
    """The parent :class:`ClipCollection`"""

    _chapters_filename: ClassVar[str] = 'chapters.vtt'

    @property
    def id(self) -> CLIP_ID:
        """Alias for :attr:`ParseClipData.id`"""
        return self.parse_data.id

    @property
    def name(self) -> str:
        """Alias for :attr:`ParseClipData.name`"""
        return self.parse_data.name

    @property
    def unique_name(self) -> str:
        """Alias for :attr:`ParseClipData.unique_name`"""
        return self.parse_data.unique_name

    @property
    def location(self) -> Location:
        """Alias for :attr:`ParseClipData.location`"""
        return self.parse_data.location

    @property
    def datetime(self) -> datetime.datetime:
        return self.parse_data.datetime

    @property
    def duration(self):
        return datetime.timedelta(seconds=self.parse_data.duration)

    @property
    def complete(self) -> bool: return self.files.complete

    @property
    def root_dir_abs(self) -> Path:
        """The :attr:`root_dir` with its parent prepended
        """
        return self.parent.base_dir / self.root_dir

    def get_file_path(self, key: ClipFileUploadKey, absolute: bool = False) -> Path:
        """Get the relative or absolute path for the given file type
        """
        root_dir = self.root_dir_abs if absolute else self.root_dir
        return self.files.build_path(root_dir, key)

    @classmethod
    def from_parse_data(cls, parent: ClipCollection, parse_data: ParseClipData) -> Self:
        """Create an instance from a :class:`ParseClipData` instance
        """
        root_dir = parse_data.build_fs_dir(None)
        obj = cls(
            parse_data=parse_data,
            root_dir=root_dir,
            parent=parent,
        )
        obj.files = ClipFiles.from_parse_data(
            clip=obj,
            parse_data=parse_data,
        )
        return obj

    def iter_url_paths(
        self,
        actual: bool = True,
        absolute: bool = True
    ) -> Iterator[tuple[ClipFileKey, URL, Path]]:
        """Iterate over the clip's file types, url's and filenames

        Arguments:
            actual: If True, only yields file types with valid URL's
            absolute: If True, the :attr:`root_dir_abs` is prepended to each
                filename

        """
        if actual:
            assert self.parse_data.actual_links is not None
            it = self.parse_data.actual_links.iter_existing()
        else:
            it = self.parse_data.original_links.iter_existing()
        root_dir = self.root_dir_abs if absolute else self.root_dir
        for urlkey, url in it:
            filename = self.files.build_path(root_dir, urlkey)
            yield urlkey, url, filename

    @overload
    def iter_paths(
        self,
        absolute: bool = True,
        for_download: bool = False
    ) -> Iterator[tuple[ClipFileUploadKey, Path]]: ...
    @overload
    def iter_paths(
        self,
        absolute: bool = True,
        for_download: bool = True
    ) -> Iterator[tuple[ClipFileKey, Path]]: ...
    def iter_paths(
        self,
        absolute: bool = True,
        for_download: bool = True
    ) -> Iterator[tuple[ClipFileKey|ClipFileUploadKey, Path]]:
        """Iterate over paths in :attr:`files`

        Arguments:
            absolute: Whether to yield absolute paths
            for_download: If ``True`` (the default), only the filenames expected
                to be on the Granicus server will be yielded.  If ``False``,
                locally-generated files will be included (such as
                :attr:`ClipFiles.chapters`).

        Yields:
            :
                - **key**:  The file key as :obj:`ClipFileKey` (or :obj:`ClipFileUploadKey`
                    if *for_download* it True)
                - **filename**: The :class:`Path` for the file relative to the
                    :attr:`root_dir` (or :attr:`root_dir_abs` if *absolute* is True).

        """
        root_dir = self.root_dir_abs if absolute else self.root_dir
        for key, p in self.files.iter_existing(for_download=for_download):
            filename = self.files.build_path(root_dir, key)
            yield key, filename

    def relative_to(self, base_dir: Path) -> Self:
        new_root_dir = self.parse_data.build_fs_dir(base_dir)
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
        obj = cls(
            parse_data=ParseClipData.deserialize(data['parse_data']),
            root_dir=Path(data['root_dir']),
            parent=data['parent'],
        )
        file_data = data['files'].copy()
        file_data['clip'] = obj
        obj.files = ClipFiles.deserialize(file_data)
        return obj

ClipDict = dict[CLIP_ID, Clip]

@dataclass
class ClipCollection(Serializable):
    """Container for :attr:`Clips <Clip>`
    """
    base_dir: Path
    """Root filesystem path for the clip assets"""

    clips: ClipDict = field(default_factory=dict)
    clips_by_dt: dict[datetime.datetime, list[CLIP_ID]] = field(init=False)

    def __post_init__(self):
        self.clips_by_dt = self._sort_clips_by_dt()

    def _sort_clips_by_dt(self) -> dict[datetime.datetime, list[CLIP_ID]]:
        result: dict[datetime.datetime, list[CLIP_ID]] = {}
        keys = [int(key) for key in self.clips.keys()]
        for int_key in sorted(keys):
            key = CLIP_ID(str(int_key))
            clip = self.clips[key]
            l = result.setdefault(clip.datetime, [])
            l.append(clip.id)
        return result

    @classmethod
    def load(cls, filename: PathLike) -> Self:
        """Loads an instance from previously saved data
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = json.loads(filename.read_text())
        obj = cls.deserialize(data)
        for clip in obj:
            clip.files.check_chapters_file()
        return obj

    def save(self, filename: PathLike, indent: int|None = 2) -> None:
        """Saves all clip data as JSON to the given filename
        """
        if not isinstance(filename, Path):
            filename = Path(filename)
        data = self.serialize()
        filename.write_text(json.dumps(data, indent=indent))

    def add_clip(self, parse_data: ParseClipData) -> Clip:
        """Parse a :class:`Clip` from the :class:`ParseClipData` and
        add it to the collection
        """
        clip = Clip.from_parse_data(parent=self, parse_data=parse_data)
        if clip.id in self.clips:
            raise KeyError(f'Clip with id "{clip.id}" exists')
        self.clips[clip.id] = clip
        l = self.clips_by_dt.setdefault(clip.datetime, [])
        l.append(clip.id)
        return clip

    def __getitem__(self, key: CLIP_ID) -> Clip:
        return self.clips[key]

    def __contains__(self, key):
        return key in self.clips

    def __len__(self):
        return len(self.clips)

    def __iter__(self):
        yield from self.iter_sorted()

    def iter_sorted(self, reverse: bool = True) -> Iterator[Clip]:
        dt_keys = sorted(self.clips_by_dt.keys())
        if reverse:
            dt_keys = reversed(dt_keys)
        for dt in dt_keys:
            clip_ids = self.clips_by_dt[dt]
            if reverse:
                clip_ids = reversed(clip_ids)
            for clip_id in clip_ids:
                yield self[clip_id]

    def relative_to(self, base_dir: Path) -> Self:
        clips = {k:v.relative_to(base_dir) for k,v in self.clips.items()}
        return dataclasses.replace(
            self,
            base_dir=self.base_dir.relative_to(base_dir), clips=clips,
        )

    def merge(self, other: ClipCollection) -> ClipCollection:
        """Merge the clips in this instance with another
        """
        def dt_diff(a: datetime.datetime, b: datetime.datetime) -> datetime.timedelta:
            if a > b:
                return a - b
            return b - a

        def dt_in_range(a: datetime.datetime, b: datetime.datetime) -> bool:
            return dt_diff(a, b) <= datetime.timedelta(minutes=1)

        self_keys = set(self.clips.keys())
        oth_keys = set(other.clips.keys())
        missing_in_self = oth_keys - self_keys      # I Don't Have
        missing_in_other = self_keys - oth_keys     # I Have, Other Doesn't
        all_keys = self_keys | oth_keys
        key_order = list(self.clips.keys())
        key_order.extend(missing_in_self)
        assert len(key_order) == len(set(key_order))
        all_clips: dict[CLIP_ID, Clip] = {}
        for key in key_order:
            if key in missing_in_other:
                assert key not in other
                all_clips[key] = self[key]
                continue
            elif key in missing_in_self:
                assert key not in self
                all_clips[key] = other[key]
                continue
            assert key in all_keys
            self_clip = self[key]
            oth_clip = other[key]
            c = self_clip
            if self_clip != oth_clip:
                assert dt_in_range(self_clip.datetime, oth_clip.datetime)
                self_p, oth_p = self_clip.parse_data, oth_clip.parse_data
                self_p.check(oth_p)
                oth_p.check(self_p)
                if self_p.actual_links is None and oth_p.actual_links is not None:
                    c = oth_clip
            oth_clip.parse_data.update(self_clip.parse_data)
            all_clips[key] = c
        assert set(all_clips.keys()) == self_keys | oth_keys
        return dataclasses.replace(self, clips=all_clips)

    def serialize(self) -> dict[str, Any]:
        clips = {k: v.serialize() for k,v in self.clips.items()}
        return dict(base_dir=str(self.base_dir), clips=clips)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        obj = cls(base_dir=Path(data['base_dir']))
        for key, val in data['clips'].items():
            val = val.copy()
            val['parent'] = obj
            clip = Clip.deserialize(val)
            assert clip.id == key
            obj.clips[key] = clip
        obj.clips_by_dt = obj._sort_clips_by_dt()
        return obj

@dataclass
class ClipIndex(Serializable):
    """Model for only essential :class:`Clip` data to be included in
    :class:`ClipsIndex`
    """
    id: CLIP_ID                 #: :attr:`Clip.id`
    location: Location          #: :attr:`Clip.location`
    name: str                   #: :attr:`Clip.name`
    datetime: datetime.datetime #: :attr:`Clip.datetime`
    data_file: Path
    """Path to the full :class:`Clip` data file within its :attr:`~Clip.root_dir`
    """

    dt_fmt: ClassVar[str] = '%a, %d %b %Y %H:%M:%S GMT'

    @classmethod
    def from_clip(cls, clip: Clip, root_dir: Path) -> Self:
        """Create an instance from a :class:`Clip`

        Arguments:
            clip: The :class:`Clip` instance
            root_dir: Relative parent directory of the :attr:`Clip.root_dir`.
                This will typically be the :attr:`ClipsIndex.root_dir`
        """
        assert not root_dir.is_absolute()
        return cls(
            id=clip.id,
            location=clip.location,
            name=clip.name,
            datetime=clip.datetime,
            data_file=cls._get_data_filename(clip, root_dir),
        )

    @classmethod
    def _get_data_filename(cls, clip: Clip, root_dir: Path) -> Path:
        return root_dir / clip.root_dir / 'data.json'

    def write_data(
        self,
        clip: Clip|ClipCollection,
        exist_ok: bool = False,
        indent: int|None = 2
    ) -> None:
        """Serialize the clip data and save it to :attr:`data_file`

        Arguments:
            clip: A :class:`ClipCollection` or :class:`Clip` instance
            exist_ok: If ``False`` and the :attr:`data_file` exists,
                it will not be overwritten and an exception will be raised
            indent: Indentation parameter to pass to :func:`json.dumps`

        """
        if not self.data_file.parent.exists():
            return
        if not exist_ok and self.data_file.exists():
            raise Exception(f'Data file exists: {self.data_file}')
        logger.debug(f'writing data to {self.data_file}')
        if isinstance(clip, ClipCollection):
            clip = clip[self.id]
        root_dir = self.data_file.parent
        clip_files = {key: str(root_dir / f.name) for key, f in clip.iter_paths(for_download=False)}
        clip_data = clip.serialize()
        clip_data['files'].update(clip_files)
        self.data_file.write_text(json.dumps(clip_data, indent=indent))

    def serialize(self) -> dict[str, Any]:
        return dict(
            id=self.id,
            location=self.location,
            name=self.name,
            datetime=self.datetime.strftime(self.dt_fmt),
            data_file=str(self.data_file)
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        # kw = data.copy()
        return cls(
            id=data['id'],
            location=data['location'],
            name=data['name'],
            datetime=datetime.datetime.strptime(data['datetime'], cls.dt_fmt),
            data_file=Path(data['data_file']),
        )


@dataclass
class ClipsIndex(Serializable):
    """An index of all clips containing a minimal amount of data

    When serialized, this data will contain only basic clip information
    with relative paths to each clip's full data representation.

    This is intended for web services to use in order to avoid fetching a
    large amount of unnecessary data.
    """
    clips: dict[CLIP_ID, ClipIndex]
    """Mapping of :class:`ClipIndex` using the :attr:`ClipIndex.id` as keys
    """

    root_dir: Path
    """Relative parent directory of the :attr:`ClipCollection.base_dir`
    """

    @classmethod
    def from_clip_collection(cls, clips: ClipCollection, root_dir: PathLike) -> Self:
        """Create an instance from a :class:`ClipCollection`
        """
        if not isinstance(root_dir, Path):
            root_dir = Path(root_dir)
        assert not root_dir.is_absolute()
        root_rel = clips.base_dir.relative_to(root_dir)
        d = {clip.id: ClipIndex.from_clip(clip, root_rel) for clip in clips}
        return cls(clips=d, root_dir=root_rel)

    def write_data(
        self,
        clip_collection: ClipCollection|None = None,
        exist_ok: bool = False,
        indent: int|None = 2
    ) -> None:
        """Write the root index data and each :class:`clip's <ClipIndex>` full
        data

        The root index will be stored as "clip-index.json" within the
        :attr:`root_dir`.  All items in :attr:`clips` will be flattened as
        a :class:`list` of the serialized form of :class:`ClipIndex`.

        Arguments:
            clip_collection: If provided, the :meth:`~ClipIndex.write_data`
                method will be called on each item in :attr:`clips` using the
                clip collection's data.  If ``None``, only the root index data
                will be saved.
            exist_ok: If ``False`` and the data file exists, it will not be
                overwritten and an exception will be raised.  This will also
                be passed when calling :meth:`ClipIndex.write_data`.
            indent: Indentation parameter to pass to :func:`json.dumps`

        """
        if clip_collection is not None:
            for clip in self.clips.values():
                clip.write_data(clip_collection, exist_ok=exist_ok, indent=indent)

        filename = self.root_dir / 'clip-index.json'
        if not exist_ok and filename.exists():
            raise Exception(f'Data file exists: {filename}')
        data = self.serialize()
        filename.write_text(json.dumps(data, indent=indent))

    def serialize(self) -> dict[str, Any]:
        return dict(
            clips=[clip.serialize() for clip in self.clips.values()],
            root_dir=str(self.root_dir),
        )

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        clip_list = [ClipIndex.deserialize(d) for d in data['clips']]
        clips = {clip.id: clip for clip in clip_list}
        return cls(
            clips=clips,
            root_dir=Path(data['root_dir']),
        )

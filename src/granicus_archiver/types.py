from __future__ import annotations
from typing import ClassVar, Any, Self
from abc import ABC, abstractmethod

import dataclasses
from dataclasses import dataclass
import datetime

from multidict import MultiMapping

from .utils import SHA1Hash

Headers = MultiMapping[str]|dict[str, str]
UTC = datetime.timezone.utc


class Serializable(ABC):

    @abstractmethod
    def serialize(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError


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

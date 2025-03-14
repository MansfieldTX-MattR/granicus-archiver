from __future__ import annotations
from typing import (
    NewType, TypeVar, Generic, TypedDict, NotRequired,
    AsyncGenerator, Iterable,
)
from pathlib import Path

from ..utils import SHA1Hash, SHA256Hash, MD5Hash
from aiogoogle.resource import GoogleAPI, Resource
from aiogoogle.models import Request

__all__ = (
    'FileId', 'DriveFileMeta', 'DriveFileMetaFull', 'FileUploadResponse', 'FileListResponse',
    'DriveResource', 'DriveFiles', 'FolderCache',
)

T = TypeVar('T')

FileId = NewType('FileId', str)
"""A unique id for a file or directory in Drive"""

FolderCache = dict[Path, FileId]
"""Mapping of paths to :obj:`FileId` to aid in caching Drive folders"""


class DriveFileMeta(TypedDict):
    """Metadata for a Drive file or folder
    """
    name: str                           #: The file or folder name
    id: NotRequired[FileId]             #: The item id (not required for uploads)
    mimeType: NotRequired[str]          #: Content type
    parents: NotRequired[list[FileId]]  #: Parent folder(s)
    size: NotRequired[str]              #: Size in bytes
    webViewLink: NotRequired[str]       #: Sharable link to the item
    webContentLink: NotRequired[str]    #: Download link


class DriveFileMetaFull(TypedDict):
    """Similar to :class:`DriveFileMeta` but with all fields required
    """
    name: str                           #: The file or folder name
    id: FileId                          #: The item id
    mimeType: str                       #: Content type
    parents: list[FileId]               #: Parent folder(s)
    size: str                           #: Size in bytes
    webViewLink: str                    #: Sharable link to the item
    webContentLink: str                 #: Download link
    md5Checksum: MD5Hash                #: MD5 Checksum
    sha1Checksum: SHA1Hash              #: SHA1 Checksum
    sha256Checksum: SHA256Hash          #: SHA256 Checksum


class FileUploadResponse(TypedDict):
    """Response type for a Drive upload
    """
    id: FileId                          #: The file id

class FilePageResponse(TypedDict, Generic[T]):
    """A single result during pagination in :class:`FileListResponse`
    """
    files: Iterable[T]
    """Iterable of either :class:`DriveFileMeta` or :class:`DriveFileMetaFull` objects
    """


FileListResponse = AsyncGenerator[FilePageResponse[T], None]
"""Response type for file lists as an :term:`asynchronous generator` yielding
:class:`FilePageResponse` objects
"""


class DriveResource(GoogleAPI):
    """Overload for :class:`aiogoogle.resource.GoogleAPI` as a Drive resource

    This wraps the response of :meth:`aiogoogle.resource.GoogleAPI.discover`
    with ``api_name`` and ``api_version`` arguments set to ``("drive", "v3")``
    """
    files: DriveFiles
    """A :class:`DriveFiles` resource
    """

class DriveFiles(Resource):
    """Resource for Drive files
    """
    def list(self, **kwargs) -> Request: ...

    def create(self, **kwargs) -> Request: ...

    def update(self, **kwargs) -> Request: ...

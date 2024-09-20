from __future__ import annotations
from typing import (
    NewType, TypeVar, Generic, TypedDict, NotRequired,
    AsyncGenerator, Iterable,
)

from aiogoogle.client import Aiogoogle
from aiogoogle.resource import GoogleAPI, Resource
from aiogoogle.models import Request

__all__ = (
    'FileId', 'FileMeta', 'FileMetaFull', 'FileUploadResponse', 'FileListResponse',
    'DriveResource', 'DriveFiles',
)

T = TypeVar('T')

FileId = NewType('FileId', str)

class FileMeta(TypedDict):
    name: str
    id: NotRequired[FileId]
    mimeType: NotRequired[str]
    parents: NotRequired[list[FileId]]
    size: NotRequired[str]
    webViewLink: NotRequired[str]
    webContentLink: NotRequired[str]

class FileMetaFull(TypedDict):
    name: str
    id: FileId
    mimeType: str
    parents: list[FileId]
    size: str
    webViewLink: str
    webContentLink: str

class FileUploadResponse(TypedDict):
    id: FileId

class FilePageResponse(TypedDict, Generic[T]):
    files: Iterable[T]

FileListResponse = AsyncGenerator[FilePageResponse[T], None]

class DriveResource(GoogleAPI):
    files: DriveFiles

class DriveFiles(Resource):
    def list(self, **kwargs) -> Request: ...

    def create(self, **kwargs) -> Request: ...

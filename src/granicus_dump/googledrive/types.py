from __future__ import annotations
from typing import TypedDict, NotRequired

from aiogoogle.client import Aiogoogle
from aiogoogle.resource import GoogleAPI, Resource
from aiogoogle.models import Request

FileId = str

class FileMeta(TypedDict):
    name: str
    id: NotRequired[str]
    mimeType: NotRequired[str]
    parents: NotRequired[list[FileId]]
    size: NotRequired[str]

class DriveResource(GoogleAPI):
    files: DriveFiles

class DriveFiles(Resource):
    def list(self, **kwargs) -> Request: ...

    def create(self, **kwargs) -> Request: ...

from __future__ import annotations
from typing import Coroutine, Any, TypedDict
from pathlib import Path
import asyncio
import json

from loguru import logger
from aiohttp import web

from ..aws.client import ClientBase
from ..legistar.guid_model import RGuidLegistarData
from ..utils import SHA1Hash
from .config import APP_CONF_KEY
from .types import ConfigKey, DataFileType, DataFiles


class DataFileMetadata(TypedDict):
    """Metadata for a data file
    """
    e_tag: str
    """The ETag of the file"""
    sha1: SHA1Hash
    """The SHA1 hash of the file"""


class S3Client(ClientBase):
    """S3 client for downloading data files and assets
    """
    data_dirs: DataFiles
    """Remote data directories"""
    data_files_local: DataFiles
    """Local data files"""
    data_files_remote: DataFiles
    """Remote data files"""
    metadata_file: Path
    """Filename to store cahced metadata"""
    data_file_metadata: dict[DataFileType, DataFileMetadata|None]
    """Cached metadata for data files"""
    def __init__(self, app: web.Application) -> None:
        # import app key locally because app.cleanup_ctx has issues otherwise
        self.app = app
        self.app_conf = app[APP_CONF_KEY]
        config = app[ConfigKey]
        super().__init__(config)

        data_dirs: DataFiles = {
            'clips': config.aws.clips_prefix,
            'legistar': config.aws.legistar_prefix,
            'legistar_rguid': config.aws.legistar_rguid_prefix,
        }
        s3_data_dir = self.app_conf.s3_data_dir
        assert s3_data_dir is not None
        data_files_local: DataFiles = {
            'clips': s3_data_dir / config.data_file,
            'legistar': s3_data_dir / config.legistar.data_file,
            'legistar_rguid': s3_data_dir / RGuidLegistarData._get_data_file(config),
        }
        data_files_remote: DataFiles = {
            k: data_dirs[k] / data_files_local[k].name
            for k in data_files_local
        }
        self.search_dir_local = s3_data_dir / '_search-index'
        self.search_dir_remote = config.aws.legistar_prefix / '_search-index'
        self.search_dir_local.mkdir(parents=True, exist_ok=True)
        self.data_dirs = data_dirs
        self.data_files_local = data_files_local
        self.data_files_remote = data_files_remote
        self.metadata_file = s3_data_dir / 's3metadata.json'
        self.data_file_metadata = self.load_data_file_metadata()

    async def get_search_index_dir(self) -> None:
        """Download the search index directory

        Existing files will be overwritten.
        """
        index_dir = self.search_dir_remote
        objs = [obj async for obj in self.iter_objects(index_dir) if not obj.key.endswith('/')]
        for obj in objs:
            local_file = self.search_dir_local / obj.key.rsplit('/', 1)[-1]
            logger.debug(f'Found search index file "{obj.key}"')
            local_file.parent.mkdir(parents=True, exist_ok=True)
            await self.download_object(obj.key, local_file)
            logger.info(f'Downloaded search index file "{obj.key}" to "{local_file}"')

    async def get_data_files(self) -> bool:
        """Download data files if they have changed remotely
        """
        coros = set[Coroutine[Any, Any, bool]]()
        for key in self.data_files_local.keys():
            coros.add(self.download_data_file(key))

        if len(coros):
            r = await asyncio.gather(*coros)
            changed = any(r)
            if changed:
                self.save_data_file_metadata()
            return changed
        return False

    async def download_data_file(
        self,
        key: DataFileType,
        remote_metadata: DataFileMetadata|None = None
    ) -> bool:
        """Download a data file if it has changed remotely
        """
        local_file = self.data_files_local[key]
        remote_file = self.data_files_remote[key]
        if remote_metadata is None:
            remote_metadata = await self.get_data_file_remote_meta(key)
        cached_metadata = self.data_file_metadata[key]
        if cached_metadata is not None and cached_metadata == remote_metadata:
            assert local_file.exists()
            logger.debug(f'Data for "{key}" is up to date')
            return False
        local_file.parent.mkdir(parents=True, exist_ok=True)
        await self.download_object(remote_file, local_file)
        logger.info(f'Downloaded data for "{key}" to "{local_file}"')
        self.data_file_metadata[key] = remote_metadata
        return True

    async def get_data_file_remote_meta(self, key: DataFileType) -> DataFileMetadata:
        """Get the remote metadata for a data file
        """
        remote_file = self.data_files_remote[key]
        obj = await self.get_object(remote_file)
        e_tag = await obj.e_tag
        sha1 = await self.get_object_sha1(remote_file)
        assert sha1 is not None
        return {
            'e_tag': e_tag,
            'sha1': sha1,
        }

    def load_data_file_metadata(self) -> dict[DataFileType, DataFileMetadata|None]:
        """Load the data file metadata from disk
        """
        if self.metadata_file.exists():
            return json.loads(self.metadata_file.read_text())
        return {k: None for k in self.data_files_local}

    def save_data_file_metadata(self) -> None:
        """Save the data file metadata to disk
        """
        self.metadata_file.write_text(json.dumps(self.data_file_metadata))


S3ClientKey = web.AppKey('S3Client', S3Client)
"""App key for the :class:`.s3client.S3Client` instance"""

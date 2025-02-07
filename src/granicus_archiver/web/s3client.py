from __future__ import annotations
from typing import Coroutine, Any
from pathlib import Path
import asyncio

from loguru import logger
from aiohttp import web

from ..aws.client import ClientBase
from ..legistar.guid_model import RGuidLegistarData
from ..utils import get_file_hash
from .config import APP_CONF_KEY
from .types import *


class S3Client(ClientBase):
    """S3 client for downloading data files and assets
    """
    data_dirs: DataFiles
    """Remote data directories"""
    data_files_local: DataFiles
    """Local data files"""
    data_files_remote: DataFiles
    """Remote data files"""
    def __init__(self, app: web.Application) -> None:
        # import app key locally because app.cleanup_ctx has issues otherwise
        from .types import ConfigKey
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
        self.data_dirs = data_dirs
        self.data_files_local = data_files_local
        self.data_files_remote = data_files_remote

    async def get_data_files(self) -> bool:
        """Download data files if they have changed remotely
        """
        async def do_download(
            key: DataFileType,
            local_file: Path,
            remote_file: Path
        ) -> bool:
            if local_file.exists():
                local_hash = get_file_hash(local_file, 'sha1')
                remote_hash = await self.get_object_sha1(remote_file)
                assert remote_hash is not None
                if local_hash == remote_hash:
                    logger.debug(f'Data for "{key}" is up to date')
                    return False
            local_file.parent.mkdir(parents=True, exist_ok=True)
            await self.download_object(remote_file, local_file)
            logger.info(f'Downloaded data for "{key}" to "{local_file}"')
            return True

        coros = set[Coroutine[Any, Any, bool]]()
        for key, local_file in self.data_files_local.items():
            remote_file = self.data_files_remote[key]
            coros.add(do_download(key, local_file, remote_file))

        if len(coros):
            r = await asyncio.gather(*coros)
            return any(r)
        return False

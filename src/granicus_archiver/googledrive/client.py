from __future__ import annotations
from typing import (
    TypeVar, Generic, Self, Coroutine, TypedDict, Literal, Any, AsyncGenerator,
    overload, cast, TYPE_CHECKING
)
from abc import ABC, abstractmethod
import mimetypes
from pathlib import Path
import asyncio
import json

from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError as GoogleHTTPError
from loguru import logger
import aiojobs
import aiofile

from .types import *
from .cache import FileCache, MetaCacheKey, MetaCount
from .pathtree import PathNode
from . import config
from ..types import FileMeta as ClipFileMeta
from ..clips.model import (
    ClipCollection, Clip, ClipFileKey, ClipFileUploadKey, CLIP_ID,
)
from ..legistar.types import GUID, REAL_GUID, LegistarFileUID, _GuidT, _ItemT
from ..legistar.model import AbstractLegistarModel, LegistarData, DetailPageResult
from ..legistar.guid_model import RGuidLegistarData, RGuidDetailResult
from ..utils import (
    JobWaiters,
    CompletionCounts,
    get_file_hash_async,
    SHA1Hash,
    HashMismatchError,
    aio_read_iter,
)
from ..clips import html_builder

if TYPE_CHECKING:
    from ..config import Config

_Rt = TypeVar('_Rt')

FOLDER_MTYPE = 'application/vnd.google-apps.folder'

UPLOAD_CHECK_LIMIT = 4

class UploadError(Exception):
    """Exception raised for errors during upload
    """


class RateLimitError(GoogleHTTPError):
    """Exception raised when a rate limit is encountered
    """

class SchedulersTD(TypedDict):
    general: aiojobs.Scheduler
    clip_uploads: aiojobs.Scheduler
    uploads: aiojobs.Scheduler
    upload_checks: aiojobs.Scheduler




def get_client(root_conf: Config) -> Aiogoogle:
    client_creds = config.OAuthClientConf.load_from_env()._asdict()
    user_creds = config.UserCredentials.load(root_conf=root_conf)._asdict()
    return Aiogoogle(user_creds=user_creds, client_creds=client_creds)   # type: ignore


class GoogleClient:
    """Thin wrapper for :class:`aiogoogle.client.Aiogoogle` to simplify
    operations
    """
    aiogoogle: Aiogoogle        #: The actual google client
    drive_v3: DriveResource     #: Drive resource

    folder_cache: FolderCache
    """A cache of Drive folders and their id
    """

    meta_cache: FileCache
    """Cache for uploaded file metadata
    """

    def __init__(self, root_conf: Config) -> None:
        self.root_conf = root_conf
        self.folder_cache = {}
        self.meta_cache = FileCache()
        self.load_cache()

    @property
    def folder_cache_file(self) -> Path:
        return self.root_conf.google.folder_cache_file

    @property
    def meta_cache_file(self) -> Path:
        return self.root_conf.google.meta_cache_file

    async def __aenter__(self) -> Self:
        self.cache_lock = asyncio.Lock()
        self.aiogoogle = get_client(root_conf=self.root_conf)
        await self.aiogoogle.__aenter__()
        drive_v3 = await self.aiogoogle.discover("drive", "v3")
        self.drive_v3 = cast(DriveResource, drive_v3)
        return self

    async def __aexit__(self, *args) -> None:
        await self.aiogoogle.__aexit__(*args)

    def escape_filename(self, filename: str|Path) -> str:
        """Escape filenames for use within quoted portions of api queries
        """
        if isinstance(filename, Path):
            filename = str(filename)
        # Prepend a forward-slash before all single quotes (enforcing an escape)
        if "'" in filename:
            filename = filename.replace("'", "\\'")
        return filename

    def load_cache(self):
        if self.folder_cache_file.exists():
            d = json.loads(self.folder_cache_file.read_text())
            d = {Path(k): v for k,v in d.items()}
            self.folder_cache.update(d)
        if self.meta_cache_file.exists():
            d = json.loads(self.meta_cache_file.read_text())
            meta_cache = FileCache.deserialize(d)
            self.meta_cache.update(meta_cache)

    def save_cache(self):
        """Save :attr:`folder_cache` and :attr:`meta_cache` to disk
        """
        self.save_folder_cache()
        self.meta_cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.meta_cache_file.write_text(
            json.dumps(self.meta_cache.serialize(), indent=2)
        )

    def save_folder_cache(self):
        d = {str(k):v for k,v in self.folder_cache.items()}
        self.folder_cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.folder_cache_file.write_text(json.dumps(d, indent=2))

    def find_folder_cache(self, folder: Path) -> tuple[list[FileId], Path]|None:
        """Search for cached Drive folders previously found by :meth:`find_folder`

        Returns the longest matching path and id (similar to :meth:`find_folder`)
        """
        assert not folder.is_absolute()
        f_ids = []
        parts = folder.parts
        p: Path|None = None
        for i in range(len(parts)):
            _p = Path('/'.join(parts[:i+1]))
            if _p not in self.folder_cache:
                break
            p = _p
            f_ids.append(self.folder_cache[p])
        if p is not None and len(f_ids):
            assert len(f_ids) == len(p.parts)
            return f_ids, p

    def cache_folder_parts(self, *parts_and_ids: tuple[str, FileId]):
        """Store Drive folder ids in the :attr:`folder_cache`

        Each argument should be a tuple of the folder name and its id
        (for each folder level)
        """
        p = None
        for part, f_id in parts_and_ids:
            if p is None:
                p = Path(part)
            else:
                p = p / part
            self.folder_cache[p] = f_id

    def cache_single_folder(self, folder: Path, f_id: FileId):
        """Store a single Drive folder id in the :attr:`folder_cache`
        """
        self.folder_cache[folder] = f_id


    def find_cached_file_meta(
        self,
        key: MetaCacheKey
    ) -> DriveFileMetaFull|None:
        """Search the cache for metadata by :attr:`.model.Clip.id` and file type
        """
        meta = self.meta_cache.get(key)
        if meta is None or 'size' not in meta:
            return None
        return meta

    def set_cached_file_meta(
        self,
        key: MetaCacheKey,
        meta: DriveFileMetaFull
    ) -> None:
        """Store metadata for the :attr:`.model.Clip.id` and file type in the cache
        """
        self.meta_cache[key] = meta

    async def as_user(self, *requests, resp_type: type[_Rt], full_res: bool = False) -> _Rt:
        """Send requests using :meth:`aiogoogle.client.Aiogoogle.as_user`
        casting their responses as *resp_type*
        """
        try:
            response = await self.aiogoogle.as_user(*requests, full_res=full_res)
        except GoogleHTTPError as exc:
            if 'rate limit' in str(exc).lower():
                msg = exc.args[0]
                raise RateLimitError(msg, res=exc.res, req=exc.req)
            raise
        return cast(_Rt, response)

    @overload
    def list_files(
        self,
        q: str,
        spaces: str = 'drive',
        fields: list[str]|str|None = ...,
        full_res: bool = True
    ) -> AsyncGenerator[DriveFileMetaFull]: ...
    @overload
    def list_files(
        self,
        q: str,
        spaces: str = 'drive',
        fields: list[str]|str|None = ...,
        full_res: bool = False
    ) -> AsyncGenerator[DriveFileMeta]: ...
    async def list_files(
        self,
        q: str,
        spaces: str = 'drive',
        fields: list[str]|str|None = None,
        full_res: bool = False
    ) -> AsyncGenerator[DriveFileMeta|DriveFileMetaFull]:
        """List files using the given query string

        The result will be an :term:`asynchronous generator` of
        :class:`~.types.DriveFileMeta` (if *full_res* is ``False``) or
        :class:`~.types.DriveFileMetaFull` (if *full_res* is ``True``).
        """
        req = self.drive_v3.files.list(q=q, spaces=spaces, fields=fields)
        if full_res:
            res = await self.as_user(
                req, resp_type=FileListResponse[DriveFileMetaFull], full_res=True,
            )
        else:
            res = await self.as_user(
                req, resp_type=FileListResponse[DriveFileMeta], full_res=False,
            )
        async for page in res:
            for f in page['files']:
                yield f

    async def find_folder(self, folder: Path) -> tuple[FileId, Path]|None:
        """Find a (possibly nested) Drive folder

        Searches each :attr:`part <pathlib.Path.parts>` of the given *folder*
        for a Drive folder with a matching name, then recursively searches for each
        sub-folder.

        Returns the longest matching path and the id for each part of that path.
        If the root of the given folder was not found, returns ``None``
        """
        async def find_part(
            *folder_parts: str,
            parent_id: FileId|None
        ) -> list[FileId]|None:
            folder_part = folder_parts[0]
            folders_remain = folder_parts[1:]
            folder_part_cleaned = self.escape_filename(folder_part)
            q = f"trashed = false and mimeType = '{FOLDER_MTYPE}' and name = '{folder_part_cleaned}'"
            if parent_id is not None:
                q = f"{q} and '{parent_id}' in parents"
            # logger.debug(f'{q=}')
            found_fids: list[FileId] = []
            found_parts: list[str] = []
            async for f in self.list_files(q, full_res=True):
                f_id = f.get('id')
                assert isinstance(f_id, str)
                found_fids.append(f_id)
                found_parts.append(folder_part)
                if not len(folders_remain):
                    break
                sub_fid = await find_part(*folders_remain, parent_id=f_id)
                if sub_fid is not None:
                    found_fids.extend(sub_fid)
                break
            if len(found_fids):
                return found_fids
            return None

        parts = folder.parts
        folder_ids: list[FileId]|None = None
        cached = self.find_folder_cache(folder)
        if cached is not None:
            cached_ids, cached_folder = cached
            # logger.debug(f'cache hit: {cached_folder=}')
            if cached_folder == folder:
                return cached_ids[-1], cached_folder
            # parts = cached_folder.parts
            remaining_folder = folder.relative_to(cached_folder)
            _folder_ids = await find_part(*remaining_folder.parts, parent_id=cached_ids[-1])
            if _folder_ids is not None:
                folder_ids = cached_ids + _folder_ids

        if folder_ids is None:
            folder_ids = await find_part(*parts, parent_id=None)

        if folder_ids is not None:
            # logger.info(f'{folder_ids=}')
            found_parts = parts[:len(folder_ids)]
            self.cache_folder_parts(*zip(found_parts, folder_ids))
            # if len(folder_ids) == len(parts):
            #     return folder_ids[-1]
            result_p = Path('/'.join(found_parts))
            return folder_ids[-1], result_p
        return None

    async def create_folder(
        self,
        name: str,
        parent: FileId|None
    ) -> FileId:
        """Create a Drive folder with the given *name*

        If *parent* is given, it should be a valid folder id to use as a parent
        folder
        """
        # drive_v3 = await aiogoogle.discover('drive', 'v3')
        file_meta: DriveFileMeta = {
            'name': name,
            'mimeType': FOLDER_MTYPE,
        }
        if parent is not None:
            file_meta['parents'] = [parent]
        logger.debug(f'create folder: {name=}, {parent=}')

        req = self.drive_v3.files.create(
            # body=file_meta,
            json=file_meta,
            fields="id",
        )
        res = await self.as_user(req, resp_type=FileUploadResponse)
        # cache_single_folder()

        return res['id']

    async def create_folder_nested(
        self,
        *parts: str,
        parent: FileId|None,
        parent_path: Path|None = None,
        use_cache: bool = False
    ) -> FileId:
        """Create a nested group of folders using the *parts* for each name

        Arguments:
            *parts: The folder name for each directory level
            parent: The id of the folder to create the nested folders in
            parent_path: The root path to use if *use_cache* is True
            use_cache: If True, stores the results in the :attr:`folder_cache`.
                *parent_path* must be provided to accurately store the results

        """
        prev_parent = parent
        full_path = parent_path
        for part in parts:
            prev_parent = await self.create_folder(part, prev_parent)
            if use_cache and full_path is not None:
                full_path = full_path / part
                self.cache_single_folder(full_path, prev_parent)
        assert prev_parent is not None
        return prev_parent


    async def create_folder_from_path(
        self,
        folder: Path
    ) -> FileId:
        """Find or create a (possibly nested) Drive folder with the given path
        """
        async with self.cache_lock:
            result = await self._create_folder_from_path(folder)
        return result

    async def _create_folder_from_path(self, folder: Path) -> FileId:
        find_result = await self.find_folder(folder)
        logger.debug(f'{find_result=}')
        if find_result is not None:
            f_id, find_path = find_result
            if find_path == folder:
                return f_id
            remaining_path = folder.relative_to(find_path)
            logger.debug(f'{remaining_path=}')
            # remaining_parts = folder.parts[:len(find_path.parts)]
            path_parts = remaining_path.parts
            parent_path = find_path
            parent = f_id
            use_cache = True
            # return await create_folder_nested(aiogoogle, drive_v3, *remaining_path.parts, parent=f_id)
        else:
            path_parts = folder.parts
            parent_path = None
            parent = None
            use_cache = False
        # return await create_folder_nested(aiogoogle, drive_v3, *folder.parts, parent=None)
        return await self.create_folder_nested(
            *path_parts, parent=parent,
            parent_path=parent_path, use_cache=use_cache,
        )

    async def file_exists(
        self,
        filename: str,
        parent_id: FileId|None
    ) -> bool:
        filename_cleaned = self.escape_filename(filename)
        q = f"trashed = false and mimeType != '{FOLDER_MTYPE}' and name = '{filename_cleaned}'"
        if parent_id is not None:
            q = f"{q} and '{parent_id}' in parents"
        # logger.debug(f'{q=}')
        async for f in self.list_files(q, full_res=True):
            return True
        return False

    async def get_file_meta(
        self,
        filename: Path
    ) -> DriveFileMetaFull|None:
        """Get metadata for the given file (if it exists)
        """
        folder = filename.parent
        folder_find = await self.find_folder(folder)
        if folder_find is None:
            return None
        folder_id, folder_path = folder_find
        if folder_path != folder:
            return None
        q = f"trashed = false and mimeType != '{FOLDER_MTYPE}' and name = '{filename.name}'"
        q = f"{q} and '{folder_id}' in parents"
        # logger.debug(f'{q=}')
        async for f in self.list_files(q, fields='*', full_res=True):
            return f

    async def stream_upload_file(
        self,
        local_file: Path,
        upload_filename: Path,
        check_exists: bool = True,
        folder_id: FileId|None = None,
        check_hash: bool = True,
        timeout_total: float|None = None,
        timeout_chunk: float|None = None
    ) -> DriveFileMetaFull|None:
        """Upload a file to Drive

        Arguments:
            local_file: The local filename
            upload_filename: Relative path for the uploaded file
            check_exists: Whether to check if *upload_filename* already exists
                in Drive
            folder_id: If given, the parent folder's :obj:`id <FileId>`.  If
                not provided, the parent folder(s) will be searched for and
                created if necessary.
            check_hash: If ``True``, the hash of the local file will be checked
                against the hash from the uploaded
                :class:`metadata <.types.DriveFileMetaFull>`

        Returns:
            The metadata for the uploaded file.
            If *check_exists* is True and the file exists in Drive,
            ``None`` is returned.

        Raises:
            UploadError: If *check_hash* is True and the content hashes
                do not match
        """
        chunk_size = 64*1024

        if folder_id is not None:
            parent = folder_id
        else:
            path_parts = upload_filename.parts
            if len(path_parts) > 1:
                parent = await self.create_folder_from_path(upload_filename.parent)
            else:
                parent = None

        file_meta: DriveFileMeta = {
            'name': upload_filename.name,
        }
        if parent is not None:
            file_meta['parents'] = [parent]
        if check_exists:
            exists = await self.file_exists(upload_filename.name, parent)
            if exists:
                logger.warning(f'file "{upload_filename}" exists')
                return None
        logger.debug(f'uploading "{upload_filename}"')
        async with aiofile.async_open(local_file, 'rb') as src_fd:
            chunk_iter = aio_read_iter(
                src_fd, chunk_size=chunk_size,
                timeout_total=timeout_total, timeout_chunk=timeout_chunk,
            )
            req = self.drive_v3.files.create(
                pipe_from=chunk_iter,
                fields="id",
                json=file_meta,
            )
            upload_res = await self.as_user(req, resp_type=FileUploadResponse)
            logger.debug(f"Upload complete for {upload_filename}. File ID: {upload_res['id']}")
        uploaded_meta = await self.get_file_meta(upload_filename)
        assert uploaded_meta is not None
        if check_hash:
            local_hash = await get_file_hash_async('sha1', local_file)
            remote_hash = uploaded_meta['sha1Checksum']
            matched = local_hash == remote_hash
            logger.debug(f'Checking hashes for "{local_file}": {matched=}')
            if not matched:
                raise UploadError(f'Uploaded file hash mismatch for "{local_file}"')
        return uploaded_meta

    async def update_existing_file(
        self,
        file_id: FileId,
        local_file: Path,
        check_hash: bool = True,
    ) -> DriveFileMetaFull:
        """Upload a revision for an file that already exists in Drive

        Arguments:
            file_id: The id of the file to update
            local_file: The local file to upload
            check_hash: If ``True``, the hash of the local file will be checked
                against the hash from the uploaded
                :class:`metadata <.types.DriveFileMetaFull>`

        Returns:
            DriveFileMetaFull: The metadata for the uploaded file

        Raises:
            UploadError: If *check_hash* is True and the content hashes
                do not match
        """
        req = self.drive_v3.files.update(
            fileId=file_id,
            upload_file=local_file,
            fields='*',
        )
        assert req.media_upload is not None
        # Important! This must be set to False for files.update
        # See https://github.com/omarryhan/aiogoogle/issues/118
        req.media_upload.multipart = False
        upload_res = await self.as_user(req, resp_type=DriveFileMetaFull)
        if not check_hash:
            return upload_res
        local_hash = await get_file_hash_async('sha1', local_file)
        remote_hash = upload_res['sha1Checksum']
        if local_hash != remote_hash:
            raise UploadError(f'Uploaded file hash mismatch for "{local_file}": {local_hash=}, {remote_hash=}')
        return upload_res

    async def prebuild_paths(self, *paths: Path):
        """Build multiple Drive folders from the given *paths*

        Uses a tree of :class:`~.pathtree.PathNode` objects to search for and
        create non-existent folders while minimizing Drive API calls.

        This can be more efficient during uploads since the
        :meth:`create_folder_from_path` method relies on a :class:`~asyncio.Lock`
        for concurrency.
        """
        logger.info(f'prebuilding paths...')
        count = 0
        async with self.cache_lock:
            root = PathNode.create_from_paths(*paths, folder_cache=self.folder_cache)
            to_build = root.cost
            logger.debug(f'prebuild count: {to_build}')
            for node in root.walk_non_cached():#(breadth_first=True):
                parent_id = None if node.parent is None else node.parent.folder_id
                if node.parent is not None:
                    if parent_id is None:
                        logger.warning(f'parent cache missing: {node=}, {node.parent=}')
                        parent_id = await self._create_folder_from_path(node.parent.full_path)
                        node.folder_id = parent_id

                logger.debug(f'create folder {node.full_path}, {parent_id=}')
                f_id = await self.create_folder(node.part, parent_id)
                self.cache_single_folder(node.full_path, f_id)
                node.folder_id = f_id
                count += 1
        self.save_folder_cache()
        logger.info(f'prebuild complete: {count} of {to_build}')


class ClipGoogleClient(GoogleClient):
    """Client to upload items from a :class:`.model.ClipCollection`
    """
    max_clips: int
    """Maximum number of :class:`Clips <.model.Clip>` to upload"""
    def __init__(
        self,
        root_conf: Config,
        max_clips: int = 8,
        scheduler_limit: int = 8
    ) -> None:
        super().__init__(root_conf)
        self.max_clips = max_clips
        self.scheduler_limit = scheduler_limit
        self.completion_counts = CompletionCounts(max_clips, enable_log=True)

    @property
    def upload_dir(self) -> Path:
        """Root folder name to upload within Drive
        (alias for :attr:`.config.GoogleConfig.drive_folder`)
        """
        return self.root_conf.google.drive_folder

    async def __aenter__(self) -> Self:
        self.schedulers: SchedulersTD = {
            'general': aiojobs.Scheduler(limit=32, pending_limit=1),
            'uploads': aiojobs.Scheduler(limit=self.scheduler_limit),
            'upload_checks': aiojobs.Scheduler(limit=UPLOAD_CHECK_LIMIT, pending_limit=1),
            'clip_uploads': aiojobs.Scheduler(limit=8, pending_limit=1),
        }
        self.upload_check_waiters = JobWaiters[tuple[bool, Clip, set[Path]]](scheduler=self.schedulers['upload_checks'])
        self.upload_clip_waiters = JobWaiters[bool](scheduler=self.schedulers['clip_uploads'])
        return await super().__aenter__()

    async def __aexit__(self, *args) -> None:
        logger.info('closing schedulers..')
        scheduler_list: list[aiojobs.Scheduler] = [
            self.schedulers[key] for key in self.schedulers.keys()
        ]
        await asyncio.gather(*[sch.close() for sch in scheduler_list])
        return await super().__aexit__(*args)

    def get_clip_file_upload_path(self, clip: Clip, key: ClipFileUploadKey) -> Path:
        """Get the uploaded filename for a clip asset (relative to :attr:`upload_dir`)
        """
        rel_filename = clip.get_file_path(key, absolute=False)
        return self.upload_dir / rel_filename

    async def upload_data_file(self) -> DriveFileMetaFull:
        """Upload the data file to Drive
        """
        local_filename = self.root_conf.data_file
        assert not local_filename.is_absolute()
        upload_filename = self.upload_dir / local_filename.name
        meta = await self.get_file_meta(upload_filename)
        if meta is not None:
            local_hash = await get_file_hash_async('sha1', local_filename)
            remote_hash = meta['sha1Checksum']
            if local_hash == remote_hash:
                logger.info(f'Data file matches hosted version: {upload_filename}')
                return meta
            logger.info(f'Update existing data file: {local_filename} -> {upload_filename}')
            upload_meta = await self.update_existing_file(
                meta['id'],
                local_filename,
                check_hash=True,
            )
        else:
            logger.info(f'Upload new data file: {local_filename} -> {upload_filename}')
            upload_meta = await self.stream_upload_file(
                local_filename,
                upload_filename,
                check_exists=False,
                check_hash=True,
            )
        assert upload_meta is not None
        return upload_meta

    async def get_clip_file_meta(
        self,
        clip: Clip,
        key: ClipFileUploadKey
    ) -> tuple[DriveFileMetaFull|None, bool]:
        """Get metadata for the given clip file (if it exists)

        The local :attr:`~.GoogleClient.meta_cache` will first be checked.
        If not found, it will be requested using :meth:`~.GoogleClient.get_file_meta`
        and stored in the cache (if successful).

        Returns:
            (tuple):
                - **metadata** (:class:`.types.DriveFileMetaFull`, *optional*): The
                  metadata for the file, or ``None`` if it does not exist in Drive
                - **is_cached** (:class:`bool`): Whether the metadata was retrieved from the
                  :attr:`~.GoogleClient.meta_cache`
        """
        meta_key = ('clips', clip.id, key)
        meta = self.find_cached_file_meta(meta_key)
        is_cached = meta is not None
        if meta is None:
            upload_filename = self.get_clip_file_upload_path(clip, key)
            meta = await self.get_file_meta(upload_filename)
            if meta is not None:
                self.set_cached_file_meta(meta_key, meta)
        return meta, is_cached

    async def upload_clip(
        self,
        clip: Clip,
    ) -> bool:
        """Upload all assets for the given clip
        """
        waiter = JobWaiters[bool](scheduler=self.schedulers['uploads'])
        num_jobs = 0
        drive_folder_id: FileId|None = None

        async def _do_upload(
            key: ClipFileUploadKey,
            filename: Path,
            upload_filename: Path,
            folder_id: FileId|None
        ) -> bool:
            meta = await self.stream_upload_file(
                filename, upload_filename, folder_id=folder_id,
                timeout_chunk=60,
            )
            if meta is not None:
                self.set_cached_file_meta(('clips', clip.id, key), meta)
                return True
            return False

        for key, filename in clip.iter_paths(for_download=False):
            if not filename.exists():
                continue
            upload_filename = self.get_clip_file_upload_path(clip, key)
            if drive_folder_id is None:
                cached = self.find_folder_cache(upload_filename.parent)
                if cached is not None:
                    cached_ids, cached_path = cached
                    if cached_path == upload_filename.parent:
                        drive_folder_id = cached_ids[-1]
                        logger.info('CACHE HIT')
                    else:
                        logger.warning('CACHE MISS')
            if drive_folder_id is None:
                drive_folder_id = await self.create_folder_from_path(
                    upload_filename.parent
                )
            await waiter.spawn(_do_upload(
                key, filename, upload_filename, drive_folder_id,
            ))
            num_jobs += 1

        if not len(waiter):
            self.completion_counts.num_completed += 1
            return True
        results = await waiter
        all_skipped = True not in results
        logger.success(f'Upload complete for clip "{clip.unique_name}"')
        self.completion_counts.num_completed += 1
        return all_skipped


    @logger.catch(reraise=True)
    async def check_clip_needs_upload(
        self,
        clip: Clip
    ) -> tuple[bool, Clip, set[Path]]:
        """Check if the given clip has any assets that need to be uploaded
        """
        async def do_check(key: ClipFileUploadKey, filename: Path) -> tuple[bool, Path]:
            upload_filename = self.get_clip_file_upload_path(clip, key)
            upload_meta, _ = await self.get_clip_file_meta(clip, key)
            if upload_meta is None:
                return True, upload_filename
            local_meta = clip.files.get_metadata(key)
            assert local_meta is not None
            assert 'size' in upload_meta
            local_size = filename.stat().st_size
            upload_size = int(upload_meta['size'])
            if upload_size != local_size:
                logger.warning(f'size mismatch for "{filename}", {local_size=}, {upload_size=}')
            assert local_meta.sha1 is not None
            if local_meta.sha1 != upload_meta['sha1Checksum']:
                raise HashMismatchError(f'hash mismatch for "{filename}"')
            return False, upload_filename
        if self.completion_counts.full:
            return False, clip, set()
        coros: set[Coroutine[Any, Any, tuple[bool, Path]]] = set()
        for key, filename in clip.iter_paths(for_download=False):
            if not filename.exists():
                continue
            coros.add(do_check(key, filename))
        all_paths = set[Path]()
        needs_upload = False
        if len(coros):
            results = await asyncio.gather(*coros)
            for _needs_upload, upload_filename in results:
                if _needs_upload:
                    needs_upload = True
                    all_paths.add(upload_filename)
        return needs_upload, clip, all_paths

    async def handle_upload_check_jobs(self) -> None:
        """Wait for jobs from :meth:`check_clip_needs_upload` and spawns their
        :meth:`upload_clip` jobs

        The folders from each incoming job will be created with
        :meth:`~GoogleClient.prebuild_paths` before spawning the upload jobs
        """
        upload_dirs = set[Path]()
        clips_to_upload: list[Clip] = []
        if self.completion_counts.full:
            return
        async for job in self.upload_check_waiters:
            if job.exception is not None:
                raise job.exception
            needs_upload, clip, upload_filenames = job.result
            if not needs_upload:
                continue
            upload_dirs |= set([p.parent for p in upload_filenames])
            clips_to_upload.append(clip)

        if len(upload_dirs):
            _clip_ids = [c.id for c in clips_to_upload]
            logger.debug(f'{_clip_ids=}, {upload_dirs=}')
            await self.prebuild_paths(*upload_dirs)
        for clip in clips_to_upload:
            await self.upload_clip_waiters.spawn(self.upload_clip(clip))
            self.completion_counts.num_queued += 1
            if self.completion_counts.full:
                break

    async def upload_all(self, clips: ClipCollection) -> None:
        """Upload assets for the given clips (up to by :attr:`max_clips`)
        """
        upload_check_sch = self.schedulers['upload_checks']
        upload_check_limit = upload_check_sch.limit
        assert upload_check_limit is not None
        for clip in clips:
            await self.upload_check_waiters.spawn(self.check_clip_needs_upload(clip))
            if len(self.upload_check_waiters) >= upload_check_limit:
                await self.handle_upload_check_jobs()
            if self.completion_counts.full:
                break

        await self.handle_upload_check_jobs()
        if len(self.upload_check_waiters):
            logger.info(f'waiting for check_waiters ({len(self.upload_check_waiters)=})')
            await self.upload_check_waiters

        self.completion_counts.max_items = self.completion_counts.num_queued
        if len(self.upload_clip_waiters):
            logger.info(f'waiting for waiters ({len(self.upload_clip_waiters)=})')
            await self.upload_clip_waiters
        logger.success(f'all waiters finished')
        await self.upload_data_file()

    @logger.catch(reraise=True)
    async def check_meta(
        self,
        clips: ClipCollection,
        enable_hashes: bool,
        hash_logfile: Path|None = None
    ) -> bool:
        """Check metadata for all available files stored on Drive

        Arguments:
            enable_hashes: If ``True``, the :attr:`~.types.DriveFileMetaFull.sha1Checksum`
                will be compared with the hash of the local file contents
            hash_logfile: Optional file to store / load the local file hashes.
                This can greatly reduce the time required since most files are
                rather large.

        .. note::

            If using *hash_logfile*, it is recommended to delete it periodically,
            especially if there have been any major changes to the local files.

        Raises:
            UploadError: If *enable_hashes* is True and the content hashes
                do not match

        Returns:
            bool: Whether any changes to the :attr:`~GoogleClient.meta_cache` were made
        """

        def load_hash_data():
            if hash_logfile is None:
                return {}
            if not hash_logfile.exists():
                return {}
            return json.loads(hash_logfile.read_text())

        hash_dict: dict[CLIP_ID, dict[ClipFileUploadKey, SHA1Hash]] = load_hash_data()

        def save_hash_data():
            if hash_logfile is None:
                return
            hash_logfile.write_text(json.dumps(hash_dict, indent=2))

        def store_local_hash(clip: Clip|CLIP_ID, key: ClipFileUploadKey, value: SHA1Hash):
            clip_id = clip if isinstance(clip, str) else clip.id
            d = hash_dict.setdefault(clip_id, {})
            d[key] = value
            save_hash_data()

        def get_local_hash(clip: Clip|CLIP_ID, key: ClipFileUploadKey) -> SHA1Hash|None:
            clip_id = clip if isinstance(clip, str) else clip.id
            clip = clips[clip_id]
            meta = clip.files.get_metadata(key)
            if meta is not None and meta.sha1 is not None:
                return meta.sha1
            d = hash_dict.get(clip_id, {})
            return d.get(key)

        async def check_hash(
            clip_id: CLIP_ID,
            key: ClipFileUploadKey,
            filename: Path,
            remote_meta: DriveFileMetaFull,
        ):
            sch = self.schedulers['uploads']
            local_hash = get_local_hash(clip_id, key)
            if local_hash is None:
                local_hash = await get_file_hash_async('sha1', filename)
            remote_hash = remote_meta['sha1Checksum']
            matched = local_hash == remote_hash
            logger.debug(f'Checking hashes for "{filename}": {matched=}, {sch.active_count=}')
            if not matched:
                raise UploadError(f'Uploaded file hash mismatch for "{filename}"')
            if hash_logfile is not None:
                store_local_hash(clip_id, key, local_hash)

        async def check_file(
            clip: Clip,
            key: ClipFileUploadKey,
            filename: Path
        ) -> tuple[CLIP_ID, ClipFileUploadKey, DriveFileMetaFull|None, bool, Path]:
            meta, is_cached = await self.get_clip_file_meta(clip, key)
            return clip.id, key, meta, is_cached, filename

        async def check_item(clip: Clip) -> bool:
            changed = False
            hash_waiters = JobWaiters(scheduler=self.schedulers['uploads'])
            meta_waiters = JobWaiters[tuple[CLIP_ID, ClipFileUploadKey, DriveFileMetaFull|None, bool, Path]](
                scheduler=self.schedulers['general']
            )
            for key, filename in clip.iter_paths(for_download=False):
                if not filename.exists():
                    continue
                await meta_waiters.spawn(check_file(clip, key, filename))

            async for job in meta_waiters:
                if job.exception is not None:
                    raise job.exception
                clip_id, key, meta, is_cached, filename = job.result
                if meta is None:
                    continue
                if not is_cached:
                    changed = True
                if enable_hashes:
                    await hash_waiters.spawn(check_hash(clip_id, key, filename, meta))
            await hash_waiters
            return changed

        item_waiters = JobWaiters[bool](scheduler=self.schedulers['upload_checks'])
        for clip in clips:
            logger.info(f'check_item: {clip.unique_name}')
            await item_waiters.spawn(check_item(clip))

        logger.success('all items scheduled')
        results = await item_waiters
        changed = any(results)
        return changed

_LegModelT = TypeVar('_LegModelT', bound=AbstractLegistarModel)

class AbstractLegistarGoogleClient(GoogleClient, Generic[_GuidT, _ItemT, _LegModelT], ABC):
    max_clips: int
    """Maximum number of items to upload"""
    legistar_data: _LegModelT
    """A :class:`~.legistar.model.LegistarData` instance"""
    item_scheduler: aiojobs.Scheduler
    check_scheduler: aiojobs.Scheduler
    upload_scheduler: aiojobs.Scheduler
    def __init__(
        self,
        root_conf: Config,
        max_clips: int,
        legistar_data: _LegModelT|None = None
    ) -> None:
        super().__init__(root_conf)
        self.max_clips = max_clips
        if legistar_data is None:
            legistar_data = self._load_legistar_data()
        self.legistar_data = legistar_data
        self.completion_counts = CompletionCounts(max_clips, enable_log=True)

    @property
    @abstractmethod
    def upload_dir(self) -> Path: ...

    @property
    @abstractmethod
    def local_data_file(self) -> Path: ...

    @abstractmethod
    def _load_legistar_data(self) -> _LegModelT: ...

    @abstractmethod
    def _build_cache_key(self, guid: _GuidT, uid: LegistarFileUID) -> MetaCacheKey: ...

    @abstractmethod
    def get_remote_counts(self) -> MetaCount: ...

    def get_local_counts(self) -> MetaCount:
        num_items = 0
        num_files = 0
        for guid in self.legistar_data.keys():
            has_files = False
            for _ in self.legistar_data.iter_files_for_upload(guid):
                num_files += 1
                has_files = True
            if has_files:
                num_items += 1
        return MetaCount(num_items, num_files)

    def log_counts(self) -> None:
        local_counts = self.get_local_counts()
        remote_counts = self.get_remote_counts()
        logger.info(f'Local:     items={local_counts.items:6d}, files={local_counts.files:6d}')
        logger.info(f'Remote:    items={remote_counts.items:6d}, files={remote_counts.files:6d}')
        diff = MetaCount(local_counts.items - remote_counts.items, local_counts.files - remote_counts.files)
        logger.info(f'To Upload: items={diff.items:6d}, files={diff.files:6d}')

    async def __aenter__(self) -> Self:
        self.item_scheduler = aiojobs.Scheduler(limit=16)
        self.check_scheduler = aiojobs.Scheduler(limit=16, pending_limit=1)
        self.upload_scheduler = aiojobs.Scheduler(limit=32)
        self.check_waiters = JobWaiters[tuple[bool, _GuidT, set[Path]]](
            scheduler=self.check_scheduler
        )
        self.item_waiters = JobWaiters[bool](scheduler=self.item_scheduler)
        return await super().__aenter__()

    async def __aexit__(self, *args) -> None:
        await self.check_scheduler.wait_and_close()
        await self.item_scheduler.wait_and_close()
        await self.upload_scheduler.wait_and_close()
        return await super().__aexit__(*args)

    async def upload_data_file(self) -> DriveFileMetaFull:
        """Upload the data file to Drive
        """
        local_filename = self.local_data_file
        upload_filename = self.upload_dir / local_filename.name
        meta = await self.get_file_meta(upload_filename)
        if meta is not None:
            local_hash = await get_file_hash_async('sha1', local_filename)
            remote_hash = meta['sha1Checksum']
            if local_hash == remote_hash:
                logger.info(f'Data file matches hosted version: {upload_filename}')
                return meta
            logger.info(f'Update existing data file: {local_filename} -> {upload_filename}')
            upload_meta = await self.update_existing_file(
                meta['id'],
                local_filename,
                check_hash=True,
            )
        else:
            logger.info(f'Upload new data file: {local_filename} -> {upload_filename}')
            upload_meta = await self.stream_upload_file(
                local_filename,
                upload_filename,
                check_exists=False,
                check_hash=True,
            )
        assert upload_meta is not None
        return upload_meta

    def get_file_upload_path(self, guid: _GuidT, uid: LegistarFileUID) -> Path:
        """Get the uploaded filename for a legistar asset (relative to :attr:`upload_dir`)
        """
        filename, _ = self.legistar_data.get_path_for_uid(guid, uid)
        assert not filename.is_absolute()
        filename = filename.relative_to(self.legistar_data.root_dir)
        return self.upload_dir / filename

    def get_local_meta(self, guid: _GuidT, uid: LegistarFileUID) -> ClipFileMeta|None:
        """Get the local file metadata for the given *guid* and *uid*
        """
        _, meta = self.legistar_data.get_path_for_uid(guid, uid)
        return meta

    async def get_remote_meta(
        self,
        guid: _GuidT,
        uid: LegistarFileUID
    ) -> tuple[DriveFileMetaFull|None, bool]:
        """Get metadata for the filename matching *guid* and *uid* (if it exists)

        The local :attr:`~.GoogleClient.meta_cache` will first be checked.
        If not found, it will be requested using :meth:`~.GoogleClient.get_file_meta`
        and stored in the cache (if successful).

        Returns:
            (tuple):
                - **metadata** (:class:`~.types.DriveFileMetaFull`, *optional*): The
                  metadata for the file, or ``None`` if it does not exist in Drive
                - **is_cached** (:class:`bool`): Whether the metadata was retrieved from the
                  :attr:`~.GoogleClient.meta_cache`
        """
        meta_key = self._build_cache_key(guid, uid)
        meta = self.find_cached_file_meta(meta_key)
        is_cached = meta is not None
        if meta is None:
            upload_filename = self.get_file_upload_path(guid, uid)
            meta = await self.get_file_meta(upload_filename)
            if meta is not None:
                self.set_cached_file_meta(meta_key, meta)
        return meta, is_cached

    async def upload_legistar_file(
        self,
        guid: _GuidT,
        uid: LegistarFileUID,
        filename: Path,
        upload_filename: Path,
        folder_id: FileId
    ) -> bool:
        """Upload a single legistar file
        """
        meta = await self.stream_upload_file(
            filename, upload_filename, folder_id=folder_id,
        )
        if meta is not None:
            meta_key = self._build_cache_key(guid, uid)
            self.set_cached_file_meta(meta_key, meta)
            return True
        return False

    async def upload_legistar_item(self, guid: _GuidT):
        """Upload all files for the legistar item matching the given *guid*
        """
        waiters = JobWaiters[bool](scheduler=self.upload_scheduler)
        folder_ids: list[FileId|None] = [None, None]
        it = self.legistar_data.iter_files_for_upload(guid)
        for uid, filename, local_meta, is_attachment in it:
            upload_filename = self.get_file_upload_path(guid, uid)
            f_id_index = 1 if is_attachment else 0
            folder_id = folder_ids[f_id_index]
            if folder_id is None:
                await asyncio.sleep(0)
                cached = self.find_folder_cache(upload_filename.parent)
                if cached is not None:
                    cached_ids, cached_path = cached
                    if cached_path == upload_filename.parent:
                        folder_id = folder_ids[f_id_index] = cached_ids[-1]
                        logger.info('CACHE HIT')
                    else:
                        logger.warning('CACHE MISS')
                if folder_id is None:
                    folder_id = folder_ids[f_id_index] = await self.create_folder_from_path(
                        upload_filename.parent
                    )
            await waiters.spawn(self.upload_legistar_file(
                guid, uid, filename, upload_filename, folder_id,
            ))
            # logger.warning(f'{sch.active_count=}, {len(sch)=}, {sch.pending_count=}, {len(waiters)=}')

        if not len(waiters):
            self.completion_counts.num_completed += 1
            return True
        results = await waiters
        all_skipped = True not in results
        logger.info(f'Upload complete for guid: {guid}')
        self.completion_counts.num_completed += 1
        return all_skipped

    async def check_item_needs_upload(self, guid: _GuidT) -> tuple[bool, _GuidT, set[Path]]:
        """Check if the item matching *guid* has any local files that need to
        be uploaded
        """
        async def do_check(
            uid: LegistarFileUID,
            filename: Path,
            local_meta: ClipFileMeta
        ) -> tuple[bool, Path]:
            upload_filename = self.get_file_upload_path(guid, uid)
            upload_meta, _ = await self.get_remote_meta(guid, uid)
            if upload_meta is None:
                return True, upload_filename
            assert 'size' in upload_meta
            local_size = filename.stat().st_size
            upload_size = int(upload_meta['size'])
            if upload_size != local_size:
                logger.warning(f'size mismatch for "{filename}", {local_size=}, {upload_size=}')
            if local_meta.sha1 is not None:
                if local_meta.sha1 != upload_meta['sha1Checksum']:
                    current_hash = await get_file_hash_async('sha1', filename)
                    if current_hash != local_meta.sha1:
                        raise HashMismatchError(f'LOCAL hash mismatch for "{filename}"')
                    raise HashMismatchError(f'REMOTE hash mismatch for "{filename}"')
            return False, upload_filename
        if self.completion_counts.full:
            return False, guid, set()
        coros: set[Coroutine[Any, Any, tuple[bool, Path]]] = set()
        it = self.legistar_data.iter_files_for_upload(guid)
        for uid, filename, local_meta, is_attachment in it:
            coros.add(do_check(uid, filename, local_meta))

        upload_filenames: set[Path] = set()
        needs_upload = False
        if len(coros):
            results = await asyncio.gather(*coros)
            for _needs_upload, upload_filename in results:
                if _needs_upload:
                    needs_upload = True
                    upload_filenames.add(upload_filename)
        return needs_upload, guid, upload_filenames

    async def handle_upload_check_jobs(self) -> None:
        """Wait for jobs from :meth:`check_item_needs_upload` and spawns their
        :meth:`upload_legistar_item` jobs

        The folders from each incoming job will be created with
        :meth:`~GoogleClient.prebuild_paths` before spawning the upload jobs
        """
        upload_dirs = set[Path]()
        guids_to_upload: list[_GuidT] = []
        if self.completion_counts.full:
            return
        async for job in self.check_waiters:
            if job.exception is not None:
                raise job.exception
            needs_upload, guid, upload_filenames = job.result
            if not needs_upload:
                continue
            upload_dirs |= set([p.parent for p in upload_filenames])
            guids_to_upload.append(guid)

        if len(upload_dirs):
            # logger.debug(f'{upload_dirs=}')
            await self.prebuild_paths(*upload_dirs)
        for guid in guids_to_upload:
            await self.item_waiters.spawn(self.upload_legistar_item(guid))
            self.completion_counts.num_queued += 1
            if self.completion_counts.full:
                break

    async def upload_all(self):
        """Upload files for all items in :attr:`legistar_data` (up to by :attr:`max_clips`)
        """
        check_waiters = self.check_waiters
        check_limit = self.check_scheduler.limit
        assert check_limit is not None
        item_waiters = self.item_waiters
        if self.max_clips == 0:
            return

        try:
            for guid in self.legistar_data.keys():
                await check_waiters.spawn(self.check_item_needs_upload(guid))
                if len(check_waiters) >= check_limit:
                    await self.handle_upload_check_jobs()
                if self.completion_counts.full:
                    break
            await self.handle_upload_check_jobs()
            logger.info(f'all jobs scheduled, waiting ({len(item_waiters)=}), {len(check_waiters)=}, {self.completion_counts}')
            await check_waiters
            logger.debug('all items queued')
            self.completion_counts.max_items = self.completion_counts.num_queued
            await item_waiters
            logger.success('all jobs completed')
            await self.upload_data_file()
        except RateLimitError:
            self.save_cache()
            raise

    @logger.catch(reraise=True)
    async def check_meta(self, enable_hashes: bool) -> bool:
        """Check metadata for all available files stored on Drive

        Arguments:
            enable_hashes: If ``True``, the :attr:`~.types.DriveFileMetaFull.sha1Checksum`
                will be compared with the hash of the local file contents

        Raises:
            UploadError: If *enable_hashes* is True and the content hashes
                do not match

        Returns:
            bool: Whether any changes to the :attr:`~GoogleClient.meta_cache` were made
        """
        async def check_hash(
            filename: Path,
            local_meta: ClipFileMeta,
            remote_meta: DriveFileMetaFull,
        ):
            if local_meta.sha1 is not None:
                local_hash = local_meta.sha1
            else:
                local_hash = await get_file_hash_async('sha1', filename)
            remote_hash = remote_meta['sha1Checksum']
            matched = local_hash == remote_hash
            logger.debug(f'Checking hashes for "{filename}": {matched=}')
            if not matched:
                raise UploadError(f'Uploaded file hash mismatch for "{filename}"')

        async def check_file(
            guid: _GuidT,
            uid: LegistarFileUID,
            local_meta: ClipFileMeta,
            filename: Path
        ) -> tuple[ClipFileMeta, DriveFileMetaFull|None, bool, Path]:
            meta, is_cached = await self.get_remote_meta(guid, uid)
            return local_meta, meta, is_cached, filename

        async def check_item(guid: _GuidT) -> bool:
            changed = False
            hash_waiters = JobWaiters(scheduler=self.upload_scheduler)
            meta_waiters = JobWaiters[tuple[ClipFileMeta, DriveFileMetaFull|None, bool, Path]](
                scheduler=self.upload_scheduler,
            )

            it = self.legistar_data.iter_files_for_upload(guid)
            for uid, filename, local_meta, is_attachment in it:
                await meta_waiters.spawn(check_file(guid, uid, local_meta, filename))

            async for job in meta_waiters:
                if job.exception is not None:
                    raise job.exception
                local_meta, remote_meta, is_cached, filename = job.result
                if remote_meta is None:
                    continue
                if not is_cached:
                    changed = True
                if enable_hashes:
                    await hash_waiters.spawn(check_hash(filename, local_meta, remote_meta))
            await hash_waiters
            return changed

        item_waiters = JobWaiters[bool](scheduler=self.check_scheduler)
        for guid in self.legistar_data.keys():
            await item_waiters.spawn(check_item(guid))

        logger.success('all items scheduled')
        try:
            results = await item_waiters
        except RateLimitError:
            self.save_cache()
            raise
        changed = any(results)
        return changed



class LegistarGoogleClient(AbstractLegistarGoogleClient[GUID, DetailPageResult, LegistarData]):
    """Client to upload items from :class:`~.legistar.model.LegistarData`
    """

    @property
    def upload_dir(self) -> Path:
        """Root folder name to upload within Drive
        (alias for :attr:`.config.GoogleConfig.legistar_drive_folder`)
        """
        return self.root_conf.google.legistar_drive_folder

    @property
    def local_data_file(self) -> Path:
        return self.root_conf.legistar.data_file

    def _load_legistar_data(self) -> LegistarData:
        return LegistarData.load(self.local_data_file)

    def _build_cache_key(self, guid: GUID, uid: LegistarFileUID) -> MetaCacheKey:
        return ('legistar', guid, uid)

    def get_remote_counts(self) -> MetaCount:
        counts = self.meta_cache.get_counts()
        return counts.legistar


class RGuidLegistarGoogleClient(AbstractLegistarGoogleClient[REAL_GUID, RGuidDetailResult, RGuidLegistarData]):
    """Client to upload items from :class:`~.legistar.guid_model.RGuidLegistarData`
    """
    @property
    def upload_dir(self) -> Path:
        """Root folder name to upload within Drive
        (alias for :attr:`.config.GoogleConfig.rguid_legistar_drive_folder`)
        """
        return self.root_conf.google.rguid_legistar_drive_folder

    @property
    def local_data_file(self) -> Path:
        return RGuidLegistarData._get_data_file(self.root_conf)

    def _load_legistar_data(self) -> RGuidLegistarData:
        return RGuidLegistarData.load(self.local_data_file)

    def _build_cache_key(self, guid: REAL_GUID, uid: LegistarFileUID) -> MetaCacheKey:
        return ('legistar_rguid', guid, uid)

    def get_remote_counts(self) -> MetaCount:
        counts = self.meta_cache.get_counts()
        return counts.legistar_rguid


@logger.catch
async def upload_clips(
    clips: ClipCollection,
    root_conf: Config,
    max_clips: int,
    scheduler_limit: int,
) -> None:
    client = ClipGoogleClient(
        root_conf=root_conf,
        max_clips=max_clips,
        scheduler_limit=scheduler_limit,
    )
    try:
        async with client:
            await client.upload_all(clips)
    finally:
        client.save_cache()


@logger.catch
async def get_all_clip_file_meta(clips: ClipCollection, root_conf: Config) -> None:
    logger.info('Updating clip metadata from Drive...')

    async with ClipGoogleClient(root_conf=root_conf, max_clips=0) as client:
        schedulers = client.schedulers
        scheduler = schedulers['general']
        waiters: JobWaiters[bool] = JobWaiters(scheduler=scheduler)

        async def get_and_check_meta(clip: Clip, key: ClipFileKey, local_meta: ClipFileMeta) -> bool:
            meta, is_cached = await client.get_clip_file_meta(clip, key)
            if meta is None:
                return False
            if not is_cached:
                logger.debug(f'got meta for {clip.id}, {key}')
            filename = clip.get_file_path(key, absolute=True)
            assert filename.exists(), f'{filename=}'
            return not is_cached

        for clip in clips:
            for key in clip.files.path_attrs:
                local_meta = clip.files.get_metadata(key)
                if local_meta is None:
                    continue
                await waiters.spawn(get_and_check_meta(clip, key, local_meta))

        results = await waiters
        changed = any(results)
    if changed:
        logger.info(f'Meta changed, saving cache file')
        client.save_cache()
    else:
        logger.info('No changes detected')


@logger.catch(reraise=True)
async def check_clip_file_meta(
    clips: ClipCollection,
    root_conf: Config,
    check_hashes: bool,
    hash_logfile: Path|None = None
) -> None:
    async with ClipGoogleClient(root_conf=root_conf, max_clips=0) as client:
        changed = await client.check_meta(
            clips,
            enable_hashes=check_hashes,
            hash_logfile=hash_logfile,
        )
    if changed:
        logger.info(f'Meta changed, saving cache file')
        client.save_cache()
    else:
        logger.info('No changes detected')


async def build_html(clips: ClipCollection, html_dir: Path, root_conf: Config) -> str:
    await get_all_clip_file_meta(clips, root_conf=root_conf)
    client = ClipGoogleClient(root_conf=root_conf)

    def link_callback(clip: Clip, key: ClipFileKey) -> str|None:
        meta = client.find_cached_file_meta(('clips', clip.id, key))
        if meta is None:
            return None
        return meta.get('webViewLink')
    return html_builder.build_html(clips, html_dir, link_callback)


async def upload_legistar(
    root_conf: Config,
    max_clips: int
) -> None:
    client = LegistarGoogleClient(root_conf=root_conf, max_clips=max_clips)
    client.log_counts()
    async with client:
        await client.upload_all()
    client.save_cache()
    client.log_counts()

async def check_legistar_meta(root_conf: Config, check_hashes: bool) -> None:
    client = LegistarGoogleClient(root_conf=root_conf, max_clips=0)
    async with client:
        changed = await client.check_meta(enable_hashes=check_hashes)

    if changed:
        logger.info(f'Meta changed, saving cache file')
        client.save_cache()
    else:
        logger.info('No changes detected')

async def upload_legistar_rguid(
    root_conf: Config,
    max_clips: int
) -> None:
    client = RGuidLegistarGoogleClient(root_conf=root_conf, max_clips=max_clips)
    client.log_counts()
    async with client:
        await client.upload_all()
    client.save_cache()
    client.log_counts()

async def check_legistar_rguid_meta(root_conf: Config, check_hashes: bool) -> None:
    client = RGuidLegistarGoogleClient(root_conf=root_conf, max_clips=0)
    async with client:
        changed = await client.check_meta(enable_hashes=check_hashes)

    if changed:
        logger.info(f'Meta changed, saving cache file')
        client.save_cache()
    else:
        logger.info('No changes detected')

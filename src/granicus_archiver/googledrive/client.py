from __future__ import annotations
from typing import (
    TypeVar, Self, Coroutine, TypedDict, Literal, Any, cast, TYPE_CHECKING
)
import mimetypes
from pathlib import Path
import asyncio
import json

from aiogoogle.client import Aiogoogle
from loguru import logger
import aiojobs
import aiofile

from .types import *
from . import config
from ..model import ClipCollection, Clip, ClipFileKey, ClipFileUploadKey, CLIP_ID
from ..utils import JobWaiters
from .. import html_builder

if TYPE_CHECKING:
    from ..config import Config

_Rt = TypeVar('_Rt')

FOLDER_MTYPE = 'application/vnd.google-apps.folder'

FOLDER_CACHE: dict[Path, FileId] = {}
"""A cache of Drive folders and their id
"""


META_CACHE: dict[CLIP_ID, dict[ClipFileUploadKey, FileMetaFull]] = {}
"""Cache for uploaded file metadata
"""

CACHE_LOCK = asyncio.Lock()
CACHE_FILE = Path(__file__).resolve().parent / 'folder-cache.json'
META_CACHE_FILE = Path(__file__).resolve().parent / 'meta-cache.json'


UPLOAD_CHECK_LIMIT = 4

class SchedulersTD(TypedDict):
    general: aiojobs.Scheduler
    uploads: aiojobs.Scheduler
    upload_checks: aiojobs.Scheduler

SchedulerKey = Literal['general', 'uploads', 'upload_checks']


SCHEDULERS: SchedulersTD|None = None
def get_schedulers(limit: int|None = None) -> SchedulersTD:
    global SCHEDULERS
    if SCHEDULERS is None:
        if limit is None:
            raise RuntimeError('scheduler limits must be set')
        SCHEDULERS = {
            'general':aiojobs.Scheduler(limit=32),
            'uploads':aiojobs.Scheduler(limit=limit),
            'upload_checks':aiojobs.Scheduler(limit=UPLOAD_CHECK_LIMIT),
        }
    elif limit is not None:
        raise RuntimeError('schedulers already created')
    return SCHEDULERS


def get_scheduler(key: SchedulerKey) -> aiojobs.Scheduler:
    d = get_schedulers()
    return d[key]



def load_cache():
    if CACHE_FILE.exists():
        d = json.loads(CACHE_FILE.read_text())
        d = {Path(k): v for k,v in d.items()}
        FOLDER_CACHE.update(d)
    if META_CACHE_FILE.exists():
        d = json.loads(META_CACHE_FILE.read_text())
        META_CACHE.update(d)

def save_cache():
    d = {str(k):v for k,v in FOLDER_CACHE.items()}
    CACHE_FILE.write_text(json.dumps(d, indent=2))

    META_CACHE_FILE.write_text(json.dumps(META_CACHE, indent=2))


def find_folder_cache(folder: Path) -> tuple[list[FileId], Path]|None:
    """Search for cached Drive folders previously found by :func:`find_folder`

    Returns the longest matching path and id (similar to :func:`find_folder`)
    """
    assert not folder.is_absolute()
    f_ids = []
    parts = folder.parts
    p: Path|None = None
    # async with CACHE_LOCK:
    for i in range(len(parts)):
        _p = Path('/'.join(parts[:i+1]))
        if _p not in FOLDER_CACHE:
            break
        p = _p
        f_ids.append(FOLDER_CACHE[p])
    if p is not None and len(f_ids):
        assert len(f_ids) == len(p.parts)
        return f_ids, p

def cache_folder_parts(*parts_and_ids: tuple[str, FileId]):
    """Store Drive folder ids in the :attr:`FOLDER_CACHE`

    Each argument should be a tuple of the folder name and its id
    (for each folder level)
    """
    p = None
    for part, f_id in parts_and_ids:
        if p is None:
            p = Path(part)
        else:
            p = p / part
        FOLDER_CACHE[p] = f_id

def cache_single_folder(folder: Path, f_id: FileId):
    FOLDER_CACHE[folder] = f_id


def find_cached_file_meta(
    clip_id: CLIP_ID,
    key: ClipFileUploadKey
) -> FileMetaFull|None:
    """Search the cache for metadata by :attr:`.model.Clip.id` and file type
    """
    d = META_CACHE.get(clip_id)
    if d is None:
        return None
    meta = d.get(key)
    if meta is None or 'size' not in meta:
        return None
    return meta


def set_cached_file_meta(
    clip_id: CLIP_ID,
    key: ClipFileUploadKey,
    meta: FileMetaFull
) -> None:
    """Store metadata for the :attr:`.model.Clip.id` and file type in the cache
    """
    d = META_CACHE.setdefault(clip_id, {})
    d[key] = meta


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
    def __init__(self, root_conf: Config) -> None:
        self.root_conf = root_conf

    async def __aenter__(self) -> Self:
        self.aiogoogle = get_client(root_conf=self.root_conf)
        await self.aiogoogle.__aenter__()
        drive_v3 = await self.aiogoogle.discover("drive", "v3")
        self.drive_v3 = cast(DriveResource, drive_v3)
        return self

    async def __aexit__(self, *args) -> None:
        await self.aiogoogle.__aexit__(*args)

    async def as_user(self, *requests, resp_type: type[_Rt], full_res: bool = False) -> _Rt:
        """Send requests using :meth:`aiogoogle.client.Aiogoogle.as_user`
        casting their responses as *resp_type*
        """
        response = await self.aiogoogle.as_user(*requests, full_res=full_res)
        return cast(_Rt, response)

    async def get_file_list(self, *requests) -> FileListResponse[FileMeta]:
        """Get a :obj:`~.types.FileListResponse` containing
        :class:`~.types.FileMeta`
        """
        return await self.as_user(
            *requests,
            resp_type=FileListResponse[FileMeta],
        )

    async def get_file_list_full(self, *requests) -> FileListResponse[FileMetaFull]:
        """Get a :obj:`~.types.FileListResponse` containing
        :class:`~.types.FileMetaFull`
        """
        return await self.as_user(
            *requests,
            resp_type=FileListResponse[FileMetaFull], full_res=True,
        )

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
            q = f"trashed = false and mimeType = '{FOLDER_MTYPE}' and name = '{folder_part}'"
            if parent_id is not None:
                q = f"{q} and '{parent_id}' in parents"
            # logger.debug(f'{q=}')
            req = self.drive_v3.files.list(
                q=q,
                spaces='drive',
            )
            found_fids: list[FileId] = []
            found_parts: list[str] = []
            res = await self.get_file_list_full(req)

            async for page in res:
                for f in page['files']:
                    # logger.debug(f'{parent_id=}, {f.get("id")=}, {f.get("name")=}')
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
                break
            if len(found_fids):
                return found_fids
            return None

        parts = folder.parts
        folder_ids: list[FileId]|None = None
        cached = find_folder_cache(folder)
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
            cache_folder_parts(*zip(found_parts, folder_ids))
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
        file_meta: FileMeta = {
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
            use_cache: If True, stores the results in the :attr:`FOLDER_CACHE`.
                *parent_path* must be provided to accurately store the results

        """
        prev_parent = parent
        full_path = parent_path
        for part in parts:
            prev_parent = await self.create_folder(part, prev_parent)
            if use_cache and full_path is not None:
                full_path = full_path / part
                cache_single_folder(full_path, prev_parent)
        assert prev_parent is not None
        return prev_parent


    async def create_folder_from_path(
        self,
        folder: Path
    ) -> FileId:
        """Find or create a (possibly nested) Drive folder with the given path
        """
        async with CACHE_LOCK:
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
        q = f"trashed = false and mimeType != '{FOLDER_MTYPE}' and name = '{filename}'"
        if parent_id is not None:
            q = f"{q} and '{parent_id}' in parents"
        # logger.debug(f'{q=}')
        req = self.drive_v3.files.list(
            q=q,
            spaces='drive',
        )
        res = await self.get_file_list_full(req)
        async for page in res:
            for f in page['files']:
                return True
        return False

    async def get_file_meta(
        self,
        filename: Path
    ) -> FileMetaFull|None:
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
        req = self.drive_v3.files.list(
            q=q,
            spaces='drive',
            fields='*',
            # fields=['id', 'size', 'name'],
        )
        res = await self.get_file_list_full(req)
        async for page in res:
            for f in page['files']:
                return f

    async def stream_upload_file(
        self,
        local_file: Path,
        upload_filename: Path,
        check_exists: bool = True,
        folder_id: FileId|None = None
    ) -> FileMetaFull|None:
        chunk_size = 64*1024

        if folder_id is not None:
            parent = folder_id
        else:
            path_parts = upload_filename.parts
            if len(path_parts) > 1:
                parent = await self.create_folder_from_path(upload_filename.parent)
            else:
                parent = None

        file_meta: FileMeta = {
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
            chunk_iter = src_fd.iter_chunked(chunk_size)
            req = self.drive_v3.files.create(
                pipe_from=chunk_iter,
                fields="id",
                json=file_meta,
            )
            upload_res = await self.as_user(req, resp_type=FileUploadResponse)
            logger.debug(f"Upload complete for {upload_filename}. File ID: {upload_res['id']}")
        uploaded_meta = await self.get_file_meta(upload_filename)
        assert uploaded_meta is not None
        return uploaded_meta


class ClipGoogleClient(GoogleClient):

    async def upload_clip(
        self,
        clip: Clip,
        upload_dir: Path
    ) -> bool:
        """Upload all assets for the given clip
        """
        waiter = JobWaiters[bool](scheduler=get_scheduler('uploads'))
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
            )
            if meta is not None:
                set_cached_file_meta(clip.id, key, meta)
                return True
            return False

        for key, filename in clip.iter_paths(for_download=False):
            if not filename.exists():
                continue
            rel_filename = clip.get_file_path(key, absolute=False)
            upload_filename = upload_dir / rel_filename
            if drive_folder_id is None:
                drive_folder_id = await self.create_folder_from_path(
                    upload_filename.parent
                )
            await waiter.spawn(_do_upload(
                key, filename, upload_filename, drive_folder_id,
            ))
            num_jobs += 1

        if not len(waiter):
            return True
        results = await waiter
        all_skipped = True not in results
        logger.success(f'Upload complete for clip "{clip.unique_name}"')
        return all_skipped


    async def check_clip_needs_upload(
        self,
        clip: Clip,
        upload_dir: Path
    ) -> bool:
        """Check if the given clip has any assets that need to be uploaded
        """
        async def do_check(key: ClipFileUploadKey, filename: Path) -> bool:
            upload_meta = find_cached_file_meta(clip.id, key)
            if upload_meta is None:
                rel_filename = clip.get_file_path(key, absolute=False)
                upload_filename = upload_dir / rel_filename
                upload_meta = await self.get_file_meta(upload_filename)
            if upload_meta is None:
                return True
            assert 'size' in upload_meta
            local_size = filename.stat().st_size
            upload_size = int(upload_meta['size'])
            if upload_size != local_size:
                logger.warning(f'size mismatch for "{upload_filename}", {local_size=}, {upload_size=}')
            return False

        coros: set[Coroutine[Any, Any, bool]] = set()
        for key, filename in clip.iter_paths(for_download=False):
            if not filename.exists():
                continue
            coros.add(do_check(key, filename))
        if len(coros):
            results = await asyncio.gather(*coros)
            if any(results):
                return True
        return False


@logger.catch
async def upload_clips(
    clips: ClipCollection,
    root_conf: Config,
    max_clips: int,
    scheduler_limit: int,
) -> None:
    upload_dir = root_conf.google.drive_folder
    load_cache()
    schedulers = get_schedulers(scheduler_limit)
    upload_check_waiters = JobWaiters(scheduler=get_scheduler('upload_checks'))
    upload_clip_waiters = JobWaiters[bool](scheduler=get_scheduler('general'))

    # Using a dict here for nested context mutability
    tracking_vars = {'num_uploaded': 0}

    async with ClipGoogleClient(root_conf=root_conf) as client:

        async def upload_clip_if_needed(clip: Clip):
            if tracking_vars['num_uploaded'] >= max_clips:
                return False
            needs_upload = await client.check_clip_needs_upload(clip, upload_dir)
            if not needs_upload or tracking_vars['num_uploaded'] >= max_clips:
                return False
            tracking_vars['num_uploaded'] += 1
            logger.info(f'Upload clip {clip.unique_name}')
            await upload_clip_waiters.spawn(client.upload_clip(clip, upload_dir))
            return needs_upload

        for clip in clips:
            await upload_check_waiters.spawn(upload_clip_if_needed(clip))
            if tracking_vars['num_uploaded'] >= max_clips:
                break

        if len(upload_check_waiters):
            logger.info(f'waiting for check_waiters ({len(upload_check_waiters)=})')
            await upload_check_waiters

        if len(upload_clip_waiters):
            logger.info(f'waiting for waiters ({len(upload_clip_waiters)=})')
            await upload_clip_waiters

        logger.info('closing schedulers..')
        scheduler_list: list[aiojobs.Scheduler] = [schedulers[key] for key in schedulers.keys()]
        await asyncio.gather(*[sch.close() for sch in scheduler_list])
    save_cache()


@logger.catch
async def get_all_clip_file_meta(clips: ClipCollection, root_conf: Config) -> None:
    upload_dir = root_conf.google.drive_folder
    logger.info('Updating clip metadata from Drive...')
    load_cache()
    schedulers = get_schedulers(limit=8)
    scheduler = schedulers['general']
    waiters: JobWaiters[bool] = JobWaiters(scheduler=scheduler)
    async with ClipGoogleClient(root_conf=root_conf) as client:

        async def get_meta(clip: Clip, key: ClipFileKey) -> bool:
            rel_filename = clip.get_file_path(key, absolute=False)
            upload_filename = upload_dir / rel_filename
            logger.debug(f'getting meta for {clip.id}, {key}')
            meta = await client.get_file_meta(upload_filename)
            if meta is not None:
                logger.debug(f'got meta for {clip.id}, {key}')
                set_cached_file_meta(clip.id, key, meta)
                return True
            return False

        for clip in clips:
            for key in clip.files.path_attrs:
                local_meta = clip.files.get_metadata(key)
                if local_meta is None:
                    continue
                meta = find_cached_file_meta(clip.id, key)
                if meta is not None:
                    continue
                await waiters.spawn(get_meta(clip, key))

        results = await waiters
        changed = any(results)
        await scheduler.close()
    if changed:
        logger.info(f'Meta changed, saving cache file')
        save_cache()

async def build_html(clips: ClipCollection, html_dir: Path, root_conf: Config) -> str:
    await get_all_clip_file_meta(clips, root_conf=root_conf)

    def link_callback(clip: Clip, key: ClipFileKey) -> str|None:
        meta = find_cached_file_meta(clip.id, key)
        if meta is None:
            return None
        return meta.get('webViewLink')
    return html_builder.build_html(clips, html_dir, link_callback)

from __future__ import annotations
from typing import Coroutine, Any, cast
import mimetypes
from pathlib import Path
import asyncio
import json

from aiogoogle.client import Aiogoogle
from loguru import logger
import aiojobs
import aiofile

from .types import FileId, FileMeta, DriveResource
from . import config
from ..model import ClipCollection, Clip, ClipFileKey
from ..utils import JobWaiters

FOLDER_MTYPE = 'application/vnd.google-apps.folder'

FOLDER_CACHE: dict[Path, FileId] = {}
CACHE_LOCK = asyncio.Lock()
CACHE_FILE = Path(__file__).resolve().parent / 'folder-cache.json'


SCHEDULER: aiojobs.Scheduler|None = None
def get_scheduler() -> aiojobs.Scheduler:
    global SCHEDULER
    if SCHEDULER is None:
        SCHEDULER = aiojobs.Scheduler(limit=8)
    return SCHEDULER



def load_cache():
    if CACHE_FILE.exists():
        d = json.loads(CACHE_FILE.read_text())
        d = {Path(k): v for k,v in d.items()}
        FOLDER_CACHE.update(d)

def save_cache():
    d = {str(k):v for k,v in FOLDER_CACHE.items()}
    CACHE_FILE.write_text(json.dumps(d, indent=2))


def find_folder_cache(folder: Path) -> tuple[list[FileId], Path]|None:
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
    p = None
    for part, f_id in parts_and_ids:
        if p is None:
            p = Path(part)
        else:
            p = p / part
        FOLDER_CACHE[p] = f_id

def cache_single_folder(folder: Path, f_id: FileId):
    FOLDER_CACHE[folder] = f_id


def get_client() -> Aiogoogle:
    client_creds = config.OAuthClientConf.load_from_env()._asdict()
    user_creds = config.UserCredentials.load()._asdict()
    return Aiogoogle(user_creds=user_creds, client_creds=client_creds)   # type: ignore


async def find_folder(aiogoogle: Aiogoogle, drive_v3: DriveResource, folder: Path) -> tuple[FileId, Path]|None:
    # drive_v3 = await aiogoogle.discover('drive', 'v3')

    async def find_part(*folder_parts: str, parent_id: FileId|None) -> list[FileId]|None:
        folder_part = folder_parts[0]
        folders_remain = folder_parts[1:]
        q = f"trashed = false and mimeType = '{FOLDER_MTYPE}' and name = '{folder_part}'"
        if parent_id is not None:
            q = f"{q} and '{parent_id}' in parents"
        # logger.debug(f'{q=}')
        req = drive_v3.files.list(
            q=q,
            spaces='drive',
        )
        found_fids: list[FileId] = []
        found_parts: list[str] = []
        res = await aiogoogle.as_user(req, full_res=True)
        async for page in res:
            for f in page['files']:
                # logger.debug(f'{parent_id=}, {f.get("id")=}, {f.get("name")=}')
                f_id: FileId = f.get('id')
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


async def create_folder(aiogoogle: Aiogoogle, drive_v3: DriveResource, name: str, parent: FileId|None) -> FileId:
    # drive_v3 = await aiogoogle.discover('drive', 'v3')
    file_meta: FileMeta = {
        'name': name,
        'mimeType': FOLDER_MTYPE,
    }
    if parent is not None:
        file_meta['parents'] = [parent]
    logger.debug(f'create folder: {name=}, {parent=}')

    req = drive_v3.files.create(
        # body=file_meta,
        json=file_meta,
        fields="id",
    )
    res = await aiogoogle.as_user(req)
    # cache_single_folder()

    return res['id']

async def create_folder_nested(
    aiogoogle: Aiogoogle,
    drive_v3: DriveResource,
    *parts: str,
    parent: FileId|None,
    parent_path: Path|None = None,
    use_cache: bool = False
) -> FileId:
    prev_parent = parent
    full_path = parent_path
    for part in parts:
        prev_parent = await create_folder(aiogoogle, drive_v3, part, prev_parent)
        if use_cache and full_path is not None:
            full_path = full_path / part
            cache_single_folder(full_path, prev_parent)
    assert prev_parent is not None
    return prev_parent


async def create_folder_from_path(aiogoogle: Aiogoogle, drive_v3: DriveResource, folder: Path) -> FileId:
    async with CACHE_LOCK:
        find_result = await find_folder(aiogoogle, drive_v3, folder)
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
        return await create_folder_nested(
            aiogoogle, drive_v3, *path_parts, parent=parent,
            parent_path=parent_path, use_cache=use_cache,
        )

async def file_exists(aiogoogle: Aiogoogle, drive_v3: DriveResource, filename: str, parent_id: FileId|None) -> bool:
    q = f"trashed = false and mimeType != '{FOLDER_MTYPE}' and name = '{filename}'"
    if parent_id is not None:
        q = f"{q} and '{parent_id}' in parents"
    # logger.debug(f'{q=}')
    req = drive_v3.files.list(
        q=q,
        spaces='drive',
    )
    res = await aiogoogle.as_user(req, full_res=True)
    async for page in res:
        for f in page['files']:
            return True
    return False

async def get_file_meta(aiogoogle: Aiogoogle, drive_v3: DriveResource, filename: Path) -> FileMeta|None:
    folder = filename.parent
    folder_find = await find_folder(aiogoogle, drive_v3, folder)
    if folder_find is None:
        return None
    folder_id, folder_path = folder_find
    if folder_path != folder:
        return None
    q = f"trashed = false and mimeType != '{FOLDER_MTYPE}' and name = '{filename.name}'"
    q = f"{q} and '{folder_id}' in parents"
    # logger.debug(f'{q=}')
    req = drive_v3.files.list(
        q=q,
        spaces='drive',
        fields='*',
        # fields=['id', 'size', 'name'],
    )
    res = await aiogoogle.as_user(req, full_res=True)
    async for page in res:
        for f in page['files']:
            return f



async def stream_upload_file(local_file: Path, upload_filename: Path):
    async with get_client() as aiogoogle:
        drive_v3 = cast(DriveResource, await aiogoogle.discover("drive", "v3"))
        await _stream_upload_file(aiogoogle, drive_v3, local_file, upload_filename)


async def _stream_upload_file(
    aiogoogle: Aiogoogle,
    drive_v3: DriveResource,
    local_file: Path,
    upload_filename: Path,
    check_exists: bool = True,
    folder_id: str|None = None
) -> bool:
    chunk_size = 64*1024

    if folder_id is not None:
        parent = folder_id
    else:
        path_parts = upload_filename.parts
        if len(path_parts) > 1:
            parent = await create_folder_from_path(aiogoogle, drive_v3, upload_filename.parent)
        else:
            parent = None

    file_meta: FileMeta = {
        'name': upload_filename.name,
    }
    if parent is not None:
        file_meta['parents'] = [parent]
    if check_exists:
        exists = await file_exists(aiogoogle, drive_v3, upload_filename.name, parent)
        if exists:
            logger.debug(f'file "{upload_filename}" exists')
            return False
    logger.info(f'uploading "{upload_filename}"')
    async with aiofile.async_open(local_file, 'rb') as src_fd:
        chunk_iter = src_fd.iter_chunked(chunk_size)
        req = drive_v3.files.create(
            pipe_from=chunk_iter,
            fields="id",
            json=file_meta,
        )
        upload_res = await aiogoogle.as_user(req)
        logger.success(f"Uploaded complete for {upload_filename}. File ID: {upload_res['id']}")
    return True


async def upload_clip(aiogoogle: Aiogoogle, drive_v3: DriveResource, clip: Clip, upload_dir: Path) -> bool:
    waiter = JobWaiters[bool](scheduler=get_scheduler())
    num_jobs = 0
    drive_folder_id: str|None = None

    for key, url, filename in clip.iter_url_paths():
        if not filename.exists():
            continue
        rel_filename = clip.get_file_path(key, absolute=False)
        upload_filename = upload_dir / rel_filename
        if drive_folder_id is None:
            drive_folder_id = await create_folder_from_path(
                aiogoogle, drive_v3, upload_filename.parent
            )
        await waiter.spawn(_stream_upload_file(
            aiogoogle, drive_v3, filename, upload_filename, folder_id=drive_folder_id,
        ))
        num_jobs += 1
    results = await waiter
    all_skipped = True not in results
    return all_skipped


async def check_clip_needs_upload(aiogoogle: Aiogoogle, drive_v3: DriveResource, clip: Clip, upload_dir: Path) -> bool:
    async def do_check(key: ClipFileKey, filename: Path) -> bool:
        rel_filename = clip.get_file_path(key, absolute=False)
        upload_filename = upload_dir / rel_filename
        upload_meta = await get_file_meta(aiogoogle, drive_v3, upload_filename)
        if upload_meta is None:
            return True
        assert 'size' in upload_meta
        local_size = filename.stat().st_size
        upload_size = int(upload_meta['size'])
        if upload_size != local_size:
            logger.warning(f'size mismatch for "{upload_filename}", {local_size=}, {upload_size=}')
        return False

    coros: set[Coroutine[Any, Any, bool]] = set()
    for key, url, filename in clip.iter_url_paths():
        if not filename.exists():
            continue
        coros.add(do_check(key, filename))
    if len(coros):
        results = await asyncio.gather(*coros)
        if any(results):
            return True
    return False


@logger.catch
async def upload_clips(clips: ClipCollection, upload_dir: Path, max_clips: int):
    load_cache()
    scheduler = get_scheduler()
    assert scheduler.limit is not None
    waiter = JobWaiters[bool](scheduler=scheduler)
    async with get_client() as aiogoogle:
        drive_v3 = cast(DriveResource, await aiogoogle.discover("drive", "v3"))
        num_uploaded = 0
        for clip in clips:
            needs_upload = await check_clip_needs_upload(aiogoogle, drive_v3, clip, upload_dir)
            if not needs_upload:
                logger.debug(f'skipping {clip.unique_name}')
                continue
            logger.debug(f'Upload clip {clip.unique_name}')
            await waiter.spawn(upload_clip(aiogoogle, drive_v3, clip, upload_dir))
            num_uploaded += 1
            if num_uploaded >= max_clips:
                break
            # if len(waiter) >= 4:
            #     done, pending = await waiter.wait()
            #     # _done_count = 0
            #     for job_result in done:
            #         if job_result.exception:
            #             raise job_result.exception
            #         if job_result.result:
            #             num_uploaded += 1
            #             if num_uploaded >= max_clips:
            #                 break
            #     done_results = [jr.result for jr in done]
            #     logger.info(f'{done_results=}')
            # num_iter += 1
            # if num_iter >= max_iter:
            #     logger.warning('exiting from max_iter')
            #     break

        logger.debug(f'waiting for waiters ({len(waiter)=})')
        results = await waiter
        # logger.info(f'{results=}')

    save_cache()

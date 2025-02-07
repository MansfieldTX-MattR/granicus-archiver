from __future__ import annotations
from typing import (
    TypeVar, Generic, Iterator, AsyncGenerator, Literal, Self, TypedDict,
    cast, overload, TYPE_CHECKING
)
from abc import ABC, abstractmethod
from pathlib import Path
import mimetypes
from urllib.parse import urlencode
import asyncio
import sys

from loguru import logger

import aiojobs
import aioboto3
import botocore.exceptions
from yarl import URL

if TYPE_CHECKING or 'sphinx.ext.autodoc' in sys.modules:
    from types_aiobotocore_s3 import S3ServiceResource, S3Client
    from types_aiobotocore_s3.service_resource import Bucket, Object, ObjectSummary

else:
    S3ServiceResource = object
    S3Client = object
    Bucket = object
    Object = object
    ObjectSummary = object



from ..config import Config
from ..model import FileMeta, Clip, ClipFileUploadKey, ClipCollection
from ..legistar.types import GUID, REAL_GUID, LegistarFileUID, _GuidT, _ItemT
from ..legistar.model import AbstractLegistarModel, LegistarData, DetailPageResult
from ..legistar.guid_model import RGuidDetailResult, RGuidLegistarData
from ..utils import (
    CompletionCounts, JobWaiters,
    HashMismatchError, SHA1Hash, get_file_hash_async
)

Key = str|Path
ACL = Literal[
    'private', 'public-read', 'public-read-write', 'authenticated-read',
    'aws-exec-read', 'bucket-owner-read', 'bucket-owner-full-control'
]

class SchedulersTD(TypedDict):
    general: aiojobs.Scheduler
    clip_uploads: aiojobs.Scheduler
    uploads: aiojobs.Scheduler
    upload_checks: aiojobs.Scheduler


def _key_to_str(key: Key) -> str:
    if isinstance(key, Path):
        assert not key.is_absolute()
        return str(key)
    return key


class ClientBase:
    """Base class for AWS clients
    """
    session: aioboto3.Session
    """The boto3 session"""
    s3_resource: S3ServiceResource
    """The S3 service resource"""
    s3_client: S3Client
    """The S3 client"""
    bucket: Bucket
    """The S3 bucket"""
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session = aioboto3.Session()
        self._is_open = False

    async def __aenter__(self) -> Self:
        self._is_open = True
        ctx = self.session.resource('s3')
        self.s3_resource = cast(S3ServiceResource, await ctx.__aenter__())
        ctx = self.session.client('s3')
        self.s3_client = cast(S3Client, await ctx.__aenter__()) # type: ignore
        self.bucket = await self.s3_resource.Bucket(self.config.aws.bucket_name)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._is_open:
            await self.s3_resource.__aexit__(exc_type, exc_val, exc_tb)
            await self.s3_client.__aexit__(exc_type, exc_val, exc_tb)
        self._is_open = False

    def url_for_key(self, key: Key, scheme: str|None = None) -> URL:
        """Get a URL for an S3 key within :attr:`bucket`
        """
        url = URL(f'https://s3.amazonaws.com/{self.config.aws.bucket_name}/{key}')
        if scheme is not None:
            url = url.with_scheme(scheme)
        return url

    async def get_object(self, key: Key) -> Object:
        """Get an S3 object within :attr:`bucket` by key
        """
        key = _key_to_str(key)
        obj = await self.bucket.Object(key)
        await obj.load()
        return obj

    async def iter_objects(self, prefix: Path|str) -> AsyncGenerator[ObjectSummary, None]:
        """Iterate over objects in :attr:`bucket` with a given (optional) prefix
        """
        if isinstance(prefix, Path):
            assert not prefix.is_absolute()
            prefix = f'{prefix}/'
        async for obj in self.bucket.objects.filter(Prefix=prefix):
            yield obj

    async def object_exists(self, key: Key) -> bool:
        """Check if an object exists in :attr:`bucket` by key
        """
        try:
            await self.get_object(key)
            return True
        except botocore.exceptions.ClientError as exc:
            if exc.response.get('Error', {}).get('Code') == '404':
                return False
            raise

    async def get_object_tags(self, obj: Object|Key) -> dict[str, str]:
        """Get the tags for an S3 object
        """
        key = _key_to_str(obj) if isinstance(obj, (Path, str)) else obj.key
        tag_set = await self.s3_client.get_object_tagging(
            Bucket=self.bucket.name,
            Key=key,
        )
        tags: dict[str, str] = {}
        for tag in tag_set['TagSet']:
            tags[tag['Key']] = tag['Value']
        return tags

    async def get_object_sha1(self, obj: Object|Key) -> SHA1Hash|None:
        """Get the SHA1 hash for an S3 object
        """
        tags = await self.get_object_tags(obj)
        if 'SHA1' not in tags:
            return None
        return SHA1Hash(tags['SHA1'])

    async def check_object_hash(self, obj: Object|Key, local_hash: SHA1Hash) -> None:
        """Check the SHA1 hash of an S3 object against a local hash

        Raises:
            HashMismatchError: If the hashes do not match
        """
        remote_hash = await self.get_object_sha1(obj)
        if remote_hash != local_hash:
            raise HashMismatchError(
                f'Hash mismatch for {obj}',
                local_hash,
                remote_hash,
            )

    @overload
    async def upload(
        self,
        key: Key,
        local_filename: Path,
        wait_exists: bool = True,
        check_exists: bool = ...,
        local_meta: FileMeta|None = ...,
        acl: ACL|None = ...,
    ) -> Object:
        ...
    @overload
    async def upload(
        self,
        key: Key,
        local_filename: Path,
        wait_exists: bool = ...,
        check_exists: bool = ...,
        local_meta: FileMeta|None = ...,
        acl: ACL|None = ...,
    ) -> Object:
        ...
    @overload
    async def upload(
        self,
        key: Key,
        local_filename: Path,
        wait_exists: bool = False,
        check_exists: bool = ...,
        local_meta: FileMeta|None = ...,
        acl: ACL|None = ...,
    ) -> Key:
        ...
    async def upload(
        self,
        key: Key,
        local_filename: Path,
        wait_exists: bool = True,
        check_exists: bool = True,
        local_meta: FileMeta|None = None,
        acl: ACL|None = None,
    ) -> Object|Key:
        """Upload a file to S3

        Arguments:
            key: The S3 key to upload the file to
            local_filename: The local file to upload
            wait_exists: If ``True``, wait for the object to exist before returning
            check_exists: If ``True``, check if the object exists before uploading
            local_meta: The metadata for the local file
            acl: The ACL for the uploaded object

        """
        async def get_local_hash() -> SHA1Hash:
            if local_meta is not None and local_meta.sha1 is not None:
                return local_meta.sha1
            return await get_file_hash_async(local_filename, 'sha1')

        key = _key_to_str(key)
        if check_exists and await self.object_exists(key):
            logger.warning(f'Object already exists: {key}')
            if wait_exists:
                return await self.get_object(key)
            return key
        mt = mimetypes.guess_type(local_filename)[0]
        assert mt is not None
        local_hash = await get_local_hash()
        tags = {
            'Content-Type': mt,
            'SHA1': local_hash,
        }
        extra_args = {
            'ContentType': mt,
            'Tagging': urlencode(tags),
        }
        if acl is not None:
            extra_args['ACL'] = acl
        await self.bucket.upload_file(
            Filename=str(local_filename),
            Key=key,
            ExtraArgs=extra_args,
        )
        if wait_exists:
            waiter = self.s3_client.get_waiter('object_exists')
            await waiter.wait(
                Bucket=self.bucket.name,
                Key=key,
            )
            obj = await self.get_object(key)
            return obj
        return key

    async def download_object(
        self,
        key: Key,
        local_filename: Path
    ) -> None:
        """Download an S3 object to a local file
        """
        key = _key_to_str(key)
        await self.bucket.download_file(
            Key=key,
            Filename=str(local_filename),
        )


class ClipClient(ClientBase):
    """AWS Client to upload items from a :class:`.model.ClipCollection`
    """
    def __init__(
        self,
        config: Config,
        max_clips: int = 8,
        scheduler_limit: int = 8,
    ) -> None:
        super().__init__(config)
        self.max_clips = max_clips
        self.scheduler_limit = scheduler_limit
        self.completion_counts = CompletionCounts(max_clips, enable_log=True)

    @property
    def upload_dir(self) -> Path:
        """Root folder name (key prefix) for uploaded clips
        (alias for :attr:`.config.AWSConfig.clips_prefix`)
        """
        return self.config.aws.clips_prefix

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        self.schedulers: SchedulersTD = {
            'general': await aiojobs.create_scheduler(),
            'clip_uploads': await aiojobs.create_scheduler(limit=16, pending_limit=1),
            'uploads': await aiojobs.create_scheduler(limit=self.scheduler_limit),
            'upload_checks': await aiojobs.create_scheduler(limit=16, pending_limit=1),
        }
        self.upload_check_waiters = JobWaiters[tuple[bool, Clip, set[ClipFileUploadKey]]](
            scheduler=self.schedulers['upload_checks'],
        )
        self.upload_clip_waiters = JobWaiters[bool](
            scheduler=self.schedulers['clip_uploads'],
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        logger.info('closing schedulers..')
        scheduler_list: list[aiojobs.Scheduler] = [
            self.schedulers[key] for key in self.schedulers.keys()
        ]
        await asyncio.gather(*[sch.close() for sch in scheduler_list])
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    def get_clip_file_upload_path(self, clip: Clip, key: ClipFileUploadKey) -> Path:
        """Get the uploaded filename for a clip asset (relative to :attr:`upload_dir`)
        """
        rel_filename = clip.get_file_path(key, absolute=False)
        return self.upload_dir / rel_filename

    async def upload_data_file(self) -> None:
        """Upload the clips data file to S3
        """
        local_file = self.config.data_file
        remote_file = self.upload_dir / local_file.name
        exists = await self.object_exists(remote_file)
        if exists:
            remote_hash = await self.get_object_sha1(remote_file)
            local_hash = await get_file_hash_async(local_file, 'sha1')
            if remote_hash == local_hash:
                logger.info(f'Data file matches hosted version: {remote_file}')
                return
        logger.info(f'Uploading {local_file} to {remote_file}')
        await self.upload(
            key=remote_file,
            local_filename=local_file,
            check_exists=False,
        )

    @logger.catch(reraise=True)
    async def upload_clip(self, clip: Clip, *file_keys: ClipFileUploadKey) -> bool:
        """Upload all assets for the given clip
        """
        waiter = JobWaiters[bool](scheduler=self.schedulers['uploads'])
        num_jobs = 0

        async def _do_upload(
            key: ClipFileUploadKey,
            local_file: Path,
            remote_file: Path
        ) -> bool:
            logger.debug(f'uploading {local_file} to {remote_file}')
            local_meta = clip.files.get_metadata(key)
            assert local_meta is not None
            try:
                await self.upload(
                    key=remote_file,
                    local_filename=local_file,
                    check_exists=True,
                    wait_exists=False,
                    local_meta=local_meta,
                    acl='public-read',
                )
                return True
            except botocore.exceptions.ClientError as exc:
                logger.error(f'Failed to upload {local_file}: {exc}')
                return False

        for file_key, local_file in clip.iter_paths(for_download=False):
            if len(file_keys) and file_key not in file_keys:
                continue
            if not local_file.exists():
                continue
            remote_file = self.get_clip_file_upload_path(clip, file_key)
            await waiter.spawn(_do_upload(file_key, local_file, remote_file))
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
    async def check_clip_needs_upload(self, clip: Clip) -> tuple[bool, Clip, set[ClipFileUploadKey]]:
        """Check if the given clip hash any assets that need to be uploaded
        """
        remote_files: dict[Path, ClipFileUploadKey] = {}
        for file_key, filename in clip.iter_paths(for_download=False):
            if not filename.exists():
                continue
            remote_file = self.get_clip_file_upload_path(clip, file_key)
            remote_files[remote_file] = file_key
        parent_dirs = {f.parent for f in remote_files.keys()}
        assert len(parent_dirs) == 1, f'{parent_dirs=}'
        parent_dir = parent_dirs.pop()
        async for obj_summary in self.iter_objects(parent_dir):
            remote_file = Path(obj_summary.key)
            if remote_file in remote_files:
                del remote_files[remote_file]
        return len(remote_files) > 0, clip, set(remote_files.values())

    async def handle_upload_check_jobs(self) -> None:
        """Wait for jobs from :meth:`check_clip_needs_upload` and spawns their
        :meth:`upload_clip` jobs
        """
        if self.completion_counts.full:
            return
        async for job in self.upload_check_waiters:
            if job.exception is not None:
                raise job.exception
            needs_upload, clip, file_keys = job.result
            if not needs_upload:
                continue
            await self.upload_clip_waiters.spawn(self.upload_clip(clip, *file_keys))
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

        if len(self.upload_clip_waiters):
            logger.info(f'waiting for waiters ({len(self.upload_clip_waiters)=})')
            await self.upload_clip_waiters
        await self.upload_data_file()
        logger.success(f'all waiters finished')




_ModelT = TypeVar('_ModelT', bound=AbstractLegistarModel)

class LegistarClientBase(ClientBase, Generic[_GuidT, _ItemT, _ModelT], ABC):
    max_clips: int
    """The maximum number of items to upload"""
    legistar_data: _ModelT
    """A :class:~.legistar.model.AbstractLegistarModel` instance"""
    item_scheduler: aiojobs.Scheduler
    check_scheduler: aiojobs.Scheduler
    upload_scheduler: aiojobs.Scheduler

    def __init__(
        self,
        config: Config,
        max_clips: int,
        legistar_data: _ModelT|None = None
    ) -> None:
        super().__init__(config)
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
    def legistar_data_file(self) -> Path: ...

    @abstractmethod
    def _load_legistar_data(self) -> _ModelT: ...

    @abstractmethod
    def iter_item_guids(self) -> Iterator[_GuidT]: ...

    async def __aenter__(self) -> Self:
        self.item_scheduler = aiojobs.Scheduler(limit=16)
        self.check_scheduler = aiojobs.Scheduler(limit=16, pending_limit=1)
        self.upload_scheduler = aiojobs.Scheduler(limit=32)
        self.check_waiters = JobWaiters[tuple[bool, _GuidT, set[LegistarFileUID]]](
            scheduler=self.check_scheduler
        )
        self.item_waiters = JobWaiters[bool](scheduler=self.item_scheduler)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.check_scheduler.wait_and_close()
        await self.item_scheduler.wait_and_close()
        await self.upload_scheduler.wait_and_close()
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    def get_file_upload_path(self, guid: _GuidT, uid: LegistarFileUID) -> Path:
        """Get the uploaded filename for a legistar asset (relative to :attr:`upload_dir`)
        """
        filename, _ = self.legistar_data.get_path_for_uid(guid, uid)
        assert not filename.is_absolute()
        filename = filename.relative_to(self.legistar_data.root_dir)
        return self.upload_dir / filename

    async def upload_data_file(self) -> None:
        """Upload the legistar data file to S3
        """
        local_file = self.legistar_data_file
        remote_file = self.upload_dir / local_file.name
        exists = await self.object_exists(remote_file)
        if exists:
            remote_hash = await self.get_object_sha1(remote_file)
            local_hash = await get_file_hash_async(local_file, 'sha1')
            if remote_hash == local_hash:
                logger.info(f'Data file matches hosted version: {remote_file}')
                return
        logger.info(f'Uploading {local_file} to {remote_file}')
        await self.upload(
            key=remote_file,
            local_filename=local_file,
            check_exists=False,
        )

    async def upload_legistar_file(
        self,
        guid: _GuidT,
        uid: LegistarFileUID,
        local_file: Path,
        local_meta: FileMeta,
    ) -> bool:
        """Upload a single legistar file
        """
        remote_file = self.get_file_upload_path(guid, uid)
        try:
            await self.upload(
                key=remote_file,
                local_filename=local_file,
                check_exists=True,
                local_meta=local_meta,
                acl='public-read',
            )
            return True
        except botocore.exceptions.ClientError as exc:
            logger.error(f'Failed to upload {local_file}: {exc}')
            return False

    async def upload_legistar_item(self, guid: _GuidT, *uids: LegistarFileUID) -> bool:
        """Upload all assets for the legistar item matching the given *guid*
        """
        waiter = JobWaiters[bool](scheduler=self.upload_scheduler)
        num_jobs = 0
        it = self.legistar_data.iter_files_for_upload(guid)
        for uid, local_file, local_meta, is_attachment in it:
            if len(uids) and uid not in uids:
                continue
            if not local_file.exists():
                continue
            await waiter.spawn(
                self.upload_legistar_file(guid, uid, local_file, local_meta)
            )
            num_jobs += 1
        if not len(waiter):
            return True
        results = await waiter
        all_skipped = True not in results
        logger.success(f'Upload complete for Legistar item "{guid}"')
        self.completion_counts.num_completed += 1
        return all_skipped

    async def check_item_needs_upload(self, guid: _GuidT) -> tuple[bool, _GuidT, set[LegistarFileUID]]:
        """Check if the item matching *guid* has any local files that need to
        be uploaded
        """
        remote_files: dict[Path, LegistarFileUID] = {}
        for uid, local_file, local_meta, is_attachment in self.legistar_data.iter_files_for_upload(guid):
            if not local_file.exists():
                continue
            remote_file = self.get_file_upload_path(guid, uid)
            remote_files[remote_file] = uid
        if not len(remote_files):
            return False, guid, set()
        parent_dirs = {f.parent for f in remote_files.keys()}

        # attachments use a subdirectory
        for parent_dir in parent_dirs:
            async for obj_summary in self.iter_objects(parent_dir):
                remote_file = Path(obj_summary.key)
                if remote_file in remote_files:
                    del remote_files[remote_file]
        return len(remote_files) > 0, guid, set(remote_files.values())

    async def handle_upload_check_jobs(self) -> None:
        """Wait for jobs from :meth:`check_item_needs_upload` and spawns their
        :meth:`upload_legistar_item` jobs
        """
        if self.completion_counts.full:
            return
        async for job in self.check_waiters:
            if job.exception is not None:
                raise job.exception
            needs_upload, guid, uids = job.result
            if not needs_upload:
                continue
            await self.item_waiters.spawn(self.upload_legistar_item(guid, *uids))
            self.completion_counts.num_queued += 1
            if self.completion_counts.full:
                break

    async def upload_all(self) -> None:
        """Upload files for all items in :attr:`legistar_data` (up to by :attr:`max_clips`)
        """
        upload_check_sch = self.check_scheduler
        upload_check_limit = upload_check_sch.limit
        assert upload_check_limit is not None
        for guid in self.iter_item_guids():
            await self.check_waiters.spawn(self.check_item_needs_upload(guid))
            if len(self.check_waiters) >= upload_check_limit:
                await self.handle_upload_check_jobs()
            if self.completion_counts.full:
                break

        await self.handle_upload_check_jobs()
        if len(self.check_waiters):
            logger.info(f'waiting for check_waiters ({len(self.check_waiters)=})')
            await self.check_waiters

        if len(self.item_waiters):
            logger.info(f'waiting for waiters ({len(self.item_waiters)=})')
            await self.item_waiters
        await self.upload_data_file()
        logger.success(f'all waiters finished')


class LegistarClient(LegistarClientBase[GUID, DetailPageResult, LegistarData]):
    """Client to upload items from :class:`~.legistar.model.LegistarData`
    """
    @property
    def upload_dir(self) -> Path:
        return self.config.aws.legistar_prefix

    @property
    def legistar_data_file(self) -> Path:
        return self.config.legistar.data_file

    def _load_legistar_data(self) -> LegistarData:
        return LegistarData.load(self.legistar_data_file)

    def iter_item_guids(self) -> Iterator[GUID]:
        yield from self.legistar_data.keys()


class RGuidLegistarClient(LegistarClientBase[REAL_GUID, RGuidDetailResult, RGuidLegistarData]):
    """Client to upload items from :class:`~.legistar.guid_model.RGuidLegistarData`
    """
    @property
    def upload_dir(self) -> Path:
        return self.config.aws.legistar_rguid_prefix

    @property
    def legistar_data_file(self) -> Path:
        # return self.config.legistar.
        return RGuidLegistarData._get_data_file(self.config)

    def _load_legistar_data(self) -> RGuidLegistarData:
        return RGuidLegistarData.load(self.legistar_data_file)

    def iter_item_guids(self) -> Iterator[REAL_GUID]:
        yield from self.legistar_data.keys()




@logger.catch
async def upload_clips(
    clips: ClipCollection,
    config: Config,
    max_clips: int,
    scheduler_limit: int,
) -> None:
    client = ClipClient(
        config=config,
        max_clips=max_clips,
        scheduler_limit=scheduler_limit,
    )
    async with client:
        await client.upload_all(clips)

@logger.catch
async def upload_legistar(
    config: Config,
    max_clips: int,
) -> None:
    client = LegistarClient(
        config=config,
        max_clips=max_clips,
    )
    async with client:
        await client.upload_all()

@logger.catch
async def upload_legistar_rguid(
    config: Config,
    max_clips: int,
) -> None:
    client = RGuidLegistarClient(
        config=config,
        max_clips=max_clips,
    )
    async with client:
        await client.upload_all()

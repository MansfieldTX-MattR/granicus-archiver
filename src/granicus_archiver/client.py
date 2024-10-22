from typing import TypeVar, TypedDict, Literal, Coroutine, Any
import asyncio
from pathlib import Path

from loguru import logger
import tempfile

from aiohttp import ClientSession, ClientTimeout, ServerTimeoutError
import aiojobs
import aiofile
from yarl import URL

from .config import Config
from .parser import parse_page, parse_player_page
from .model import (
    ClipCollection, Clip, ParseClipData, ParseClipLinks, ClipFileKey,
    AgendaTimestampCollection, AgendaTimestamp, AgendaTimestamps, FileMeta,
    CheckError, FilesizeMagicNumber,
)
from .utils import JobWaiters, is_same_filesystem
from .downloader import (
    Downloader,
    StupidZeroContentLengthError,
    ThisShouldBeA404ErrorButItsNot,
)



T = TypeVar('T')



# class BatchQueue(Generic[T]):
#     def __init__(self, max_batches: int = MAX_BATCHES) -> None:
#         self.max_batches = max_batches
#         self._q = asyncio.Queue(maxsize=max_batches)



#     # async def __aenter__(self) -> Self:

class SchedulersTD(TypedDict):
    general: aiojobs.Scheduler
    downloads: aiojobs.Scheduler
    copies: aiojobs.Scheduler

SchedulerKey = Literal['general', 'downloads', 'copies']
SchedulerKeys: list[SchedulerKey] = ['general', 'downloads', 'copies']


SCHEDULERS: SchedulersTD|None = None
def get_schedulers(limit: int|None = None) -> SchedulersTD:
    global SCHEDULERS
    if SCHEDULERS is None:
        if limit is None:
            raise RuntimeError('scheduler limits must be set')
        SCHEDULERS = {
            'general':aiojobs.Scheduler(limit=16, pending_limit=1),
            'downloads':aiojobs.Scheduler(limit=limit),
            'copies':aiojobs.Scheduler(limit=2),
        }
    elif limit is not None:
        raise RuntimeError('schedulers already created')
    return SCHEDULERS


def get_scheduler(key: SchedulerKey, limit: int|None = None) -> aiojobs.Scheduler:
    d = get_schedulers(limit=limit)
    return d[key]


async def close_schedulers() -> None:
    logger.info('closing schedulers..')
    scheduler_list = [get_scheduler(key) for key in SchedulerKeys]
    await asyncio.gather(*[sch.wait_and_close() for sch in scheduler_list])
    logger.debug('schedulers closed')


async def get_main_data(session: ClientSession, data_url: URL, base_dir: Path) -> ClipCollection:
    async with session.get(str(data_url)) as response:
        if not response.ok:
            response.raise_for_status()
        html = await response.text()
        clips = parse_page(html, base_dir=base_dir, scheme=data_url.scheme)
    return clips

# @logger.catch
async def get_real_pdf_link(session: ClientSession, src_url: URL) -> URL:
    logger.info(f'get_real_pdf_link: {src_url=}')
    # https://docs.google.com/gview?url=http://legistar.granicus.com/Mansfield/meetings/2018/3/2571_M_City_Council_18-03-26_Meeting_Minutes.pdf&embedded=true
    async with session.get(src_url, allow_redirects=False) as response:
        assert response.status == 302
        logger.debug(f'{response.url=}, {response.real_url=}')
        logger.debug(f'{response.headers=}')
        re_url = response.headers['Location']
        assert len(re_url)
        re_url = URL(re_url)
        if re_url.host == 'docs.google.com':
            real_url = URL(re_url.query.getone('url'))
        else:
            real_url = re_url
        logger.debug(f'{real_url=}')
        return real_url


async def replace_pdf_links(
    session: ClientSession,
    parse_clip: ParseClipData,
) -> ParseClipData:
    if not parse_clip.has_incomplete_links():
        return parse_clip
    link_kw = {}
    for key, url in parse_clip.iter_incomplete_links():
        real_key = key.split('_')[0]
        if key == 'audio' or key == 'video':
            link_kw[real_key] = url
            continue
        real_url = await get_real_pdf_link(session, url)
        if parse_clip.actual_links is not None:
            parse_clip.actual_links[key] = real_url
        else:
            link_kw[real_key] = real_url
    if parse_clip.actual_links is None:
        links = ParseClipLinks(**link_kw)
        parse_clip.actual_links = links
    return parse_clip


async def replace_all_pdf_links(session: ClientSession, clips: ClipCollection) -> None:
    scheduler = get_scheduler('general')
    jobs: set[aiojobs.Job[ParseClipData]] = set()
    for clip in clips:
        if not clip.parse_data.has_incomplete_links():
            continue
        job = await scheduler.spawn(replace_pdf_links(session, clip.parse_data))
        jobs.add(job)
    if len(jobs):
        await asyncio.gather(*[job.wait() for job in jobs])


async def download_clip(downloader: Downloader, clip: Clip) -> None:
    download_waiter = JobWaiters[aiojobs.Job|None](scheduler=get_scheduler('downloads'))
    copy_waiter = JobWaiters(scheduler=get_scheduler('copies'))

    def set_zero_length_meta(key: ClipFileKey) -> None:
        if key == 'video':
            logger.warning(f'Cannot set zero-length meta on video: "{clip.unique_name=}')
            return
        cur_meta = clip.files.get_metadata(key)
        if cur_meta is None:
            cur_meta = FileMeta.create_zero_length()
            clip.files.set_metadata(key, cur_meta)
        else:
            logger.warning(f'{cur_meta=}')

    async def download_file(
        key: ClipFileKey,
        url: URL,
        filename: Path,
        temp_dir: Path
    ) -> aiojobs.Job|None:
        temp_filename = temp_dir / filename.name
        logger.debug(f'download {url} to {filename}')
        chunk_size = 64*1024
        if key == 'video':
            # 1 hour total, 1 minute for connect, 1 minute between reads
            timeout = ClientTimeout(total=60*60, sock_connect=60, sock_read=60)
        else:
            timeout = ClientTimeout(total=300)
        try:
            dl = await downloader.download(
                url=url, filename=temp_filename, chunk_size=chunk_size,
                timeout=timeout,
            )
        except ServerTimeoutError as exc:
            if temp_filename.exists():
                temp_filename.unlink()
            logger.exception(exc)
            return None
        except StupidZeroContentLengthError as exc:
            logger.warning(str(exc))
            set_zero_length_meta(key)
            return None
        except ThisShouldBeA404ErrorButItsNot as exc:
            logger.warning(f'{exc.__class__.__name__}: {exc} {filename=}, {url=}')
            set_zero_length_meta(key)
            return None

        logger.debug(f'download complete for "{clip.unique_name} - {key}"')
        copy_coro = copy_clip_to_dest(
            key, src_file=temp_filename, dst_file=filename, meta=dl.meta,
        )
        return await copy_waiter.spawn(copy_coro)


    async def copy_clip_to_dest(
        key: ClipFileKey,
        src_file: Path,
        dst_file: Path,
        meta: FileMeta
    ) -> None:
        if is_same_filesystem(src_file, dst_file):
            src_file.rename(dst_file)
            clip.files.ensure_path(key)
            clip.files.set_metadata(key, meta)
            return
        chunk_size = 64*1024
        logger.debug(f'copying "{clip.unique_name} - {key}"')
        try:
            async with aiofile.async_open(src_file, 'rb') as src_fd:
                async with aiofile.async_open(dst_file, 'wb') as dst_fd:
                    async for chunk in src_fd.iter_chunked(chunk_size):
                        await dst_fd.write(chunk)
            logger.debug(f'copy complete for "{clip.unique_name} - {key}"')
            src_file.unlink()
            clip.files.ensure_path(key)
            clip.files.set_metadata(key, meta)
        except:
            if dst_file.exists():
                dst_file.unlink()
            if key in clip.files.metadata:
                del clip.files.metadata[key]

    logger.info(f'downloading clip "{clip.unique_name}"')
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir).resolve()
        clip.root_dir_abs.mkdir(exist_ok=True, parents=True)
        for key, url, filename in clip.iter_url_paths():
            if filename.exists():
                logger.debug(f'filename exists: {key=}, {filename=}')
                continue
            # logger.debug(f'{scheduler.active_count=}')
            coro = download_file(key, url, filename, temp_dir)
            await download_waiter.spawn(coro)

        await download_waiter.gather()
        await copy_waiter.gather()

    logger.success(f'clip "{clip.unique_name}" complete')

def check_clip_files(clip: Clip, warnings_only: bool = False) -> int:
    try:
        clip.files.check()
    except CheckError as exc:
        if warnings_only:
            logger.warning(str(exc))
            return 0
        raise
    count = 0
    for key, url, filename in clip.iter_url_paths():
        if not filename.exists():
            continue
        meta = clip.files.get_metadata(key)
        if meta is None:
            logger.warning(f'No metadata for "{filename}"')
            filename.unlink()
            continue
        assert meta.content_length is not None
        if filename.stat().st_size != meta.content_length:
            logger.warning(f'Size mismatch for "{filename}"')
            filename.unlink()
        count += 1
    return count

@logger.catch
def check_all_clip_files(clips: ClipCollection, warnings_only: bool = False):
    clip_count = 0
    item_count = 0
    for clip in clips:
        _item_count = check_clip_files(clip, warnings_only=warnings_only)
        item_count += _item_count
        clip_count += 1
    logger.info(f'checked {clip_count} clips and {item_count} items')


async def get_agenda_timestamps(
    session: ClientSession,
    timestamps: AgendaTimestampCollection,
    clip: Clip
) -> bool:
    if clip in timestamps:
        return False
    url = clip.parse_data.player_link
    if url is None:
        return False
    logger.debug(f'getting timestamps for "{clip.id} - {clip.parse_data.name}"')
    async with session.get(url) as response:
        if not response.ok:
            response.raise_for_status()
        html = await response.text()
        items: list[AgendaTimestamp] = []
        for time_seconds, item_text in parse_player_page(html):
            items.append(AgendaTimestamp(seconds=time_seconds, text=item_text))
    timestamps.add(AgendaTimestamps(clip_id=clip.id, items=items))
    return True


async def get_all_agenda_timestamps(
    session: ClientSession,
    clips: ClipCollection,
    timestamps: AgendaTimestampCollection
):
    scheduler = get_scheduler('general')
    waiter: JobWaiters[bool] = JobWaiters(scheduler=scheduler)
    for clip in clips:
        if clip in timestamps:
            continue
        if clip.parse_data.player_link is None:
            continue
        await waiter.spawn(get_agenda_timestamps(session, timestamps, clip))
    await waiter


def build_web_vtt(
    clip: Clip,
    timestamps: AgendaTimestampCollection,
    mkdir: bool = False
) -> bool:
    if clip not in timestamps:
        return False
    vtt_filename = clip.get_file_path('chapters', absolute=True)
    if vtt_filename.exists():
        return False
    if not vtt_filename.parent.exists():
        if not mkdir:
            return False
        vtt_filename.parent.mkdir(parents=True)
    ts_obj = timestamps[clip]
    if not len(ts_obj):
        return False
    logger.debug(f'{vtt_filename}')
    vtt_text = ts_obj.build_vtt(clip)
    vtt_filename.write_text(vtt_text)
    clip.files.check_chapters_file()
    return True



async def check_clip_file_meta(
    session: ClientSession,
    clip: Clip,
    report_only: bool = True
) -> bool:

    if session.closed:
        # Just in case this task was cancelled from other exceptions
        return False

    async def get_file_meta(
        key: ClipFileKey,
        url: URL,
        filename: Path
    ) -> tuple[ClipFileKey, Path, FileMeta|Literal[False]]:
        if session.closed:
            # Another escape hatch for task cancellation
            return key, filename, False
        async with session.get(url) as resp:
            meta = FileMeta.from_headers(resp.headers)
        return key, filename, meta

    coros = set[Coroutine[Any, Any, tuple[ClipFileKey, Path, FileMeta|Literal[False]]]]()
    for key, url, filename in clip.iter_url_paths():
        if not filename.exists():
            continue
        coros.add(get_file_meta(key, url, filename))

    changed = False
    for c in asyncio.as_completed(coros):
        key, filename, remote_meta = await c
        if remote_meta is False:
            continue
        local_meta = clip.files.get_metadata(key)
        if local_meta is None:
            if not filename.exists():
                continue
            logger.warning(f'{filename} has no local metadata')
            st = filename.stat()
            assert st.st_size == remote_meta.content_length, f'content length incorrect for {filename}'
            if not report_only:
                # Since there is no local metadata, it's safe to just add it here
                # (after making sure the filesize matches above)
                logger.info(f'inserting meta for {filename}')
                clip.files.set_metadata(key, remote_meta)
            changed = True
        else:
            if local_meta.content_length != remote_meta.content_length:
                if remote_meta.etag is None or remote_meta.last_modified is None:
                    # There's no way to be certain which of the two is valid
                    # so just warn here and skip all checks below
                    logger.warning(f'invalid remote meta for {filename}: {remote_meta=}, {local_meta=}')
                    continue
                filesize = filename.stat().st_size
                if remote_meta.content_length == filesize:
                    logger.warning(f'{filename} content-length mismatch: filesize={filesize}, local={local_meta.content_length}, remote={remote_meta.content_length}')
                    if not report_only:
                        # Remote meta matches, so replace the local with it
                        logger.info(f'replacing meta for content_length: {filename}')
                        clip.files.set_metadata(key, remote_meta)
                    changed = True
                elif (FilesizeMagicNumber.is_magic_number(local_meta) or
                      FilesizeMagicNumber.is_magic_number(filesize)):
                    logger.warning(f'{filename} size is {filesize}, but remote is {remote_meta.content_length}.  This is likely a bad file')
                    if not report_only:
                        # Delete the file so it can be re-downloaded
                        logger.info(f'delete "{filename}"')
                        filename.unlink()
                        del clip.files.metadata[key]
                    changed = True
                    # Skip any further checks since the file and metadata
                    # have been deleted
                    continue
                else:
                    # We don't really know what to do at this point
                    logger.warning(f'filesize mis-match: remote={remote_meta.content_length}, local={local_meta.content_length}, filesize={filesize}, {filename=}')
            if local_meta.etag != remote_meta.etag:
                if local_meta.etag is None:
                    logger.warning(f'{filename} has no etag, but the remote value is "{remote_meta.etag}"')
                    if not report_only:
                        # Safe to replace since we have no local etag
                        logger.info(f'replacing meta for etag: {filename}')
                        clip.files.set_metadata(key, remote_meta)
                    changed = True
                    continue
                logger.warning(f'etag mismatch for {filename}, local="{local_meta.etag}", remote="{remote_meta.etag}"')
    return changed


@logger.catch
async def check_all_clip_meta(
    conf: Config,
    report_only: bool = True
) -> None:
    schedulers = get_schedulers(limit=16)
    data_file = conf.data_file
    if not data_file.exists():
        raise Exception('No data file')
    clips = ClipCollection.load(data_file)

    async with ClientSession() as session:
        meta_waiter = JobWaiters[bool](scheduler=schedulers['general'])
        for clip in clips:
            await meta_waiter.spawn(check_clip_file_meta(
                session, clip, report_only=report_only,
            ))
        meta_changed = await meta_waiter
        if not report_only and any(meta_changed):
            clips.save(data_file)
        await close_schedulers()


@logger.catch
async def amain(
    data_url: URL,
    data_file: Path,
    timestamp_file: Path,
    out_dir: Path,
    scheduler_limit: int,
    max_clips: int|None = None,
    folder: str|None = None,
):
    schedulers = get_schedulers(limit=scheduler_limit)
    waiter = JobWaiters(scheduler=schedulers['general'])

    local_clips: ClipCollection|None = None
    if data_file.exists():
        local_clips = ClipCollection.load(data_file)
    if timestamp_file.exists():
        timestamps = AgendaTimestampCollection.load(timestamp_file)
    else:
        timestamps = AgendaTimestampCollection()
    async with ClientSession() as session:
        download_scheduler = schedulers['downloads']
        downloader = Downloader(session=session, scheduler=download_scheduler)
        clips = await get_main_data(session, data_url, out_dir)
        if local_clips is not None:
            clips = clips.merge(local_clips)
        await replace_all_pdf_links(session, clips)
        await get_all_agenda_timestamps(session, clips, timestamps)
        clips.save(data_file)
        timestamps.save(timestamp_file)
        check_all_clip_files(clips)
        i = 0
        try:
            for clip in clips:
                if max_clips == 0:
                    break
                if folder is not None and clip.location != folder:
                    continue
                build_web_vtt(clip, timestamps, mkdir=True)
                if clip.complete:
                    continue
                # logger.debug(f'{scheduler.active_count=}')
                await waiter.spawn(download_clip(downloader, clip))
                i += 1
                if max_clips is not None and i >= max_clips:
                    break
            await waiter
            await close_schedulers()
        finally:
            clips.save(data_file)
    return clips

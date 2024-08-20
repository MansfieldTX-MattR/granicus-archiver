
import asyncio
from pathlib import Path
from dataclasses import dataclass

import click
from aiohttp import ClientSession

from .model import ClipCollection, AgendaTimestampCollection, ClipsIndex
from . import client
from . import html_builder
from .googledrive import auth as googleauth
from .googledrive import client as googleclient


@dataclass
class BaseContext:
    out_dir: Path
    data_file: Path
    timestamp_file: Path


@click.group
@click.option(
    '-o', '--out-dir',
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help='Root directory to store downloaded files',
)
@click.option(
    '--data-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
    help='Filename to store download information. Defaults to "<out-dir>/data.json"',
)
@click.option(
    '--timestamp-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
    help='Filename to store clip timestamp information. Defaults to "<out-dir>/timestamp-data.yaml"',
)
@click.pass_context
def cli(
    ctx: click.Context,
    out_dir: Path,
    data_file: Path|None,
    timestamp_file: Path|None
):
    if data_file is None:
        data_file = out_dir / 'data.json'
    if timestamp_file is None:
        timestamp_file = out_dir / 'timestamp-data.yaml'
    ctx.obj = BaseContext(
        out_dir=out_dir, data_file=data_file, timestamp_file=timestamp_file,
    )

@cli.command
@click.pass_obj
def build_vtt(obj: BaseContext):
    """Build vtt files with chapters from agenda timestamp data
    """
    clips = ClipCollection.load(obj.data_file)
    timestamps = AgendaTimestampCollection.load(obj.timestamp_file)
    changed = False
    for clip in clips:
        _changed = client.build_web_vtt(clip, timestamps)
        if _changed:
            changed = True
    if changed:
        clips.save(obj.data_file)


@cli.command
@click.pass_obj
def check(obj: BaseContext):
    """Check downloaded files using the stored metadata
    """
    clips = ClipCollection.load(obj.data_file)
    client.check_all_clip_files(clips)

@cli.command
@click.pass_obj
def fix_dts(obj: BaseContext):
    out_dir, data_file = obj.out_dir, obj.data_file

    async def do_fix():
        local_clips: ClipCollection|None = None
        if data_file.exists():
            local_clips = ClipCollection.load(data_file)
        async with ClientSession() as session:
            clips = orig_clips = await client.get_main_data(session, out_dir)
        if local_clips is not None:
            local_keys = set(local_clips.clips.keys())
            overlap = set(clips.clips.keys()) & local_keys
            clips = clips.merge(local_clips)
            assert len(overlap)
            for key in overlap:
                orig_clip = orig_clips.clips[key]
                clip = clips[key]
                clip.parse_data.date = orig_clip.parse_data.date
            clips.save(data_file)

    asyncio.run(do_fix())

@cli.command
@click.option(
    '--max-clips', type=int, required=False,
    help='Maximum number of clips to download. If not provided, there is no limit',
)
@click.option(
    '--io-job-limit', type=int, default=8, show_default=True,
    help='Limit number of concurrent uploads to this amount'
)
@click.option(
    '--folder', type=str,
)
@click.pass_obj
def download(
    obj: BaseContext,
    max_clips: int|None,
    io_job_limit: int,
    folder: str|None,
):
    clips = asyncio.run(client.amain(
        data_file=obj.data_file,
        timestamp_file=obj.timestamp_file,
        out_dir=obj.out_dir,
        scheduler_limit=io_job_limit,
        max_clips=max_clips,
        folder=folder,
    ))

@cli.command
@click.argument(
    'html-filename',
    type=click.Path(dir_okay=False, file_okay=True, path_type=Path)
)
@click.pass_obj
def build_html(obj: BaseContext, html_filename: Path):
    """Build a static html file with links to all downloaded content and
    save it to HTML_FILENAME
    """
    clips = ClipCollection.load(obj.data_file)
    html = html_builder.build_html(clips, html_dir=html_filename.parent)
    html_filename.write_text(html)

@cli.command
@click.argument(
    'html-filename',
    type=click.Path(dir_okay=False, file_okay=True, path_type=Path)
)
@click.option(
    '--drive-folder',
    type=click.Path(path_type=Path),
    default=Path('granicus-archive/data'),
    show_default=True,
    help='Name of the root folder the files were uploaded to',
)
@click.pass_obj
def build_drive_html(obj: BaseContext, html_filename: Path, drive_folder: Path):
    """Build a static html file with links to the all content uploaded to
    Drive
    """
    clips = ClipCollection.load(obj.data_file)
    html = asyncio.run(googleclient.build_html(
        clips, html_dir=html_filename.parent, upload_dir=drive_folder
    ))
    html_filename.write_text(html)

@cli.command
@click.pass_obj
def authorize(obj: BaseContext):
    """Launch a browser window to authorize uploads to Drive
    """
    asyncio.run(googleauth.run_app())


@cli.command
@click.option(
    '--drive-folder',
    type=click.Path(path_type=Path),
    default=Path('granicus-archive/data'),
    show_default=True,
    help='Name of the root folder to upload to',
)
@click.option(
    '--max-clips', type=int,
    help='Maximum number of clips to download. If not provided, there is no limit',
)
@click.option(
    '--io-job-limit', type=int, default=8, show_default=True,
    help='Limit number of concurrent downloads to this amount'
)
@click.pass_obj
def upload(
    obj: BaseContext,
    max_clips: int,
    drive_folder: Path,
    io_job_limit: int
):
    """Upload all local content to Google Drive
    """
    clips = ClipCollection.load(obj.data_file)
    asyncio.run(googleclient.upload_clips(
        clips,
        upload_dir=drive_folder,
        max_clips=max_clips,
        scheduler_limit=io_job_limit,
    ))

@cli.command
@click.option(
    '--index-root',
    type=click.Path(dir_okay=True, file_okay=True, exists=True, path_type=Path),
    default=Path('.'),
    show_default=True,
)
@click.option(
    '--indent', type=int, default=2, show_default=True
)
@click.option('--overwrite/--no-overwrite', default=True)
@click.pass_obj
def write_clip_index(obj: BaseContext, indent: int, index_root: Path, overwrite: bool):
    clips = ClipCollection.load(obj.data_file)
    clips_index = ClipsIndex.from_clip_collection(clips, index_root)
    clips_index.write_data(
        clip_collection=clips,
        exist_ok=overwrite,
        indent=None if indent<=0 else indent
    )


if __name__ == '__main__':
    cli()

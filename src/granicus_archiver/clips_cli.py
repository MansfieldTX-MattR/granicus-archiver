from __future__ import annotations
from typing import TYPE_CHECKING
import os
import asyncio
from pathlib import Path
import click
from yarl import URL

from .model import ClipCollection, AgendaTimestampCollection, ClipsIndex
from . import client
from . import html_builder
from .googledrive import client as googleclient
if TYPE_CHECKING:
    from .cli import BaseContext


@click.group(name='clips')
@click.pass_obj
def cli(obj: BaseContext):
    """Granicus clips sub-commands
    """
    root_dir_abs = obj.config.out_dir_abs
    root_dir_rel = obj.config.out_dir
    if root_dir_rel.resolve() != root_dir_abs:
        click.echo(f'{root_dir_abs=}')
        click.confirm(
            f'Current working directory does not match your config. Continue?',
            abort=True,
        )


@cli.command
@click.pass_obj
def build_vtt(obj: BaseContext):
    """Build vtt files with chapters from agenda timestamp data
    """
    clips = ClipCollection.load(obj.config.data_file)
    timestamps = AgendaTimestampCollection.load(obj.config.timestamp_file)
    changed = False
    for clip in clips:
        _changed = client.build_web_vtt(clip, timestamps)
        if _changed:
            changed = True
    if changed:
        clips.save(obj.config.data_file)

@cli.command(name='check-meta')
@click.option(
    '--report-only/--no-report-only', default=True, show_default=True,
    help='If enabled, no modifications will be made to the data'
)
@click.pass_obj
def check_clip_meta(obj: BaseContext, report_only: bool):
    """Compare local metadata against remote request headers
    """
    asyncio.run(client.check_all_clip_meta(obj.config, report_only=report_only))
    click.echo('Check complete')


@cli.command(name='check')
@click.option(
    '--warnings-only/--no-warnings-only', default=True, show_default=True,
    help='Whether to treat check errors as warnings or exceptions'
)
@click.pass_obj
def check_clips(obj: BaseContext, warnings_only: bool):
    """Check downloaded files using the stored metadata
    """
    clips = ClipCollection.load(obj.config.data_file)
    client.check_all_clip_files(clips, warnings_only=warnings_only)
    click.echo('Check complete')



@cli.command(name='download')
@click.option(
    '--max-clips', type=int, required=False,
    help='Maximum number of clips to download. If not provided, there is no limit',
)
@click.option(
    '--io-job-limit', type=int, default=4, show_default=True,
    help='Limit number of concurrent uploads to this amount'
)
@click.option(
    '--temp-dir',
    type=click.Path(
        file_okay=False,
        dir_okay=True,
        exists=True,
        path_type=Path,
    ),
    help="Directory for temporary files. Only set this if you know what you're doing",
)
@click.option(
    '--folder', type=str,
)
@click.pass_obj
def download_clips(
    obj: BaseContext,
    max_clips: int|None,
    io_job_limit: int,
    temp_dir: Path|None,
    folder: str|None,
):
    """Download files for Granicus clips
    """
    data_url = obj.config.granicus_data_url
    if data_url is None:
        url_str = click.prompt('Please enter the URL for granicus data: ', type=str)
        data_url = URL(url_str)
        obj.config.update(granicus_data_url=data_url)
        obj.config.save(obj.config_file)

    if temp_dir is not None:
        os.environ['TMPDIR'] = str(temp_dir)
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_p = Path(td).resolve()
            assert td_p.parent == temp_dir.resolve()
    clips = asyncio.run(client.amain(
        data_url=data_url,
        data_file=obj.config.data_file,
        timestamp_file=obj.config.timestamp_file,
        out_dir=obj.config.out_dir,
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
    clips = ClipCollection.load(obj.config.data_file)
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
    google_conf = obj.config.google
    if google_conf.drive_folder != drive_folder:
        google_conf.drive_folder = drive_folder
        obj.config.save(obj.config_file)
    clips = ClipCollection.load(obj.config.data_file)
    html = asyncio.run(googleclient.build_html(
        clips, html_dir=html_filename.parent, root_conf=obj.config,
    ))
    html_filename.write_text(html)


@cli.command(name='upload')
@click.option(
    '--drive-folder',
    type=click.Path(path_type=Path),
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
def upload_clips(
    obj: BaseContext,
    max_clips: int,
    drive_folder: Path|None,
    io_job_limit: int
):
    """Upload all local content to Google Drive
    """
    if drive_folder is not None:
        google_conf = obj.config.google
        if google_conf.drive_folder != drive_folder:
            google_conf.drive_folder = drive_folder
            if google_conf.drive_folder == google_conf.legistar_drive_folder:
                click.confirm(
                    'Granicus and Legistar drive folders match. Continue?',
                    abort=True,
                )
            obj.config.save(obj.config_file)
    clips = ClipCollection.load(obj.config.data_file)
    asyncio.run(googleclient.upload_clips(
        clips,
        root_conf=obj.config,
        max_clips=max_clips,
        scheduler_limit=io_job_limit,
    ))


@cli.command(name='check-uploads')
@click.option(
    '--check-hashes/--no-check-hashes', default=False,
    help='If supplied, verify checksums of uploaded files',
)
@click.option(
    '--hash-logfile',
    type=click.Path(path_type=Path),
    help='Optional file to store/load local file hashes (for faster checks)'
)
@click.pass_obj
def check_clips_uploads(obj: BaseContext, check_hashes: bool, hash_logfile: Path|None):
    """Check uploaded metadata and verify content hashes
    """
    clips = ClipCollection.load(obj.config.data_file)
    asyncio.run(googleclient.check_clip_file_meta(
        clips,
        root_conf=obj.config,
        check_hashes=check_hashes,
        hash_logfile=hash_logfile,
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
    clips = ClipCollection.load(obj.config.data_file)
    clips_index = ClipsIndex.from_clip_collection(clips, index_root)
    clips_index.write_data(
        clip_collection=clips,
        exist_ok=overwrite,
        indent=None if indent<=0 else indent
    )

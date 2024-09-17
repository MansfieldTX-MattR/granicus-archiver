
import asyncio
from pathlib import Path
from dataclasses import dataclass

import click
from aiohttp import ClientSession
from yarl import URL

from .config import Config
from .model import ClipCollection, AgendaTimestampCollection, ClipsIndex
from . import client
from . import html_builder
from .googledrive import auth as googleauth
from .googledrive import client as googleclient
from .legistar import client as legistar_client


@dataclass
class BaseContext:
    config: Config
    config_file: Path


@click.group
@click.option(
    '-c', '--config-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    default=Config.default_filename,
)
@click.option(
    '-o', '--out-dir',
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    help='Root directory to store downloaded files',
)
@click.option(
    '--data-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
    help='Filename to store download information. Defaults to "<out-dir>/data.json"',
)
@click.option(
    '--legistar-data-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
    help='Filename to store legistar information. Defaults to "<out-dir>/legistar-data.json"',
)
@click.option(
    '--legistar-feed-url',
    type=str, required=False,
    help='RSS Feed URL for Legistar calendar events',
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
    config_file: Path,
    out_dir: Path,
    data_file: Path|None,
    legistar_data_file: Path|None,
    legistar_feed_url: str|None,
    timestamp_file: Path|None
):
    feed_url = None if legistar_feed_url is None else URL(legistar_feed_url)
    conf_kw = dict(
        out_dir=out_dir,
        data_file=data_file,
        legistar_data_file=legistar_data_file,
        legistar_feed_url=feed_url,
        timestamp_file=timestamp_file,
    )
    conf_kw = {k:v for k,v in conf_kw.items() if v is not None}
    if config_file.exists():
        config = Config.load(config_file)
        if config.update(**conf_kw):
            config.save(config_file)
    else:
        config = Config.build_defaults(**conf_kw)
        config.save(config_file)
    if config.legistar_feed_url is None:
        feed_url = click.prompt('Please enter the Legistar RSS Feed URL', type=str)
        feed_url = URL(feed_url)
        config.update(legistar_feed_url=feed_url)
        config.save(config_file)

    ctx.obj = BaseContext(
        config=config,
        config_file=config_file,
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


@cli.command
@click.pass_obj
def check(obj: BaseContext):
    """Check downloaded files using the stored metadata
    """
    clips = ClipCollection.load(obj.config.data_file)
    client.check_all_clip_files(clips)

    feed_url = obj.config.legistar_feed_url
    if feed_url is None:
        return
    asyncio.run(legistar_client.amain(
        data_file=obj.config.data_file,
        legistar_data_file=obj.config.legistar_data_file,
        legistar_feed_url=feed_url,
        legistar_category_maps=obj.config.legistar_category_maps,
        max_clips=0,
        check_only=True,
    ))

@cli.command
@click.pass_obj
def fix_dts(obj: BaseContext):
    out_dir, data_file = obj.config.out_dir, obj.config.data_file

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
        data_file=obj.config.data_file,
        timestamp_file=obj.config.timestamp_file,
        out_dir=obj.config.out_dir,
        scheduler_limit=io_job_limit,
        max_clips=max_clips,
        folder=folder,
    ))

@cli.command
@click.option('--granicus-folder', type=str, prompt=True)
@click.option('--legistar-category', type=str, prompt=True)
@click.pass_obj
def add_legistar_category_map(obj: BaseContext, granicus_folder: str, legistar_category: str):
    obj.config.update(legistar_category_maps={granicus_folder: legistar_category})
    obj.config.save(obj.config_file)
    click.echo('category map added')

@cli.command
@click.option(
    '--max-clips', type=int, required=False, default=0, show_default=True,
    help='Maximum number of clips to download agenda packets for. If zero, downloads are disabled',
)
@click.pass_obj
def parse_legistar(obj: BaseContext, max_clips: int):
    # if obj.config.legistar_feed_url
    feed_url = obj.config.legistar_feed_url
    if feed_url is None:
        feed_url = click.prompt('Please enter the Legistar RSS Feed URL', type=str)
        feed_url = URL(feed_url)
        obj.config.update(legistar_feed_url=feed_url)
        obj.config.save(obj.config_file)
    asyncio.run(legistar_client.amain(
        data_file=obj.config.data_file,
        legistar_data_file=obj.config.legistar_data_file,
        legistar_feed_url=feed_url,
        legistar_category_maps=obj.config.legistar_category_maps,
        max_clips=max_clips,
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

@cli.command
@click.pass_obj
def authorize(obj: BaseContext):
    """Launch a browser window to authorize uploads to Drive
    """
    asyncio.run(googleauth.run_app(root_conf=obj.config))


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
    google_conf = obj.config.google
    if google_conf.drive_folder != drive_folder:
        google_conf.drive_folder = drive_folder
        obj.config.save(obj.config_file)
    clips = ClipCollection.load(obj.config.data_file)
    asyncio.run(googleclient.upload_clips(
        clips,
        root_conf=obj.config,
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
    clips = ClipCollection.load(obj.config.data_file)
    clips_index = ClipsIndex.from_clip_collection(clips, index_root)
    clips_index.write_data(
        clip_collection=clips,
        exist_ok=overwrite,
        indent=None if indent<=0 else indent
    )


if __name__ == '__main__':
    cli()

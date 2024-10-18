
import asyncio
from pathlib import Path
from dataclasses import dataclass
import os

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
from .legistar.model import LegistarData
from . import set_local_timezone


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
    '--local-timezone', type=str, required=False,
)
@click.option(
    '--legistar-out-dir',
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    required=False,
    help='Root directory to store downloaded legistar files',
)
@click.option(
    '--legistar-data-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
    help='Filename to store legistar information. Defaults to "<legistar-out-dir>/legistar-data.json"',
)
@click.option(
    '--legistar-drive-folder',
    type=click.Path(path_type=Path),
    default=Path('granicus-archive/data/legistar'),
    show_default=True,
    help='Name of the root folder to upload legistar items to',
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
    local_timezone: str|None,
    legistar_out_dir: Path|None,
    legistar_data_file: Path|None,
    legistar_drive_folder: Path|None,
    timestamp_file: Path|None
):
    conf_kw = dict(
        out_dir=out_dir,
        data_file=data_file,
        local_timezone_name=local_timezone,
        legistar=dict(
            out_dir=legistar_out_dir,
            data_file=legistar_data_file,
        ),
        google=dict(
            legistar_drive_folder=legistar_drive_folder,
        ),
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
    if config.local_timezone_name is None:
        tzname = click.prompt('Please enter the local timezone name', type=str)
        assert len(tzname)
        config.update(local_timezone_name=tzname)
        config.save(config_file)

    ctx.obj = BaseContext(
        config=config,
        config_file=config_file,
    )
    set_local_timezone(config.local_timezone)

@cli.group()
@click.pass_obj
def clips(obj: BaseContext):
    """Granicus clips sub-commands
    """
    root_dir_abs = obj.config.out_dir_abs
    root_dir_rel = obj.config.out_dir
    if root_dir_rel.resolve() != root_dir_abs:
        click.confirm(
            f'Current working directory does not match your config',
            abort=True,
        )


@cli.group()
@click.pass_obj
def legistar(obj: BaseContext):
    """Legistar sub-commands
    """
    root_dir_abs = obj.config.legistar.out_dir_abs
    root_dir_rel = obj.config.legistar.out_dir
    if root_dir_rel.resolve() != root_dir_abs:
        click.confirm(
            f'Current working directory does not match your config',
            abort=True,
        )


@cli.group()
@click.pass_obj
def drive(obj: BaseContext):
    """Google Drive sub-commands
    """
    pass

@cli.command
@click.pass_obj
def show_config(obj: BaseContext):
    """Show the current configuration
    """
    from pprint import pformat
    sub_keys = ['google', 'legistar']
    ser = obj.config.serialize()
    for key in sub_keys:
        del ser[key]
    click.echo('Root Config:')
    click.echo(pformat(ser))
    click.echo('')

    for key in sub_keys:
        cfg = getattr(obj.config, key)
        ser = cfg.serialize()
        click.echo(f'{key.title()} Config:')
        click.echo(pformat(ser))
        click.echo('')


@legistar.command
@click.option('--name', type=str, prompt=True)
@click.option('--url', type=str, prompt=True)
@click.pass_obj
def add_feed_url(obj: BaseContext, name: str, url: str):
    """Add a legistar calendar RSS feed url
    """
    conf = obj.config.legistar
    if name in conf.feed_urls:
        if not click.confirm(f'URL exists for name "{name}".  Overwrite?'):
            return
    changed = conf.update(feed_urls={name: URL(url)})
    if changed:
        obj.config.save(obj.config_file)


@legistar.command
@click.pass_obj
def list_feed_urls(obj: BaseContext):
    """Show RSS feed urls in the current configuration
    """
    for name, url in obj.config.legistar.feed_urls.items():
        click.echo(f'{name}\t{url}')


@clips.command
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


@clips.command(name='check')
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


@legistar.command(name='check')
@click.pass_obj
def check_legistar(obj: BaseContext):
    """Check downloaded files using the stored metadata
    """
    leg_client_obj = asyncio.run(legistar_client.amain(
        config=obj.config,
        max_clips=0,
        check_only=True,
    ))
    leg_items = leg_client_obj.get_item_count()
    leg_files = leg_client_obj.get_file_count()
    click.echo('Check complete')
    click.echo(f'Legistar: item_count={leg_items}, file_count={leg_files}')


@clips.command(name='download')
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
    if temp_dir is not None:
        os.environ['TMPDIR'] = str(temp_dir)
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_p = Path(td).resolve()
            assert td_p.parent == temp_dir.resolve()
    clips = asyncio.run(client.amain(
        data_file=obj.config.data_file,
        timestamp_file=obj.config.timestamp_file,
        out_dir=obj.config.out_dir,
        scheduler_limit=io_job_limit,
        max_clips=max_clips,
        folder=folder,
    ))

@legistar.command(name='add-category-map')
@click.option('--granicus-folder', type=str, prompt=True)
@click.option('--legistar-category', type=str, prompt=True)
@click.pass_obj
def add_category_map(obj: BaseContext, granicus_folder: str, legistar_category: str):
    """Map a legistar category to its granicus folder
    """
    obj.config.legistar.update(category_maps={granicus_folder: legistar_category})
    obj.config.save(obj.config_file)
    click.echo('category map added')

@legistar.command(name='download')
@click.option('--allow-updates/--no-allow-updates', default=False)
@click.option(
    '--max-clips', type=int, required=False, default=0, show_default=True,
    help='Maximum number of clips to download agenda packets for. If zero, downloads are disabled',
)
@click.option(
    '--strip-pdf-links/--no-strip-pdf-links', default=False,
    help='Whether to remove embedded links from downloaded pdf files',
)
@click.pass_obj
def download_legistar(
    obj: BaseContext,
    allow_updates: bool,
    max_clips: int,
    strip_pdf_links: bool
):
    """Parse and download legistar files
    """
    asyncio.run(legistar_client.amain(
        config=obj.config,
        max_clips=max_clips,
        allow_updates=allow_updates,
        strip_pdf_links=strip_pdf_links,
    ))


@legistar.command
@click.option('--max-items', type=int, default=100)
@click.pass_obj
def remove_pdf_links(obj: BaseContext, max_items: int):
    """Strip embedded links from downloaded pdf files (in-place)
    """
    click.echo(f'{max_items=}')
    legistar_data = LegistarData.load(obj.config.legistar.data_file)
    i = 0
    changed = False
    for f in legistar_data.files.values():
        _changed = f.remove_all_pdf_links()
        if _changed:
            changed = True
        if i % 10 == 0 and changed:
            # Avoid saving data every iteration since this is already
            # IO intensive
            click.echo(f'saving data... {i=}')
            legistar_data.save(obj.config.legistar.data_file)
            changed = False
        if _changed:
            # Only increment if the last item triggered a change
            i += 1
        if i >= max_items:
            break
    legistar_data.save(obj.config.legistar.data_file)


@clips.command
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

@clips.command
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

@drive.command
@click.pass_obj
def authorize(obj: BaseContext):
    """Launch a browser window to authorize uploads to Drive
    """
    asyncio.run(googleauth.run_app(root_conf=obj.config))


@clips.command(name='upload')
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
def upload_clips(
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


@clips.command(name='check-uploads')
@click.pass_obj
def check_clips_uploads(obj: BaseContext):
    """Check uploaded metadata and verify content hashes
    """
    clips = ClipCollection.load(obj.config.data_file)
    asyncio.run(googleclient.get_all_clip_file_meta(clips, root_conf=obj.config))


@legistar.command(name='upload')
@click.option(
    '--max-clips', type=int, default=0,
    help='Maximum number of clips to upload',
)
@click.pass_obj
def upload_legistar(
    obj: BaseContext,
    max_clips: int
):
    """Upload all local content to Google Drive
    """
    asyncio.run(googleclient.upload_legistar(
        root_conf=obj.config,
        max_clips=max_clips,
    ))


@legistar.command(name='check-uploads')
@click.option(
    '--check-hashes/--no-check-hashes', default=False,
    help='If supplied, verify checksums of uploaded files',
)
@click.pass_obj
def check_legistar_uploads(obj: BaseContext, check_hashes: bool):
    """Check uploaded metadata and verify content hashes
    """
    asyncio.run(googleclient.check_legistar_meta(
        root_conf=obj.config, check_hashes=check_hashes,
    ))


@clips.command
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

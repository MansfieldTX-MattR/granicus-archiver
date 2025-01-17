from __future__ import annotations
from typing import TYPE_CHECKING
import os
import asyncio
from pathlib import Path
import click
from yarl import URL

from . import client as legistar_client
from . import guid_client as legistar_guid_client
from .model import LegistarData
from ..googledrive import client as googleclient
if TYPE_CHECKING:
    from ..cli import BaseContext



@click.group(name='legistar')
@click.pass_obj
def cli(obj: BaseContext):
    """Legistar sub-commands
    """
    root_dir_abs = obj.config.legistar.out_dir_abs
    root_dir_rel = obj.config.legistar.out_dir
    if root_dir_rel.resolve() != root_dir_abs:
        click.confirm(
            f'Current working directory does not match your config. Continue?',
            abort=True,
        )


@cli.command
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


@cli.command
@click.pass_obj
def list_feed_urls(obj: BaseContext):
    """Show RSS feed urls in the current configuration
    """
    for name, url in obj.config.legistar.feed_urls.items():
        click.echo(f'{name}\t{url}')


@cli.command(name='check')
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

@cli.command(name='check-rguid')
@click.pass_obj
def check_legistar_rguid(obj: BaseContext):
    """Check downloaded files using the stored metadata
    """
    leg_client_obj = asyncio.run(legistar_guid_client.amain(
        config=obj.config,
        max_clips=0,
        check_only=True,
    ))
    click.echo('Check complete')


@cli.command(name='add-category-map')
@click.option('--granicus-folder', type=str, prompt=True)
@click.option('--legistar-category', type=str, prompt=True)
@click.pass_obj
def add_category_map(obj: BaseContext, granicus_folder: str, legistar_category: str):
    """Map a legistar category to its granicus folder
    """
    obj.config.legistar.update(category_maps={granicus_folder: legistar_category})
    obj.config.save(obj.config_file)
    click.echo('category map added')


@cli.command(name='download')
@click.option('--allow-updates/--no-allow-updates', default=False)
@click.option(
    '--max-clips', type=int, required=False, default=0, show_default=True,
    help='Maximum number of clips to download agenda packets for. If zero, downloads are disabled',
)
@click.option(
    '--strip-pdf-links/--no-strip-pdf-links', default=True, show_default=True,
    help='Whether to remove embedded links from downloaded pdf files',
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
    '--incomplete-csv',
    type=click.Path(
        file_okay=True,
        dir_okay=False,
        path_type=Path,
    ),
    help='Optional file to write incomplete items to (as CSV)',
)
@click.pass_obj
def download_legistar(
    obj: BaseContext,
    allow_updates: bool,
    max_clips: int,
    strip_pdf_links: bool,
    temp_dir: Path|None,
    incomplete_csv: Path|None
):
    """Parse and download legistar files
    """
    if temp_dir is not None:
        assert temp_dir.exists()
        temp_dir_contents = [f for f in temp_dir.iterdir()]
        if len(temp_dir_contents):
            click.echo('\n'.join([str(f) for f in temp_dir_contents]))
            click.confirm('Temp dir not empty. Continue?')
        os.environ['TMPDIR'] = str(temp_dir)
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_p = Path(td).resolve()
            assert td_p.parent == temp_dir.resolve()
    c = asyncio.run(legistar_client.amain(
        config=obj.config,
        max_clips=max_clips,
        allow_updates=allow_updates,
        strip_pdf_links=strip_pdf_links,
    ))
    if incomplete_csv is not None:
        csv = c.get_incomplete_csv()
        incomplete_csv.write_text(csv)
        click.echo(f'incomplete items written to {incomplete_csv}')
    else:
        click.echo('')
        click.echo(c.get_warning_items())


@cli.command(name='download-rguid')
@click.option('--allow-updates/--no-allow-updates', default=False)
@click.option(
    '--max-clips', type=int, required=False, default=0, show_default=True,
    help='Maximum number of clips to download agenda packets for. If zero, downloads are disabled',
)
@click.option(
    '--strip-pdf-links/--no-strip-pdf-links', default=True, show_default=True,
    help='Whether to remove embedded links from downloaded pdf files',
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
    '--incomplete-csv',
    type=click.Path(
        file_okay=True,
        dir_okay=False,
        path_type=Path,
    ),
    help='Optional file to write incomplete items to (as CSV)',
)
@click.pass_obj
def download_legistar_rguid(
    obj: BaseContext,
    allow_updates: bool,
    max_clips: int,
    strip_pdf_links: bool,
    temp_dir: Path|None,
    incomplete_csv: Path|None
):
    """Parse and download legistar files
    """
    if temp_dir is not None:
        assert temp_dir.exists()
        temp_dir_contents = [f for f in temp_dir.iterdir()]
        if len(temp_dir_contents):
            click.echo('\n'.join([str(f) for f in temp_dir_contents]))
            click.confirm('Temp dir not empty. Continue?')
        os.environ['TMPDIR'] = str(temp_dir)
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td_p = Path(td).resolve()
            assert td_p.parent == temp_dir.resolve()
    c = asyncio.run(legistar_guid_client.amain(
        config=obj.config,
        max_clips=max_clips,
        allow_updates=allow_updates,
        strip_pdf_links=strip_pdf_links,
    ))
    if incomplete_csv is not None:
        csv = c.get_incomplete_csv()
        incomplete_csv.write_text(csv)
        click.echo(f'incomplete items written to {incomplete_csv}')
    else:
        click.echo('')
        click.echo(c.get_warning_items())


@cli.command
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


@cli.command(name='upload')
@click.option(
    '--max-clips', type=int, default=0,
    help='Maximum number of clips to upload',
)
@click.option(
    '--drive-folder',
    type=click.Path(path_type=Path),
    help='Name of the root folder to upload to',
)
@click.pass_obj
def upload_legistar(
    obj: BaseContext,
    drive_folder: Path|None,
    max_clips: int
):
    """Upload all local content to Google Drive
    """
    if drive_folder is not None:
        google_conf = obj.config.google
        if drive_folder != google_conf.legistar_drive_folder:
            google_conf.legistar_drive_folder = drive_folder
            if google_conf.drive_folder == google_conf.legistar_drive_folder:
                click.confirm(
                    'Granicus and Legistar drive folders match. Continue?',
                    abort=True,
                )
            elif google_conf.legistar_drive_folder == google_conf.rguid_legistar_drive_folder:
                click.confirm(
                    'Legistar and Legistar-rguid drive folders match. Continue?',
                    abort=True,
                )
            obj.config.save(obj.config_file)
    asyncio.run(googleclient.upload_legistar(
        root_conf=obj.config,
        max_clips=max_clips,
    ))


@cli.command(name='check-uploads')
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


@cli.command(name='upload-rguid')
@click.option(
    '--max-clips', type=int, default=0,
    help='Maximum number of clips to upload',
)
@click.option(
    '--drive-folder',
    type=click.Path(path_type=Path),
    help='Name of the root folder to upload to',
)
@click.pass_obj
def upload_legistar_rguid(
    obj: BaseContext,
    drive_folder: Path|None,
    max_clips: int
):
    """Upload all local content to Google Drive
    """
    if drive_folder is not None:
        google_conf = obj.config.google
        if drive_folder != google_conf.rguid_legistar_drive_folder:
            google_conf.rguid_legistar_drive_folder = drive_folder
            if google_conf.drive_folder == google_conf.rguid_legistar_drive_folder:
                click.confirm(
                    'Granicus and Legistar drive folders match. Continue?',
                    abort=True,
                )
            elif google_conf.legistar_drive_folder == google_conf.rguid_legistar_drive_folder:
                click.confirm(
                    'Legistar and Legistar-rguid drive folders match. Continue?',
                    abort=True,
                )
            obj.config.save(obj.config_file)
    asyncio.run(googleclient.upload_legistar_rguid(
        root_conf=obj.config,
        max_clips=max_clips,
    ))


@cli.command(name='check-uploads-rguid')
@click.option(
    '--check-hashes/--no-check-hashes', default=False,
    help='If supplied, verify checksums of uploaded files',
)
@click.pass_obj
def check_legistar_rguid_uploads(obj: BaseContext, check_hashes: bool):
    """Check uploaded metadata and verify content hashes
    """
    asyncio.run(googleclient.check_legistar_rguid_meta(
        root_conf=obj.config, check_hashes=check_hashes,
    ))

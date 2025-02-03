from typing import Literal
import asyncio
from pathlib import Path
from dataclasses import dataclass

import click

from .config import Config, GroupKey as ConfigGroupKey
# from .server import run_app as run_server_app
from .googledrive import auth as googleauth
from . import set_local_timezone
from .cli_lazy_group import LazyGroup


@dataclass
class BaseContext:
    config: Config
    config_file: Path


@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        'clips': 'granicus_archiver.clips_cli.cli',
        'legistar': 'granicus_archiver.legistar.cli.cli',
    }
)
@click.option(
    '-c', '--config-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    default=Config.default_filename,
    show_default=True,
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
    '--granicus-data-url',
    type=str, required=False,
    help='URL for granicus clip data',
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
    '--timestamp-file',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    required=False,
    help='Filename to store clip timestamp information. Defaults to "<out-dir>/timestamp-data.yaml"',
)
@click.pass_context
def cli(
    ctx: click.Context,
    config_file: Path,
    out_dir: Path|None,
    data_file: Path|None,
    granicus_data_url: str|None,
    local_timezone: str|None,
    legistar_out_dir: Path|None,
    legistar_data_file: Path|None,
    timestamp_file: Path|None
):
    conf_kw = dict(
        out_dir=out_dir,
        data_file=data_file,
        granicus_data_url=granicus_data_url,
        local_timezone_name=local_timezone,
        google={},
        timestamp_file=timestamp_file,
    )
    legistar_kw = {
        'out_dir': legistar_out_dir,
        'data_file': legistar_data_file,
    }
    legistar_kw = {k:v for k,v in legistar_kw.items() if v is not None}
    conf_kw = {k:v for k,v in conf_kw.items() if v is not None}
    conf_kw['legistar'] = legistar_kw
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
def drive(obj: BaseContext):
    """Google Drive sub-commands
    """
    pass

@cli.command
@click.option(
    '--group',
    type=click.Choice(choices=['root', 'google', 'legistar', 'all']),
    default='all',
    show_default=True,
)
@click.pass_obj
def show_config(obj: BaseContext, group: ConfigGroupKey|Literal['all']):
    """Show the current configuration
    """
    from pprint import pformat
    sub_keys: list[ConfigGroupKey] = ['google', 'legistar']
    ser = obj.config.serialize()
    for key in sub_keys:
        del ser[key]
    if group == 'all' or group == 'root':
        click.echo('Root Config:')
        click.echo(pformat(ser))
        click.echo('')

    for key in sub_keys:
        if group == 'all' or group == key:
            cfg = obj.config.get_group(key)
            ser = cfg.serialize()
            click.echo(f'{key.title()} Config:')
            click.echo(pformat(ser))
            click.echo('')


@drive.command
@click.pass_obj
def authorize(obj: BaseContext):
    """Launch a browser window to authorize uploads to Drive
    """
    asyncio.run(googleauth.run_app(root_conf=obj.config))




if __name__ == '__main__':
    cli()

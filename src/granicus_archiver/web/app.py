from __future__ import annotations
from typing import Self, TYPE_CHECKING
from pathlib import Path
import asyncio
import webbrowser
from dataclasses import dataclass

from loguru import logger
from aiohttp import web
from yarl import URL
import aiohttp_jinja2
import jinja2
import click

from .. import set_local_timezone
from ..config import Config
from ..model import ClipCollection
from ..legistar.model import LegistarData
from ..legistar.guid_model import RGuidLegistarData

from .types import *
from .config import AppConfig, APP_CONF_KEY
from .s3client import S3Client, S3ClientKey
if TYPE_CHECKING:
    from ..cli import BaseContext
from . import views, filters


HERE = Path(__file__).resolve().parent
STATIC_ROOT = HERE / 'static'

routes = web.RouteTableDef()


@dataclass
class WebCliContext:
    parent: BaseContext
    app_conf: AppConfig


@routes.get('/', name='home')
@aiohttp_jinja2.template('home.jinja2')
async def home(request: web.Request):
    return {}


# async def request_processor(request: web.Request):
#     return {'myrequest': request}

class AppDataContext:
    """Context manager for setting up the application data
    """
    update_timeout = 300
    """Update interval to check for updated data files"""
    data_files: DataFiles
    update_task: asyncio.Task|None
    """Task to check for updated data files

    This task is created in when the context is entered and cancelled closed.
    """
    def __init__(self, app: web.Application) -> None:
        self.app = app
        self.s3_client: S3Client|None = None
        self._closing = False
        self.update_task: asyncio.Task|None = None

    async def open(self) -> None:
        await self.__aenter__()

    async def close(self) -> None:
        await self.__aexit__(None, None, None)

    async def __aenter__(self) -> Self:
        use_s3 = self.app[APP_CONF_KEY].use_s3
        if use_s3:
            self.s3_client = S3Client(self.app)
            await self.s3_client.__aenter__()
            self.app[S3ClientKey] = self.s3_client
            self.update_task = asyncio.create_task(self.update_loop())
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._closing = True
        t = self.update_task
        self.update_task = None
        if t is not None:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self.s3_client is not None:
            await self.s3_client.__aexit__(exc_type, exc_value, traceback)

    async def load_app_models(self) -> None:
        """Load the model data

        If :attr:`.config.AppConfig.use_s3` is True, the data files are
        downloaded from S3 using :class:`.s3client.S3Client`.
        """
        config = self.app[ConfigKey]
        data_files: DataFiles
        if self.s3_client is not None:
            await self.s3_client.get_data_files()
            data_files = self.s3_client.data_files_local
        else:
            data_files = {
                'clips': config.data_file,
                'legistar': config.legistar.data_file,
                'legistar_rguid': RGuidLegistarData._get_data_file(config),
            }
        self.data_files = data_files
        self._load_app_models()

    def _load_app_models(self) -> None:
        data_files = self.data_files
        self.app[ClipsKey] = ClipCollection.load(data_files['clips'])
        self.app[LegistarDataKey] = LegistarData.load(data_files['legistar'])
        self.app[RGuidLegistarDataKey] = RGuidLegistarData.load(data_files['legistar_rguid'])

    async def update_loop(self):
        if self.s3_client is None:
            return
        while True:
            await asyncio.sleep(self.update_timeout)
            if not hasattr(self, 'data_files'):
                continue
            if self._closing:
                break
            logger.debug('Checking for updated data files')
            changed = await self.s3_client.get_data_files()
            if not changed:
                continue
            logger.info('Data files have changed. Reloading models')
            self._load_app_models()



@logger.catch(reraise=True)
async def app_data_ctx(app: web.Application):
    ctx = AppDataContext(app)
    async with ctx:
        await ctx.load_app_models()
        yield


def build_app(app_conf: AppConfig, conf: Config|None = None) -> web.Application:
    app = web.Application()
    app[APP_CONF_KEY] = app_conf
    if conf is None:
        conf = Config.load(Config.default_filename)
    app[ConfigKey] = conf
    assert conf.local_timezone_name is not None
    tz = set_local_timezone(conf.local_timezone_name)
    app[TimezoneKey] = tz
    app.cleanup_ctx.append(app_data_ctx)
    app['use_s3'] = app_conf.use_s3
    app['read_only'] = app_conf.read_only

    aiohttp_jinja2.setup(
        app,
        loader=jinja2.PackageLoader(f'{__package__}', 'templates'),
        context_processors=(
            # request_processor,
            aiohttp_jinja2.request_processor,
        ),
        filters=[
            ('datetime_format', filters.datetime_format),
            ('date_format', filters.date_format),
            ('time_format', filters.time_format),
            ('duration_format', filters.duration_format),
            ('snake_case_to_title', filters.snake_case_to_title),
        ]
    )
    env = app[aiohttp_jinja2.APP_KEY]
    env.globals.update({
        'url_query': filters.url_query,
        'static_path': filters.static_path,
        'clip_url': filters.clip_url,
        'legistar_url': filters.legistar_url,
    })
    app.add_routes(routes)
    app.add_routes(views.routes)

    static_routes = [
        web.static('/assets', STATIC_ROOT),
    ]

    if app_conf.serve_static and not app_conf.use_s3:
        static_routes.extend([
            web.static(f'/{conf.out_dir}', conf.out_dir_abs),
            web.static(f'/{conf.legistar.out_dir}', conf.legistar.out_dir_abs),
            web.static(f'/{RGuidLegistarData._get_root_dir(conf)}', RGuidLegistarData._get_root_dir(conf)),
        ])

    app.add_routes(static_routes)
    # app.add_routes([
    #     web.static('/assets', STATIC_ROOT),
    #     web.static(f'/{conf.out_dir}', conf.out_dir_abs),
    #     web.static(f'/{conf.legistar.out_dir}', conf.legistar.out_dir_abs),
    #     web.static(f'/{app[RGuidLegistarDataKey].root_dir}', app[RGuidLegistarDataKey].root_dir),
    # ])
    roots: StaticRoots = {
        'assets': Path('assets'),
        'granicus': conf.out_dir,
        'legistar': conf.legistar.out_dir,
        'legistar_rguid': RGuidLegistarData._get_root_dir(conf),
    }
    app[StaticRootsKey] = roots

    def join_static(root: Path) -> URL:
        return app_conf.static_url.joinpath(str(root))

    root_urls: StaticUrlRoots = {
        'assets': URL('/assets'),
        # 'assets': join_static(roots['assets']),
        'granicus': join_static(roots['granicus']),
        'legistar': join_static(roots['legistar']),
        'legistar_rguid': join_static(roots['legistar_rguid']),
    }
    app[StaticUrlRootsKey] = root_urls

    return app


def init_func(argv):
    app_conf = AppConfig()
    return build_app(app_conf)


@click.group(name='web')
@click.option('-h', '--hostname', default='localhost', show_default=True)
@click.option('-p', '--port', default=8080, show_default=True)
@click.option(
    '--sockfile',
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
    help='Unix socket file to use for the server (if not specified, use TCP)'
)
@click.option('--serve-static/--no-serve-static', default=True, show_default=True)
@click.option('--read-only/--no-read-only', default=True, show_default=True)
@click.option('--static-url', default='/', show_default=True)
@click.option('--use-s3/--no-use-s3', default=False, show_default=True)
@click.option('--s3-data-dir', type=click.Path(file_okay=False, dir_okay=True, path_type=Path))
@click.pass_context
def cli(
    ctx: click.Context,
    hostname: str,
    port: int,
    sockfile: Path|None,
    serve_static: bool,
    read_only: bool,
    static_url: str,
    use_s3: bool,
    s3_data_dir: Path|None
):
    _static_url = URL(static_url)
    if not _static_url.host:
        _static_url = URL(f'http://{hostname}:{port}').join(_static_url)
    conf = AppConfig(
        hostname=hostname, port=port, sockfile=sockfile, serve_static=serve_static,
        read_only=read_only, static_url=_static_url,
        use_s3=use_s3, s3_data_dir=s3_data_dir
    )
    assert ctx.parent is not None
    assert isinstance(ctx.parent.obj.config, Config)
    ctx.obj = WebCliContext(parent=ctx.parent.obj, app_conf=conf)


@cli.command()
@click.option(
    '--launch-browser/--no-launch-browser',
    default=True,
    show_default=True,
    help='Launch a browser window after starting the server'
)
@click.pass_obj
def serve(obj: WebCliContext, launch_browser: bool):
    """Run the webapp and optionally launch a browser window
    """
    parent_obj = obj.parent
    app_conf = obj.app_conf

    async def on_startup(app):
        webbrowser.open(f'http://{app_conf.hostname}:{app_conf.port}')

    app = build_app(app_conf, conf=parent_obj.config)
    if launch_browser and app_conf.sockfile is None:
        app.on_startup.append(on_startup)

    if app_conf.sockfile is not None:
        click.echo(f'Serving on {app_conf.sockfile}')
        web.run_app(app, path=str(app_conf.sockfile))
    else:
        click.echo(f'Serving on http://{app_conf.hostname}:{app_conf.port}')
        web.run_app(app, host=app_conf.hostname, port=app_conf.port)




if __name__ == '__main__':
    cli()

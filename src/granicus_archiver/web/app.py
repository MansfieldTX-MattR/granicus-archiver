from __future__ import annotations
from typing import Self
from pathlib import Path
import webbrowser

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
from . import views, filters


HERE = Path(__file__).resolve().parent
STATIC_ROOT = HERE / 'static'

routes = web.RouteTableDef()

@routes.get('/', name='home')
@aiohttp_jinja2.template('home.jinja2')
async def home(request: web.Request):
    return {}


# async def request_processor(request: web.Request):
#     return {'myrequest': request}

class AppDataContext:
    """Context manager for setting up the application data
    """
    def __init__(self, app: web.Application) -> None:
        self.app = app
        self.s3_client: S3Client|None = None

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
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
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
        self.app[ClipsKey] = ClipCollection.load(data_files['clips'])
        self.app[LegistarDataKey] = LegistarData.load(data_files['legistar'])
        self.app[RGuidLegistarDataKey] = RGuidLegistarData.load(data_files['legistar_rguid'])



async def app_data_ctx(app: web.Application):
    ctx = AppDataContext(app)
    async with ctx:
        await ctx.load_app_models()
        yield


def build_app(app_conf: AppConfig) -> web.Application:
    app = web.Application()
    app[APP_CONF_KEY] = app_conf
    conf = app[ConfigKey] = Config.load(Config.default_filename)
    assert conf.local_timezone_name is not None
    tz = set_local_timezone(conf.local_timezone_name)
    app[TimezoneKey] = tz
    app.cleanup_ctx.append(app_data_ctx)
    app['use_s3'] = app_conf.use_s3

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


@click.group()
@click.option('-h', '--hostname', default='localhost', show_default=True)
@click.option('-p', '--port', default=8080, show_default=True)
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
        hostname=hostname, port=port, serve_static=serve_static,
        read_only=read_only, static_url=_static_url,
        use_s3=use_s3, s3_data_dir=s3_data_dir
    )
    ctx.obj = conf


@cli.command()
@click.option(
    '--launch-browser/--no-launch-browser',
    default=True,
    show_default=True,
    help='Launch a browser window after starting the server'
)
@click.pass_obj
def serve(obj: AppConfig, launch_browser: bool):
    """Run the webapp and optionally launch a browser window
    """
    async def on_startup(app):
        webbrowser.open(f'http://{obj.hostname}:{obj.port}')

    app = build_app(obj)
    if launch_browser:
        app.on_startup.append(on_startup)
    click.echo(f'Serving on http://{obj.hostname}:{obj.port}')
    web.run_app(app, host=obj.hostname, port=obj.port)




if __name__ == '__main__':
    cli()

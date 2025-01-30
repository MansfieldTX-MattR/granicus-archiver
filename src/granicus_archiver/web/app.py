from __future__ import annotations

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

def build_app(app_conf: AppConfig) -> web.Application:
    app = web.Application()
    app[APP_CONF_KEY] = app_conf
    conf = app[ConfigKey] = Config.load(Config.default_filename)
    assert conf.local_timezone_name is not None
    tz = set_local_timezone(conf.local_timezone_name)
    app[TimezoneKey] = tz
    app[ClipsKey] = ClipCollection.load(conf.data_file)
    app[LegistarDataKey] = LegistarData.load(conf.legistar.data_file)
    app[RGuidLegistarDataKey] = RGuidLegistarData.load(RGuidLegistarData._get_data_file(conf))

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
    })
    app.add_routes(routes)
    app.add_routes(views.routes)

    static_routes = [
        web.static('/assets', STATIC_ROOT),
    ]

    if app_conf.serve_static:
        static_routes.extend([
            web.static(f'/{conf.out_dir}', conf.out_dir_abs),
            web.static(f'/{conf.legistar.out_dir}', conf.legistar.out_dir_abs),
            web.static(f'/{app[RGuidLegistarDataKey].root_dir}', app[RGuidLegistarDataKey].root_dir),
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
        'legistar_rguid': app[RGuidLegistarDataKey].root_dir,
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
@click.pass_context
def cli(
    ctx: click.Context,
    hostname: str,
    port: int,
    serve_static: bool,
    read_only: bool,
    static_url: str
):
    _static_url = URL(static_url)
    if not _static_url.host:
        _static_url = URL(f'http://{hostname}:{port}').join(_static_url)
    conf = AppConfig(
        hostname=hostname, port=port, serve_static=serve_static,
        read_only=read_only, static_url=_static_url,
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

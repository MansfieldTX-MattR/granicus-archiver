from __future__ import annotations
import webbrowser
import asyncio
from aiohttp import web
from aiogoogle.client import Aiogoogle
from aiogoogle.auth.utils import create_secret

from . import config
from ..config import Config


ROOT_CONF_KEY = web.AppKey('ROOT_CONF', Config)
AIOGOOGLE_KEY = web.AppKey('Aiogoogle', Aiogoogle)
AUTH_STATE_KEY = web.AppKey('AUTH_STATE', str)
CLIENT_CONF_KEY = web.AppKey('CLIENT_CONF', config.OAuthClientConf)
USER_CREDS_KEY = web.AppKey('USER_CREDS', config.UserCredentials)
CLOSE_EVENT_KEY = web.AppKey('CLOSE_EVENT', asyncio.Event)


LOCAL_ADDRESS = "localhost"
LOCAL_PORT = 5000
REDIRECT_URI = f'http://{LOCAL_ADDRESS}:{LOCAL_PORT}/callback/aiogoogle'

routes = web.RouteTableDef()


@routes.get("/authorize")
async def authorize(request: web.Request):
    aiogoogle = request.app[AIOGOOGLE_KEY]
    client_conf = request.app[CLIENT_CONF_KEY]
    state = request.app[AUTH_STATE_KEY]
    email = request.app[USER_CREDS_KEY].email
    # email = request.app[EMAIL_KEY]
    if aiogoogle.oauth2.is_ready(client_conf._asdict()):
        uri = aiogoogle.oauth2.authorization_url(
            client_creds=client_conf._asdict(),
            state=state,
            access_type="offline",
            include_granted_scopes=True,
            login_hint=email,
            prompt="select_account",
        )
        # Step A
        raise web.HTTPFound(uri)
    # else:
    return web.Response(text="Client doesn't have enough info for Oauth2", status=500)


@routes.get("/callback/aiogoogle")
async def callback(request: web.Request):
    root_conf = request.app[ROOT_CONF_KEY]
    aiogoogle = request.app[AIOGOOGLE_KEY]
    client_conf = request.app[CLIENT_CONF_KEY]
    state = request.app[AUTH_STATE_KEY]
    request.app[CLOSE_EVENT_KEY].set()
    if request.query.get("error"):
        error = {
            "error": request.query.get("error"),
            "error_description": request.query.get("error_description"),
        }
        return web.json_response(error)
    elif request.query.get("code"):
        returned_state = request.query["state"]
        # Check state
        if returned_state != state:
            return web.Response(text="NO", status=500)
        # Step D & E (D send grant code, E receive token info)
        full_user_creds = await aiogoogle.oauth2.build_user_creds(
            grant=request.query.get("code"), client_creds=client_conf._asdict()
        )
        config.save_user_credentials(
            full_user_creds,
            filename=root_conf.google.user_credentials_filename,
        )
        response_txt = '\n'.join([
            'Authorization Complete.'
            f'Credentials saved to {config.USER_CREDENTIALS_FILE}'
            'You may close this browser window'
        ])
        return web.Response(text=response_txt)
    else:
        # Should either receive a code or an error
        return web.Response(text="Something's probably wrong with your callback", status=400)


def build_app(root_conf: Config):
    app = web.Application()
    app[ROOT_CONF_KEY] = root_conf
    app[CLIENT_CONF_KEY] = client_conf = config.OAuthClientConf.load_from_env(REDIRECT_URI)
    app[USER_CREDS_KEY] = config.UserCredentials.load(root_conf=root_conf)
    aiogoogle = Aiogoogle(client_creds=client_conf._asdict())   # type: ignore
    app[AIOGOOGLE_KEY] = aiogoogle
    app[AUTH_STATE_KEY] = create_secret()

    app.add_routes(routes)

    webbrowser.open(f'http://{LOCAL_ADDRESS}:{LOCAL_PORT}/authorize')
    return app


async def run_app(root_conf: Config):
    app = build_app(root_conf=root_conf)
    app[CLOSE_EVENT_KEY] = asyncio.Event()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, LOCAL_ADDRESS, LOCAL_PORT)
    await site.start()

    await app[CLOSE_EVENT_KEY].wait()
    await runner.cleanup()

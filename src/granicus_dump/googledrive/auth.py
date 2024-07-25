import json
import webbrowser
from aiohttp import web
from aiogoogle.client import Aiogoogle
from aiogoogle.auth.creds import UserCreds, ClientCreds
from aiogoogle.auth.utils import create_secret

from . import config


AIOGOOGLE_KEY = web.AppKey('Aiogoogle', Aiogoogle)
AUTH_STATE_KEY = web.AppKey('AUTH_STATE', str)
CLIENT_CONF_KEY = web.AppKey('CLIENT_CONF', config.OAuthClientConf)
USER_CREDS_KEY = web.AppKey('USER_CREDS', config.UserCredentials)


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
    aiogoogle = request.app[AIOGOOGLE_KEY]
    client_conf = request.app[CLIENT_CONF_KEY]
    state = request.app[AUTH_STATE_KEY]
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
        config.USER_CREDENTIALS_FILE.write_text(json.dumps(full_user_creds))
        return web.json_response(full_user_creds)
    else:
        # Should either receive a code or an error
        return web.Response(text="Something's probably wrong with your callback", status=400)


def build_app():
    app = web.Application()
    app[CLIENT_CONF_KEY] = client_conf = config.OAuthClientConf.load_from_env(REDIRECT_URI)
    app[USER_CREDS_KEY] = config.UserCredentials.load()
    aiogoogle = Aiogoogle(client_creds=client_conf._asdict())   # type: ignore
    app[AIOGOOGLE_KEY] = aiogoogle
    app[AUTH_STATE_KEY] = create_secret()

    app.add_routes(routes)

    webbrowser.open(f'http://{LOCAL_ADDRESS}:{LOCAL_PORT}/authorize')
    web.run_app(app, host=LOCAL_ADDRESS, port=LOCAL_PORT)


if __name__ == '__main__':
    build_app()

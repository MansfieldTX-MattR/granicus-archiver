from typing import NamedTuple, Self
import os
from pathlib import Path
import json


USER_CREDENTIALS_FILE = Path.home() / '.granicus-oauth-user.json'

DEFAULT_REDIRECT_URI = 'http://localhost'
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
]

# def get_env_str(key: str) -> str:
#     value = os.environ[key]


class OAuthClientConf(NamedTuple):
    client_id: str
    client_secret: str
    redirect_uri: str = DEFAULT_REDIRECT_URI
    scopes: list[str] = DEFAULT_SCOPES

    @classmethod
    def load_from_env(cls, redirect_uri: str = DEFAULT_REDIRECT_URI) -> Self:
        return cls(
            client_id=os.environ['OAUTH_CLIENT_ID'],
            client_secret=os.environ['OAUTH_CLIENT_SECRET'],
            redirect_uri=redirect_uri,
            scopes=DEFAULT_SCOPES,
        )

# def get_user_email() -> str:
#     return os.environ['OAUTH_CLIENT_EMAIL']

class UserCredentials(NamedTuple):
    email: str
    access_token: str|None = None
    refresh_token: str|None = None
    expires_in: int|None = None
    expires_at: str|None = None
    scopes: list[str]|None = None
    id_token: str|None = None
    id_token_jwt: str|None = None
    token_type: str|None = None
    token_uri: str|None = None
    token_info_uri: str|None = None
    revoke_uri: str|None = None

    @classmethod
    def load(cls) -> Self:
        if USER_CREDENTIALS_FILE.exists():
            kw = json.loads(USER_CREDENTIALS_FILE.read_text())
            kw['email'] = os.environ['OAUTH_CLIENT_EMAIL']
            return cls(**kw)
        return cls(
            email=os.environ['OAUTH_CLIENT_EMAIL'],
        )

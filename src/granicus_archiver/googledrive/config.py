from __future__ import annotations
from typing import NamedTuple, Self, TYPE_CHECKING
import os
from pathlib import Path
import json
import stat
import getpass
from loguru import logger

if TYPE_CHECKING:
    from ..config import Config


ST_PERM_MASK = stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
ST_USER_RW = stat.S_IRUSR | stat.S_IWUSR


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
    def load(cls, root_conf: Config) -> Self:
        cred_file = root_conf.google.user_credentials_filename
        check_file_perms(cred_file)
        if cred_file.exists():
            kw = json.loads(cred_file.read_text())
            kw['email'] = os.environ['OAUTH_CLIENT_EMAIL']
            return cls(**kw)
        return cls(
            email=os.environ['OAUTH_CLIENT_EMAIL'],
        )


def check_file_perms(filename: Path, show_warning: bool = True):
    for p in [filename]:
        if not p.exists():
            continue
        mode = p.stat().st_mode
        cur_perms = mode & ST_PERM_MASK
        if cur_perms == ST_USER_RW:
            continue
        msg = f'Credentials file "{p}" has insecure permissions.'
        if p.owner() != getpass.getuser():
            msg = f'{msg} File not owned by current user, cannot correct'
            show_warning = True
        else:
            msg = f'{msg}  File mode changed'
            new_mode = mode - cur_perms
            new_mode |= ST_USER_RW
            assert new_mode & ST_PERM_MASK == ST_USER_RW
            p.chmod(new_mode)
        if show_warning:
            logger.warning(msg)


def save_user_credentials(data: dict, filename: Path) -> None:
    filename.write_text(json.dumps(data))
    check_file_perms(filename, show_warning=False)

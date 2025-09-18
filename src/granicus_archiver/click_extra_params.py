from __future__ import annotations
from typing import Sequence, Literal, get_args
import sys
from gettext import gettext as _

from loguru import logger
import click_extra
from click_extra.commands import default_extra_params
from click_extra.config import ConfigOption
from click_extra.logging import VerboseOption, VerbosityOption
from click_extra.version import ExtraVersionOption


_LogLevel = Literal[
    'TRACE', 'DEBUG', 'INFO', 'SUCCESS', 'WARNING', 'ERROR', 'CRITICAL',
]
LOG_LEVELS: tuple[_LogLevel, ...] = get_args(_LogLevel)
DEFAULT_LEVEL_NAME: _LogLevel = 'DEBUG'

def set_log_level(level: _LogLevel) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
    )


class LogLevelOption(click_extra.Option):
    def __init__(
        self,
        param_decls: Sequence[str] | None = None,
        default: str = DEFAULT_LEVEL_NAME,
        metavar="LEVEL",
        type=click_extra.Choice(LOG_LEVELS, case_sensitive=False),
        help=_("Either {log_levels}.").format(log_levels=", ".join(LOG_LEVELS)),
        expose_value=False,
        # is_eager=True,
        **kwargs,
    ) -> None:
        if not param_decls:
            param_decls = ["--log-level"]
        kwargs.setdefault('callback', self.set_level)
        super().__init__(
            param_decls,
            default=default,
            metavar=metavar,
            type=type,
            help=help,
            expose_value=expose_value,
            **kwargs,
        )

    def set_level(self, ctx: click_extra.Context, param: click_extra.Parameter, value: str) -> None:
        current_level = ctx.meta.get("log_level")
        if current_level == value:
            return
        assert value in LOG_LEVELS
        ctx.meta["log_level"] = value
        set_log_level(value)


def extra_params():
    extra_params = default_extra_params()
    filtered_cls = (ConfigOption, VerboseOption)
    def iter_parms():
        for param in extra_params:
            if isinstance(param, filtered_cls):
                continue
            if isinstance(param, VerbosityOption):
                param = LogLevelOption()
            elif isinstance(param, ExtraVersionOption):
                param = ExtraVersionOption(
                    package_name='granicus_archiver',
                )
            yield param
    return list(iter_parms())

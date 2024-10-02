from zoneinfo import ZoneInfo

# from .config import Config
from .model import set_timezone as model_set_timezone
from .legistar.rss_parser import set_timezone as legistar_set_timezone



def set_local_timezone(tz: ZoneInfo|str):
    if not isinstance(tz, ZoneInfo):
        tz = ZoneInfo(tz)
    model_set_timezone(tz)
    legistar_set_timezone(tz)

from typing import Literal, Iterator, overload, TYPE_CHECKING
from pathlib import Path

import datetime
from yarl import URL
from loguru import logger
# from pyquery import PyQuery as pq

from .model import CLIP_ID, ParseClipData, ParseClipLinks, ClipCollection

# if TYPE_CHECKING:
from pyquery.pyquery import PyQuery


def parse_clip_dt_str(dt_str: str) -> datetime.datetime:
    tz = ParseClipData.get_zimezone()
    utc = datetime.timezone.utc
    dt_fmt = '%m/%d/%y %H:%M'
    dt_p = datetime.datetime.strptime(dt_str, dt_fmt)
    dt_p = dt_p.replace(tzinfo=utc).astimezone(tz)
    return dt_p



class ParseError(Exception): ...

class NotFound(ParseError): ...

class MultipleResults(ParseError): ...



# def find_one(elem: Element|HTML, selector: str) -> Element:
#     result = elem.find(selector)
#     assert isinstance(result, list)
#     if len(result) > 1:
#         raise MultipleResults('More than one element found')
#     try:
#         e = result[0]
#     except IndexError:
#         raise NotFound()
#     return e

# @overload
# def find(elem: Element|HTML, selector: str, only_one: bool = False) -> list[Element]: ...
# @overload
# def find(elem: Element|HTML, selector: str, only_one: bool = True) -> Element: ...
# def find(elem: Element|HTML, selector: str, only_one: bool = False) -> list[Element]|Element:
#     result = elem.find(selector)
#     assert isinstance(result, list)
#     if only_one:
#         if len(result) > 1:
#             raise KeyError('More than one element found')
#         return result[0]
#     return result

# def find_many(elem: Element|HTML, selector: str) -> list[Element]:
#     result = elem.find(selector)
#     assert isinstance(result, list)
#     return result


def parse_page(response: str|bytes, base_dir: Path, scheme: str, use_dt_str: bool = True) -> ClipCollection:
    doc = PyQuery(response)
    clips = ClipCollection(base_dir=base_dir)
    data_root = doc.find('.list-root > table').eq(0)
    # data_root = find_one(response.html, '.list-root > table')
    # data_root = response.html.find('.list-root > table', first=True)
    # assert isinstance(data_root, Element)
    # a = data_root[0]
    # tbody = data_root.find('tbody', first=True)
    # tbody = find_one(data_root, 'tbody')
    tbody = data_root.find('tbody').eq(0)
    # for clip_row in find_many(tbody, 'tr.clip-row'):
    # clip_rows = tbody.find('tr.clip-row')
    for clip_row in tbody.find('tr.clip-row').items():
        parse_clip = parse_clip_row(clip_row, scheme=scheme, use_dt_str=use_dt_str)
        clips.add_clip(parse_clip)
    return clips

def attr_in(elem: PyQuery, name: str) -> bool:
    value = elem.attr(name)
    return value is not None

def elem_attr(elem: PyQuery, name: str) -> str:
    value = elem.attr(name)
    if value is None:
        raise KeyError(f'attr "{name}" not found')
    assert isinstance(value, str)
    return value


# The value for [data-key="player_link"] is taken from `{$Clip.PlayerPopupJS}`
# and formatted like this:
# window.open('//mansfieldtx.granicus.com/MediaPlayer.php?view_id=6&clip_id=2195','player','toolbar=no,directories=no,status=yes,scrollbars=yes,resizable=yes,menubar=no')
#
# We just care about the first argument to `window.open()`, but there doesn't
# seem to be any better way than just parsing it out

def parse_clip_row(elem: PyQuery, scheme: str, use_dt_str: bool) -> ParseClipData:
    data_type_map = {
        'string':str,
        'integer':int,
        'URL':URL,
    }

    clip_id: CLIP_ID = elem_attr(elem, 'data-clip-id')
    kw = {}
    link_kw = {}
    for td in elem.find('td').items():
        data_key = elem_attr(td, 'data-key')
        data_type = elem_attr(td, 'data-type')
        py_type = data_type_map[data_type]
        if data_key == 'date' and use_dt_str:
            # The generated timestamp from Granicus is off by one day for
            # some clips.  Use the formatted datetime string instead because
            # for some reason that's correct.
            # Why would they need to worry about consistency or correctness?
            str_val = td.text()
            assert isinstance(str_val, str)
            dt = parse_clip_dt_str(str_val)
            str_val = str(int(dt.timestamp()))
        elif attr_in(td, 'data-value'):
            str_val = elem_attr(td, 'data-value')
            if data_key == 'player_link':
                assert str_val.startswith('window.open(') and str_val.endswith(')')
                str_val = str_val.split('window.open(')[1].rstrip(')')
                str_val = str_val.split(',')[0].strip("'").strip('"')
                assert str_val.startswith('//')
                py_type = URL
        elif data_type == 'URL':
            anchor = td.find('a').eq(0)
            if len(anchor):
                str_val = elem_attr(anchor, 'href')
            else:
                link_kw[data_key] = None
                continue
        else:
            str_val = td.text()
            assert isinstance(str_val, str)
            assert len(str_val)

        value = py_type(str_val)
        if data_key == 'id':
            assert value == clip_id
        if isinstance(value, URL):
            if not value.scheme:
                value = value.with_scheme(scheme)
        if data_key in ParseClipLinks.link_attrs:
            link_kw[data_key] = value
        else:
            kw[data_key] = value
    kw['original_links'] = ParseClipLinks(**link_kw)
    return ParseClipData(**kw)



def parse_player_page(response: str|bytes) -> Iterator[tuple[int, str]]:
    """Parse the ``{$Clip.PlayerPopupJS}`` page for timestamps and agenda text

    Results are ``(time_in_seconds, item_text)``
    """
    doc = PyQuery(response)
    index_section = doc.find('#index').eq(0)
    for div_el in index_section.find('div.index-point').items():
        item_time = elem_attr(div_el, 'time')
        time_seconds = int(item_time)
        item_text = div_el.text()
        assert isinstance(item_text, str)
        yield time_seconds, item_text

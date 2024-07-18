from typing import Literal, overload, TYPE_CHECKING
from pathlib import Path

from yarl import URL
# from pyquery import PyQuery as pq

from .model import CLIP_ID, ParseClipData, ParseClipLinks, ClipCollection

# if TYPE_CHECKING:
from pyquery.pyquery import PyQuery




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


def parse_page(response: str|bytes, base_dir: Path, scheme: str) -> ClipCollection:
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
        parse_clip = parse_clip_row(clip_row, scheme=scheme)
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

def parse_clip_row(elem: PyQuery, scheme: str) -> ParseClipData:
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
        if attr_in(td, 'data-value'):
            str_val = elem_attr(td, 'data-value')
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

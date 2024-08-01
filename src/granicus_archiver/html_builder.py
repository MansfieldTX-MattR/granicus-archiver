from __future__ import annotations
from typing import Self, Callable
from pathlib import Path
import datetime

from yarl import URL

from .model import ClipCollection, Clip, ClipFileKey


ElemAttrs = dict[str, str|None]
UrlLike = URL|str
FileLinkCallback = Callable[[Clip, ClipFileKey], UrlLike|None]


def str_indent(s: str, indent: int) -> str:
    pre = ' ' * indent
    return f'{pre}{s}'


class Element:
    def __init__(
        self,
        tag: str,
        attrs: ElemAttrs|None = None,
        content: str|None = None,
        parent: Self|None = None,
        has_close_tag: bool = True
    ) -> None:
        self.tag = tag
        self.attrs = attrs
        self._content = content
        self.parent = parent
        self.has_close_tag = has_close_tag
        self.children: list[Self] = []

    @property
    def content(self) -> str|None:
        return self._content
    @content.setter
    def content(self, value: str|None) -> None:
        if value == self._content:
            return
        if value is not None and len(self.children):
            raise ValueError('Cannot set content if children exist')
        self._content = value

    def add_child(
        self,
        tag: str,
        attrs: ElemAttrs|None = None,
        content: str|None = None,
        has_close_tag: bool = True
    ) -> Self:
        child = self.__class__(
            tag=tag, attrs=attrs, content=content,
            parent=self, has_close_tag=has_close_tag,
        )
        self.append(child)
        return child

    def append(self, child: Self) -> None:
        if self.content is not None:
            raise ValueError('Cannot add child if content is set')
        if child.parent is not self:
            assert child.parent is None
        child.parent = self
        self.children.append(child)

    def extend(self, *children: Self) -> None:
        for child in children:
            self.append(child)

    def clone(self) -> Self:
        children = [c.clone() for c in self.children]
        attrs = self.attrs.copy() if self.attrs else None
        obj = self.__class__(
            tag=self.tag,
            attrs=attrs,
            content=self.content,
            has_close_tag=self.has_close_tag,
        )
        obj.extend(*children)
        return obj

    def _render_attrs(self) -> str:
        if self.attrs is None:
            return ''
        l: list[str] = []
        for key, val in self.attrs.items():
            if val is not None:
                s = f'{key}="{val}"'
            else:
                s = key
            l.append(s)
        return ' '.join(l)

    def render_as_list(self, indent: int = 0) -> list[str]:
        attrs = self._render_attrs()
        if len(attrs):
            attrs = f' {attrs}'
        open_tag = str_indent(f'<{self.tag}{attrs}>', indent)
        close_tag = f'</{self.tag}>'
        if self.content is not None:
            return [
                f'{open_tag}{self.content}{close_tag}'
            ]
        result = [open_tag]
        for c in self.children:
            result.extend(c.render_as_list(indent=indent+2))
        if self.has_close_tag:
            result.append(str_indent(close_tag, indent))
        return result

    def render_as_str(self) -> str:
        return '\n'.join(self.render_as_list())


def DocTypeElement():
    return Element('!DOCTYPE', attrs={'html':None}, has_close_tag=False)

def MetaElement(**attrs: str|None) -> Element:
    return Element('meta', attrs=attrs, has_close_tag=False)

def StyleSheet(href: str) -> Element:
    return Element(
        'link', attrs={'rel':'stylesheet', 'href':href}, has_close_tag=False
    )



header_keys = ['id', 'location', 'name', 'datetime', 'agenda', 'minutes', 'video']


def build_html(
    clips: ClipCollection,
    html_dir: Path,
    link_callback: FileLinkCallback|None = None
) -> str:
    doc = DocTypeElement()
    root = doc.add_child('html')
    head = root.add_child('head')
    head.extend(
        MetaElement(charset='utf-8'),
        MetaElement(name='viewport', content='width=device-width, initial-scale=1'),
        StyleSheet(href='https://cdn.jsdelivr.net/npm/bulma@1.0.1/css/bulma.min.css'),
    )
    body = root.add_child('body')
    section = body.add_child('section', attrs={'class':'section'})
    tbl = build_table(clips, html_dir.resolve(), link_callback)
    section.append(tbl)
    return doc.render_as_str()

def build_table(
    clips: ClipCollection,
    html_dir: Path,
    link_callback: FileLinkCallback|None
) -> Element:
    tbl_root = Element('table', attrs={'class':'table is-bordered is-striped'})
    thead_row = tbl_root.add_child('thead').add_child('tr')
    for key in header_keys:
        thead_row.add_child('th', attrs={'data-key': key}, content=key.title())
    tfoot_row = thead_row.clone()
    tbl_root.add_child('tfoot').append(tfoot_row)
    tbody = tbl_root.add_child('tbody')
    for clip in clips:
        tbody.append(build_clip_row(clip, html_dir, link_callback))
    return tbl_root


def build_clip_row(
    clip: Clip,
    html_dir: Path,
    link_callback: FileLinkCallback|None
) -> Element:
    tr = Element('tr')
    for key in header_keys:
        tag = 'th' if key == 'id' else 'td'
        cell = tr.add_child(tag, attrs={'data-key':key})
        if key in clip.files.path_attrs:
            url: UrlLike|None = None
            if link_callback is not None:
                url = link_callback(clip, key)
                if url is None:
                    continue
            else:
                value = clip.files[key]
                if value is None:
                    continue
                value = clip.parent.base_dir / value
                value = value.resolve()
                if value.exists():
                    url = str(value)
            if url is not None:
                cell.add_child(
                    'a', attrs={'href':str(url)}, content=key.title(),
                )
        else:
            value = getattr(clip.parse_data, key)
            if isinstance(value, datetime.datetime):
                value = ''.join([
                    f'<p>{value.strftime("%m/%d/%Y")}</p>',
                    f'<p>{value.strftime('%I:%M %p')}',
                ])
            elif value is None:
                value = ''
            cell.content = value
    return tr

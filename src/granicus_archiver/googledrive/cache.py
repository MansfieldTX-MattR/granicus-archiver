from __future__ import annotations
from typing import TypeVar, Generic, NamedTuple, Literal, Any, Iterator, Self, overload

from .types import DriveFileMetaFull
from ..clips.model import CLIP_ID, ClipFileUploadKey
from ..legistar.types import GUID, REAL_GUID, LegistarFileUID


IdType = TypeVar('IdType', CLIP_ID, GUID, REAL_GUID)
"""Id of an item within the top-level of :class:`MetaDict`"""
Kt = TypeVar('Kt', ClipFileUploadKey, LegistarFileUID)
"""Sub key for values within an item"""
Vt = TypeVar('Vt', bound=DriveFileMetaFull)


MetaKey = Literal['clips', 'legistar', 'legistar_rguid']
"""Top-level key for :class:`FileCache`"""
ClipCacheKey = tuple[Literal['clips'], CLIP_ID, ClipFileUploadKey]
"""Cache key for clip items"""
LegistarCacheKey = tuple[Literal['legistar'], GUID, LegistarFileUID]
"""Cache key for legistar items"""
RGuidLegistarCacheKey = tuple[Literal['legistar_rguid'], REAL_GUID, LegistarFileUID]
"""Cache key for real guid legistar items"""
MetaCacheKey = ClipCacheKey|LegistarCacheKey|RGuidLegistarCacheKey
"""Unique cache item key (union of :obj:`ClipCacheKey` and :obj:`LegistarCacheKey`)"""



class MetaDict(Generic[IdType, Kt, Vt]):
    """Generic metadata container

    Items are arranged with a top-level dict with :obj:`IdType` as keys and
    values as nested dicts of :obj:`Kt` and :class:`~.types.DriveFileMetaFull`
    """
    _items: dict[IdType, dict[Kt, Vt]]
    def __init__(self, initdict: dict[IdType, dict[Kt, Vt]]|None = None) -> None:
        if initdict is None:
            initdict = {}
        self._items = initdict

    def get(self, key: tuple[IdType, Kt]) -> Vt|None:
        try:
            result = self[key]
        except KeyError:
            result = None
        return result

    def count(self) -> int:
        return sum(len(d) for d in self._items.values())

    @overload
    def __getitem__(self, key: tuple[IdType, Kt]) -> Vt: ...
    @overload
    def __getitem__(self, key: IdType) -> dict[Kt, Vt]: ...
    def __getitem__(self, key: tuple[IdType, Kt]|IdType) -> Vt|dict[Kt, Vt]:
        if not isinstance(key, tuple):
            return self._items[key]
        item_id, item_key = key
        d = self._items[item_id]
        return d[item_key]

    def __setitem__(self, key: tuple[IdType, Kt], value: Vt) -> None:
        item_id, item_key = key
        d = self._items.setdefault(item_id, {})
        d[item_key] = value

    def __delitem__(self, key: tuple[IdType, Kt]|IdType) -> None:
        if not isinstance(key, tuple):
            del self._items[key]
            return
        item_id, item_key = key
        d = self._items[item_id]
        del d[item_key]

    def __contains__(self, key: IdType|tuple[IdType, Kt]) -> bool:
        if isinstance(key, tuple):
            item_id, item_key = key
            if item_id not in self._items:
                return False
            return item_key in self._items[item_id]
        return key in self._items

    def __iter__(self) -> Iterator[IdType]:
        yield from self._items

    def __len__(self):
        return len(self._items)

    def keys(self) -> Iterator[IdType]:
        yield from self._items.keys()

    def values(self) -> Iterator[dict[Kt, Vt]]:
        yield from self._items.values()

    def items(self) -> Iterator[tuple[IdType, dict[Kt, Vt]]]:
        yield from self._items.items()

    def update(self, other: MetaDict[IdType, Kt, Vt]) -> None:
        for item_id in other:
            item_dict = self._items.setdefault(item_id, {})
            other_dict = other[item_id]
            item_dict.update(other_dict)

    def serialize(self):
        ser_keys = set(DriveFileMetaFull.__required_keys__) | set(DriveFileMetaFull.__optional_keys__)
        def serialize_meta(meta: Vt):
            keys = set(meta.keys()) & ser_keys
            return {k: meta[k] for k in keys}

        data = {}
        for item_id, item_dict in self.items():
            d = {k: serialize_meta(v) for k, v in item_dict.items()}
            data[item_id] = d
        return data

    @classmethod
    def deserialize(cls, data: dict[Any, Any]) -> Self:
        return cls(data)


_ClipMetaDict = MetaDict[CLIP_ID, ClipFileUploadKey, DriveFileMetaFull]
_LegistarMetaDict = MetaDict[GUID, LegistarFileUID, DriveFileMetaFull]
_RGuidLegistarMetaDict = MetaDict[REAL_GUID, LegistarFileUID, DriveFileMetaFull]


class MetaCount(NamedTuple):
    items: int
    files: int


class CacheCounts(NamedTuple):
    clips: MetaCount
    legistar: MetaCount
    legistar_rguid: MetaCount


class FileCache:
    """Container for multiple :class:`MetaDict` objects
    """
    _meta_keys: list[MetaKey] = ['clips', 'legistar', 'legistar_rguid']
    def __init__(self) -> None:
        self.clips = _ClipMetaDict()
        self.legistar = _LegistarMetaDict()
        self.legistar_rguid = _RGuidLegistarMetaDict()

    def get_counts(self) -> CacheCounts:
        counts: dict[MetaKey, MetaCount] = {}
        for key in self._meta_keys:
            mdict = self[key]
            counts[key] = MetaCount(
                items=len(mdict),
                files=mdict.count(),
            )
        return CacheCounts(**counts)

    def get(self, cache_key: MetaCacheKey) -> DriveFileMetaFull|None:
        try:
            result = self[cache_key]
        except KeyError:
            result = None
        return result

    @overload
    def __getitem__(self, cache_key: MetaCacheKey) -> DriveFileMetaFull: ...
    @overload
    def __getitem__(self, cache_key: Literal['clips']) -> _ClipMetaDict: ...
    @overload
    def __getitem__(self, cache_key: Literal['legistar']) -> _LegistarMetaDict: ...
    @overload
    def __getitem__(self, cache_key: Literal['legistar_rguid']) -> _RGuidLegistarMetaDict: ...
    def __getitem__(self, cache_key: MetaCacheKey|MetaKey) -> DriveFileMetaFull|_ClipMetaDict|_LegistarMetaDict|_RGuidLegistarMetaDict:
        if not isinstance(cache_key, tuple):
            assert cache_key in self._meta_keys
            return getattr(self, cache_key)
        if cache_key[0] == 'clips':
            return self.clips[cache_key[1:]]
        elif cache_key[0] == 'legistar':
            return self.legistar[cache_key[1:]]
        else:
            assert cache_key[0] == 'legistar_rguid'
            return self.legistar_rguid[cache_key[1:]]

    def __setitem__(self, cache_key: MetaCacheKey, value: DriveFileMetaFull) -> None:
        if cache_key[0] == 'clips':
            self.clips[cache_key[1:]] = value
        elif cache_key[0] == 'legistar':
            self.legistar[cache_key[1:]] = value
        else:
            assert cache_key[0] == 'legistar_rguid'
            self.legistar_rguid[cache_key[1:]] = value

    def __contains__(self, cache_key: MetaCacheKey) -> bool:
        if cache_key[0] == 'clips':
            return cache_key[1:] in self.clips
        elif cache_key[0] == 'legistar':
            return cache_key[1:] in self.legistar
        else:
            assert cache_key[0] == 'legistar_rguid'
            return cache_key[1:] in self.legistar_rguid

    def update(self, other: Self) -> None:
        self.clips.update(other.clips)
        self.legistar.update(other.legistar)
        self.legistar_rguid.update(other.legistar_rguid)

    def keys(self) -> Iterator[MetaKey]:
        yield from self._meta_keys

    def serialize(self):
        return {k: self[k].serialize() for k in self._meta_keys}

    @classmethod
    def deserialize(cls, data: dict[MetaKey, dict]) -> Self:
        obj = cls()
        for key in cls._meta_keys:
            mdict = obj[key]
            if key not in data:
                continue
            mdict._items.update(data[key])
        return obj

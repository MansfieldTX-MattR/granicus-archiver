from __future__ import annotations
from typing import TypeVar, NewType, Literal, get_args, TYPE_CHECKING
import enum

if TYPE_CHECKING:
    from .model import DetailPageResult

GUID = NewType('GUID', str)
"""Globally-Unique ID (but not really in this case)"""
REAL_GUID = NewType('REAL_GUID', str)
"""The part of a :obj:`GUID` that is actually a GUID"""
Category = NewType('Category', str)
"""Feed category"""

LegistarFileKey = Literal['agenda', 'minutes', 'agenda_packet', 'video']
"""Key name for legistar files"""

LegistarFileKeys: tuple[LegistarFileKey, ...] = get_args(LegistarFileKey)

AttachmentName = NewType('AttachmentName', str)
"""Type variable to associate keys in :attr:`.model.DetailPageLinks.attachments`
with the :attr:`~.model.AbstractFile.name` of an :class:`~.model.AttachmentFile`
"""

LegistarFileUID = NewType('LegistarFileUID', str)
"""Unique ID for :obj:`LegistarFileKey` or :obj:`AttachmentName`"""

AgendaStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
"""Status of an agenda"""

MinutesStatus = Literal['Final', 'Final-Addendum', 'Draft', 'Not Viewable by the Public']
"""Status of minutes"""

ItemStatus = Literal['final', 'addendum', 'draft', 'hidden']
"""Status of an item"""

AgendaStatusItems: tuple[AgendaStatus, ...] = get_args(AgendaStatus)
MinutesStatusItems: tuple[MinutesStatus, ...] = get_args(MinutesStatus)



class _DoesNotExistEnum(enum.Enum):
    DoesNotExist = enum.auto()


NoClipT = Literal[_DoesNotExistEnum.DoesNotExist]
"""Type parameter for :obj:`NoClip`"""
NoClip = _DoesNotExistEnum.DoesNotExist
"""Used to signify an item that should have no :class:`~.model.Clip`"""

_GuidT = TypeVar('_GuidT', GUID, REAL_GUID)
_ItemT = TypeVar('_ItemT', bound='DetailPageResult')

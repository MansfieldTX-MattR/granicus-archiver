from __future__ import annotations
from typing import NewType, Literal

GUID = NewType('GUID', str)
"""Globally-Unique ID (but not really in this case)"""
REAL_GUID = NewType('REAL_GUID', str)
"""The part of a :obj:`GUID` that is actually a GUID"""
Category = NewType('Category', str)
"""Feed category"""

LegistarFileKey = Literal['agenda', 'minutes', 'agenda_packet', 'video']
"""Key name for legistar files"""

AttachmentName = NewType('AttachmentName', str)
"""Type variable to associate keys in :attr:`DetailPageLinks.attachments` with
:attr:`AttachmentFile.name`
"""

LegistarFileUID = NewType('LegistarFileUID', str)
"""Unique ID for :obj:`LegistarFileKey` or :obj:`AttachmentName`"""

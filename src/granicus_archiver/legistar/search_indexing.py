from __future__ import annotations
from typing import NamedTuple, TypedDict, Literal, Generator, Iterator
from pathlib import Path
import datetime
from contextlib import contextmanager
import re

from loguru import logger

from pypdf import PdfReader
import whoosh.index as whoosh_index
from whoosh.index import FileIndex
from whoosh.writing import IndexWriter
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.searching import Searcher
from whoosh.qparser.dateparse import DateParserPlugin

from .types import (
    REAL_GUID, LegistarFileUID, Category, is_legistar_file_key,
)
from .rss_parser import is_real_guid
from .model import file_key_to_uid
from .guid_model import RGuidLegistarData, RGuidDetailResult, LegistarFile
from ..config import Config


SchemaTerm = Literal['file_id', 'title', 'category', 'content', 'datetime']
"""Valid terms in the Whoosh schema"""

class SearchResultRaw(TypedDict):
    file_id: str
    category: Category
    page_num: int


class SearchResult(NamedTuple):
    """A single search result from the Whoosh index
    """
    file_id: FileId
    """Unique identifier for the file"""
    category: Category
    """Category of the item"""
    page_num: int
    """Page number in the document where the match was found"""
    matched_terms: list[SchemaTerm]
    """List of schema terms that matched the query"""
    score: float
    """Relevance score of the search result"""



class FileId(NamedTuple):
    """Unique identifier for a Legistar file
    """
    rguid: REAL_GUID
    """"""
    file_uid: LegistarFileUID
    """"""

    @property
    def as_str(self) -> str:
        """String representation of the FileId"""
        return f'{self.rguid}:{self.file_uid}'

    @classmethod
    def from_str(cls, s: str) -> FileId:
        """Create a FileId from its string representation"""
        parts = s.split(':', 1)
        if len(parts) != 2:
            raise ValueError(f'Invalid FileId string: {s}')
        rguid, file_uid = parts
        if not is_real_guid(rguid):
            raise ValueError(f'Invalid REAL_GUID in FileId string: {rguid}')
        if not is_legistar_file_key(file_uid):
            raise ValueError(f'Invalid LegistarFileUID in FileId string: {file_uid}')
        uid = file_key_to_uid(file_uid)
        return cls(rguid, uid)



def build_schema() -> Schema:
    """Build the :mod:`whoosh` schema for indexing Legistar files
    """
    return Schema(
        file_id=ID(stored=True, unique=True),
        category=TEXT(stored=True),
        page_num=NUMERIC(stored=True, sortable=True),
        title=TEXT(stored=False, field_boost=2.5),
        content=TEXT(stored=False),
        datetime=DATETIME(stored=False, sortable=True),
    )


def build_index(index_dir: str|Path) -> FileIndex:
    """Build a :mod:`whoosh` index at the given directory

    If the directory does not exist, it will be created.

    Args:
        index_dir: Directory to store the index
    """

    index_dir = Path(index_dir).resolve()
    index_dir.mkdir(parents=True, exist_ok=True)
    schema = build_schema()
    if whoosh_index.exists_in(index_dir):
        index = whoosh_index.open_dir(index_dir)
    else:
        index = whoosh_index.create_in(index_dir, schema)
    return index




@contextmanager
def get_searcher(index: FileIndex|str|Path) -> Generator[Searcher, None, None]:
    """Context manager to get a searcher
    """
    if isinstance(index, (str, Path)):
        index = build_index(index)
    searcher = index.searcher()
    try:
        yield searcher
    finally:
        searcher.close()


def search_contents(
    query_str: str,
    index: FileIndex|str|Path,
    limit: int = 10,
) -> list[SearchResult]:
    """Search the Whoosh index for the given query string

    Args:
        query_str: Query string to search for
        index: Whoosh index or path to the index directory
        limit: Maximum number of results to return

    Returns:
        A list of :class:`SearchResult` objects
    """
    with get_searcher(index) as searcher:
        qp = QueryParser("content", searcher.schema)
        qp.add_plugin(DateParserPlugin())
        query = qp.parse(query_str)
        results = searcher.search(query, limit=limit, scored=True, terms=True)
        has_matched_terms = results.has_matched_terms()
        result_list = []
        for result in results:
            file_id = FileId.from_str(result['file_id'])
            if has_matched_terms:
                matched_terms = result.matched_terms()
            else:
                matched_terms = []
            score = result.score
            assert score is not None
            result_list.append(SearchResult(
                file_id=file_id,
                category=result['category'],
                page_num=result['page_num'],
                matched_terms=matched_terms,
                score=score,
            ))
        result_list.sort(key=lambda r: r.score, reverse=True)
        return result_list


def add_document(
    file_id: FileId,
    category: Category,
    title: str,
    content: str,
    dt: datetime.datetime,
    page_num: int,
    writer: IndexWriter,
) -> None:
    """Add a document to the index

    .. note:: The document is not committed until :meth:`whoosh.IndexWriter.commit` is called.

    """
    writer.add_document(
        file_id=file_id.as_str,
        category=category,
        title=title,
        content=content,
        page_num=page_num,
        datetime=dt,
    )


def document_exists(
    file_id: FileId,
    index: FileIndex,
    searcher: Searcher,
) -> bool:
    """Check if a document with the given file_id exists in the index
    """
    query = QueryParser('file_id', index.schema).parse(f'"{file_id.as_str}"')
    results = searcher.search(query, limit=1)
    return len(results) > 0



def iter_files_for_item(
    item: RGuidDetailResult,
) -> Iterator[LegistarFile]:
    """Iterate over the Legistar files for a given Legistar item"""
    for file_item in item.files:
        if file_item.name != 'agenda':
            continue
        assert isinstance(file_item, LegistarFile)
        yield file_item


def index_legistar_item(
    writer: IndexWriter,
    legistar_item: RGuidDetailResult,
) -> tuple[int, set[FileId]]:
    """Index a Legistar item into the index

    Returns:
        A tuple of (number of documents indexed, set of :class:`FileId` objects indexed)
    """
    count = 0
    file_ids = set[FileId]()
    tz = legistar_item.feed_item.get_timezone()
    for file_item in iter_files_for_item(legistar_item):
        file_id = FileId(
            rguid=legistar_item.real_guid,
            file_uid=file_item.uid,
        )

        filename = legistar_item.files.get_file_path(file_item.uid, absolute=True)
        body_pages = extract_pdf_text(infile=filename)

        title = legistar_item.feed_item.title
        meeting_date = legistar_item.feed_item.meeting_date.astimezone(tz)

        # make meeting_date naive for indexing
        meeting_date = meeting_date.replace(tzinfo=None)
        title = f'{title} on {meeting_date.strftime("%Y-%m-%d")}'
        for page_num, body in enumerate(body_pages, start=1):
            add_document(
                file_id=file_id,
                category=legistar_item.feed_item.category,
                title=title,
                content=body,
                page_num=page_num,
                dt=meeting_date,
                writer=writer,
            )
        logger.debug(f'Indexed file {file_id.as_str} with title "{title}"')
        count += 1
        file_ids.add(file_id)
    return count, file_ids


def extract_pdf_text(infile: Path|str) -> list[str]:
    """Extract text from a pdf file using layout mode

    See :meth:`pypdf.PageObject.extract_text` for details.

    Arguments:
        infile: The input PDF file


    Returns:
        A list of strings, one per page in the PDF

    """
    reader = PdfReader(infile)
    parts: list[str] = []
    # footer lines:
    # CITY OF ...      ...      Printed on ... Page
    footer_pattern = re.compile(r'CITY OF .* Printed on .*Page \d+')
    def strip_footer(text: str):
        for line in text.splitlines():
            _line = line.strip()
            if footer_pattern.match(_line):
                # logger.debug(f'Stripping footer line: {line=}')
                continue
            yield line

    def strip_trailing_newlines(text: str) -> str:
        lines = text.splitlines()
        for line in reversed(lines):
            if len(line.strip()):
                break
            lines.pop()
        return '\n'.join(lines)

    for page in reader.pages:
        text = page.extract_text(extraction_mode='layout')
        text = '\n'.join(strip_footer(text))
        text = strip_trailing_newlines(text)
        parts.append(text)

    return parts


@logger.catch(reraise=True)
def index_legistar_items(
    config: Config,
    max_docs: int|None,
) -> None:
    """Index Legistar items into the index

    Args:
        config: Configuration object
        max_docs: Maximum number of documents to index in this run.
            If None, index all documents.
    """
    max_docs_before_commit = 50
    commits_pending = 0
    index_dir = config.legistar.search_index_dir
    logger.info(f'Indexing Legistar items into {index_dir}')
    index = build_index(index_dir)
    writer = index.writer()
    count = 0
    data_file = RGuidLegistarData._get_data_file(config)
    legistar_data = RGuidLegistarData.load(data_file)

    all_file_ids = set[FileId]()
    for rguid, item in legistar_data.items():
        for file_item in iter_files_for_item(item):
            fid = FileId(
                rguid=rguid,
                file_uid=file_item.uid,
            )
            all_file_ids.add(fid)
    file_ids_added = set[FileId]()
    # with index.searcher() as searcher:
    with get_searcher(index) as searcher:
        for fid in all_file_ids:
            if document_exists(fid, index, searcher):
                file_ids_added.add(fid)
    logger.info(f'Total files known: {len(all_file_ids)}, already indexed: {len(file_ids_added)}')


    for rguid, item in legistar_data.items():
        if max_docs is not None and count >= max_docs:
            break
        fid = FileId(
            rguid=rguid,
            file_uid=file_item.uid,
        )
        if fid in file_ids_added:
            continue
        _count, ids_added = index_legistar_item(
            writer=writer,
            legistar_item=item,
        )
        count += _count
        file_ids_added |= ids_added
        commits_pending += _count
        # if commits_pending >= max_docs_before_commit:
        #     writer.commit()
        #     writer = index.writer()
        #     commits_pending = 0
        #     logger.debug(f'Committed {max_docs_before_commit} items to index')
        #     pct_complete = (len(file_ids_added) / len(all_file_ids)) * 100 if len(all_file_ids) > 0 else 100
        #     logger.info(f'Indexing progress: {pct_complete:.2f}%')
        if commits_pending % 100 == 0:
            pct_complete = (len(file_ids_added) / len(all_file_ids)) * 100 if len(all_file_ids) > 0 else 100
            logger.info(f'Indexing progress: {pct_complete:.2f}%')



    pct_complete = (len(file_ids_added) / len(all_file_ids)) * 100 if len(all_file_ids) > 0 else 100
    logger.info(f'Indexing progress: {pct_complete:.2f}%')
    if commits_pending > 0:
        writer.commit()
    # writer.wait_merging_threads()
    logger.success(f'Indexed {count} items into {index_dir.relative_to(Path.cwd())}')
    logger.info(f'Total files known: {len(all_file_ids)}, items remaining: {len(all_file_ids - file_ids_added)}')

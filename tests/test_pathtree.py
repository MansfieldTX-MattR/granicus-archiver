from __future__ import annotations
from typing import NamedTuple, Iterator
from pathlib import Path
import itertools
import string

import pytest

from granicus_archiver.googledrive.types import FolderCache, FileId
from granicus_archiver.googledrive.pathtree import PathNode, NodePos


class FakePathsResult(NamedTuple):
    paths: list[Path]
    part_count: int
    n_levels: int
    n_branches: int
    branch_counts: list[int]


@pytest.fixture
def fake_paths() -> FakePathsResult:
    n_levels = 3
    n_branches = 4
    path_count_expected = 1 + sum(
        [n_branches ** i for i in range(1, n_levels+1)]
    )
    root_path = Path('a')
    part_count = 1
    paths: list[Path] = [root_path]
    path_chars = string.ascii_lowercase[:n_branches]
    path_args = [path_chars]
    branch_counts = [1]
    for i in range(n_levels):
        _branch_count = 0
        for parts in itertools.product(*path_args):
            part_count += len(parts)
            p = root_path / Path(*parts)
            paths.append(p)
            _branch_count += 1
        branch_counts.append(_branch_count)
        path_args.append(path_chars)
    assert len(paths) == path_count_expected
    assert len(paths) == len(set(paths))
    return FakePathsResult(paths, len(paths), n_levels, n_branches, branch_counts)


def test_tree_build(fake_paths):
    paths = fake_paths.paths
    fc: FolderCache = {}
    root = PathNode.create_from_paths(*paths, folder_cache=fc)
    assert len(root) == fake_paths.n_branches
    assert root.count() == fake_paths.part_count

    assert root.cost == root.count()
    for node in root.walk():
        # print(f'{node.node_pos=}')
        if node.nest_level == fake_paths.n_levels:
            assert len(node) == 0
        else:
            assert len(node) == fake_paths.n_branches
        assert node.full_path in paths
        assert node.root is root
        assert node.folder_cache is fc
    for path in paths:
        node = root.find(path)
        assert node is not None


def test_node_pos_uniqueness(fake_paths):
    paths = fake_paths.paths
    fc: FolderCache = {}
    root = PathNode.create_from_paths(*paths, folder_cache=fc)

    node_positions: dict[NodePos, PathNode] = {}
    node_paths: dict[Path, PathNode] = {}

    for node in root.walk():
        print(f'{node.node_pos=}')
        assert node.node_pos not in node_positions
        node_positions[node.node_pos] = node
        assert node.full_path not in node_paths
        node_paths[node.full_path] = node
    assert set(node_paths) == set(paths)


def test_cache_update(fake_paths):
    paths = fake_paths.paths
    fc: FolderCache = {
        Path('a/a'): FileId('aa'),
    }
    cost_expected = fake_paths.part_count - 1
    root = PathNode.create_from_paths(*paths, folder_cache=fc)
    assert root.cost == cost_expected

    node = root.find(Path('a/a'))
    assert node is not None
    assert node.folder_id == FileId('aa')

    fc[Path('a/c/c/c')] = FileId('accc')
    root.update_from_cache()
    cost_expected -= 1
    assert root.cost == cost_expected

    node = root.find(Path('a/c/c/c'))
    assert node is not None
    assert node.cost == 0
    assert node.folder_id == FileId('accc')
    while node.parent is not None:
        assert node.cost == node.count() - 1
        node = node.parent


def test_walk_breadth_first(fake_paths):
    paths = fake_paths.paths
    fc: FolderCache = {}
    root = PathNode.create_from_paths(*paths, folder_cache=fc)

    branch_counts = fake_paths.branch_counts

    cur_level = 0
    cur_branch = 0
    parts_expected = 1
    for node in root.walk(breadth_first=True):
        print(cur_level, cur_branch, node)
        assert node.nest_level == cur_level
        assert len(node.full_path.parts) == parts_expected
        if node.parent is None:
            assert cur_level == 0
            cur_level += 1
            parts_expected += 1
            continue
        cur_branch += 1
        if cur_branch == branch_counts[cur_level]:
            cur_level += 1
            parts_expected += 1
            cur_branch = 0

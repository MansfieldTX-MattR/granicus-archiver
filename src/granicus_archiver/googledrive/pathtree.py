from __future__ import annotations
from typing import NewType, NamedTuple, Self, Iterator
from os import PathLike
from pathlib import Path

from .types import FileId, FolderCache

__all__ = ('PathNode', 'PathPart', 'NodeId', 'NodePos')

PathPart = NewType('PathPart', str)
"""Type alias used for :attr:`PathNode.part`"""

NodeId = NewType('NodeId', str)
"""Type alias used for :attr:`PathNode.id`"""


class NodePos(NamedTuple):
    """Position of a :class:`PathNode` within its tree
    """
    id: NodeId
    """The node's :attr:`~PathNode.id`"""
    level: int
    """The node's :attr:`~PathNode.nest_level`"""


class PathNode:
    """Node representing one segment of a collection of filesystem paths

    This is primarily used to aid in creating multiple folders within Drive
    while avoiding costly lookups for each parent folder's :obj:`~.types.FileId`.

    Instead, the :attr:`folder_id` for each known node in the tree is read
    from the :attr:`folder_cache`.  Nodes without a :attr:`folder_id` will
    increase the :attr:`cost` of the node itself and its ancestors.

    Nodes with higher :attr:`cost` can then be prioritized during folder
    creation, setting their :attr:`folder_id` once it is known.

    Standard container methods are supported (aside from
    ``__setitem__`` and ``__delitem__``)

    Example:
        >>> a = PathNode('a', folder_cache={})
        >>> len(a)
        0
        >>> 'b' in a
        False
        >>> b = a.add('b')
        >>> len(a)
        1
        >>> 'b' in a
        True
        >>> a['b'] is b
        True
        >>> foo = a.add('foo')
        >>> len(a)
        2
        >>> [item for item in a]
        [<PathNode: "a/b">, <PathNode: "a/foo">]

    Arguments:
        part: The path :attr:`part <pathlib.PurePath.parts>` for the node
        parent: The node's parent (or ``None`` for the root node)
        folder_cache: The :obj:`~.types.FolderCache` for the entire tree.
            This is only required/valid for the root node.

    """
    part: PathPart
    """The path :attr:`part <pathlib.PurePath.parts>`"""
    full_path: Path
    """The full path as a combination of all :attr:`parent` :attr:`parts <part>`"""
    parent: Self|None
    """The parent node (or ``None`` if this is the :attr:`root` node)"""
    nest_level: int
    """The depth of this node from its :attr:`root` (with the root node
    beginning at ``0``)
    """
    children: dict[PathPart, PathNode]
    """Direct children of this node
    """
    def __init__(
        self,
        part: PathPart,
        parent: Self|None = None,
        folder_cache: FolderCache|None = None
    ) -> None:
        self.part = part
        self.parent = parent
        if self.parent is None:
            self.full_path = Path(part)
            self.nest_level = 0
        else:
            self.full_path = self.parent.full_path / part
            self.nest_level = self.parent.nest_level + 1
        if folder_cache is None:
            assert self.parent is not None
        self._folder_cache = folder_cache
        self.children = {}
        self._folder_id = self.folder_cache.get(self.full_path)
        self._cost = 1 if self.folder_id is None else 0

    @classmethod
    def create_from_paths(cls, *paths: PathLike, folder_cache: FolderCache) -> PathNode:
        """Create a root node from the given path(s)

        If multiple paths are given, they must all have a common root directory.

        Arguments:
            *paths: One or many :class:`~pathlib.Path` instances sharing a
                common root
            folder_cache: The value to set for :attr:`folder_cache` on the
                root node

        Example:
            >>> root = PathNode.create_from_paths('a/a', 'a/b', 'a/c', folder_cache={})
            >>> root
            <PathNode: "a">
            >>> aa = root['a']
            >>> aa
            <PathNode: "a/a">
            >>> [node for node in root.walk()]
            [<PathNode: "a">, <PathNode: "a/a">, <PathNode: "a/b">, <PathNode: "a/c">]

        """
        real_paths = [Path(p) for p in paths]
        roots = set([p.parts[0] for p in real_paths])
        if len(roots) != 1:
            raise ValueError('No common root path found')
        root_part = PathPart(roots.pop())
        root = PathNode(part=root_part, folder_cache=folder_cache)
        for path in real_paths:
            root.add(path)
        return root

    @property
    def id(self) -> NodeId:
        """Unique id for the node
        """
        return NodeId(str(self.full_path))

    @property
    def node_pos(self) -> NodePos:
        """A :class:`namedtuple <NodePos>` of :attr:`id` and :attr:`nest_level`
        """
        return NodePos(self.id, self.nest_level)

    @property
    def root(self) -> Self:
        """The root of the tree
        """
        if self.parent is None:
            return self
        return self.parent.root

    @property
    def folder_id(self) -> FileId|None:
        """The :obj:`~.types.FileId` of the folder matching :attr:`full_path`
        (if it exists)
        """
        return self._folder_id
    @folder_id.setter
    def folder_id(self, value: FileId|None) -> None:
        if value == self._folder_id:
            return
        self._folder_id = value
        self._cost = 1 if value is None else 0

    @property
    def cost(self) -> int:
        """The total number of the node's descendants with no :attr:`folder_id`
        (including the node itself)

        Example:
            >>> a = PathNode.create_from_paths('a/b/c', folder_cache={})
            >>> b = a['b']
            >>> c = b['c']
            >>> nodes = [a, b, c]

            Each node at this point has a local "cost" of ``1`` which is added to
            its parent's overall cost.

            >>> for node in nodes:
            ...     print(node.cost, node.folder_id, node)
            3 None a
            2 None a/b
            1 None a/b/c

            >>> c.folder_id = 'c_id'

            The last descendant now has a folder_id which reduces the cost up the tree

            >>> for node in nodes:
            ...     print(node.cost, node.folder_id, node)
            2 None a
            1 None a/b
            0 c_id a/b/c

            >>> a.folder_id = 'a_id'

            The root node now has a folder_id, but still has the cost of its children
            added.

            >>> for node in nodes:
            ...     print(node.cost, node.folder_id, node)
            1 a_id a
            1 None a/b
            0 c_id a/b/c

            >>> b.folder_id = 'b_id'

            Now all nodes have folder id's with a cost of zero.

            >>> for node in nodes:
            ...     print(node.cost, node.folder_id, node)
            0 a_id a
            0 b_id a/b
            0 c_id a/b/c

        """
        c = self._cost
        for child in self:
            c += child.cost
        return c

    @property
    def folder_cache(self) -> FolderCache:
        """:obj:`~.types.FolderCache` used to populate the :attr:`folder_id`
        for all nodes in the tree

        This is only stored on the :attr:`root` node and will persist across the
        tree.  This allows cache updates to be re-read by all nodes.
        """
        fc = self.root._folder_cache
        assert fc is not None
        return fc

    def add(self, item: PathPart|Path) -> PathNode:
        """Add a node (or nodes) to *self* from the given path

        If the given *item* is a :class:`~pathlib.Path` instance, it must
        contain this node's :attr:`part` at its root.  Any sub-directories
        will be created in the tree and the node representing the final
        element of the path will be returned.

        If *item* is a :obj:`PathPart`, a child of *self* will be added and
        returned with its :attr:`part` set to that value.

        Example:
            >>> from pathlib import Path
            >>> a = PathNode('a', folder_cache={})
            >>> b = a.add('b')
            >>> c = b.add('c')
            >>> a, b, c
            (<PathNode: "a">, <PathNode: "a/b">, <PathNode: "a/b/c">)

            This fails since the root is not included

            >>> foobar = a.add(Path('foo') / 'bar')
            Traceback (most recent call last):
                ...
            ValueError: path "foo/bar" is not relative to "a"

            >>> foobar = a.add(Path('a') / 'foo' / 'bar')
            >>> foobar
            <PathNode: "a/foo/bar">

        """
        if isinstance(item, Path):
            parts = [PathPart(p) for p in item.parts]
            if parts[0] != self.part:
                raise ValueError(f'path "{item}" is not relative to "{self.full_path}"')
            if len(parts) == 1:
                return self
            parts = parts[1:]
            child = self.get(parts[0])
            if child is None:
                child = self.add(parts[0])
            return child.add(Path(*parts))
        if item in self:
            return self[item]
        child = PathNode(item, parent=self)
        self.children[item] = child
        return child

    def find(self, path: PathLike|NodeId) -> PathNode|None:
        """Find a node with its :attr:`full_path` matching the given path

        The search will include *self* and all of its descendants.  If no
        match is found, ``None`` is returned.

        Example:
            >>> a = PathNode('a', folder_cache={})
            >>> b = a.add('b')
            >>> c = b.add('c')
            >>> a.find('a/b') is b
            True
            >>> a.find('a/b/c') is c
            True

        """
        if not isinstance(path, Path):
            path = Path(path)
        for node in self.walk():
            if node.full_path == path:
                return node

    def update_from_cache(self) -> None:
        """Update from the :attr:`folder_cache` for this node and all of its
        descendants

        This method should be called after any updates to the cache are known
        to have occurred.

        The :attr:`path <node_path>` for each node is searched for on the cache
        and if found, sets the :attr:`folder_id`.  This in turn will update
        the :attr:`cost` attribute for each affected node in the tree.

        Example:
            >>> folder_cache = {}
            >>> a = PathNode.create_from_paths('a/b/c', folder_cache=folder_cache)
            >>> b = a['b']
            >>> c = b['c']
            >>> nodes = [a, b, c]

            >>> for node in nodes:
            ...     print(node.cost, node.folder_id, node)
            3 None a
            2 None a/b
            1 None a/b/c

            Set the cached id for Path('a/b/c') then update the tree

            >>> folder_cache[Path('a/b/c')] = 'c_id'
            >>> a.update_from_cache()

            >>> for node in nodes:
            ...     print(node.cost, node.folder_id, node)
            2 None a
            1 None a/b
            0 c_id a/b/c

        """
        self.folder_id = self.folder_cache.get(self.full_path)
        for child in self:
            child.update_from_cache()

    def count(self) -> int:
        """Get the total number of descendants at this point in the tree
        (including *self*)

        Example:
            >>> a = PathNode.create_from_paths('a/b/c', folder_cache={})
            >>> a.count()
            3
            >>> foo = a.add('foo')
            >>> a.count()
            4
            >>> bar = foo.add('bar')
            >>> foo.count()
            2
            >>> a.count()
            5

        """
        i = 1
        for child in self:
            i += child.count()
        return i

    def get(self, key: PathPart) -> PathNode|None:
        """Get the child of *self* matching :attr:`part`

        If not found, ``None`` is returned.
        """
        return self.children.get(key)

    def __getitem__(self, key: PathPart) -> PathNode:
        return self.children[key]

    def __contains__(self, key: PathPart) -> bool:
        return key in self.children

    def __len__(self) -> int:
        return len(self.children)

    def __iter__(self) -> Iterator[PathNode]:
        yield from self.children.values()

    def walk_non_cached(self, breadth_first: bool = False) -> Iterator[PathNode]:
        for node in self.walk(breadth_first=breadth_first):
            if node.folder_id is None:
                yield node

    def walk(self, breadth_first: bool = False) -> Iterator[PathNode]:
        """Recursively iterate over all descendants from this point in the tree

        If *breadth_first* is ``True``, the child nodes for each :attr:`nest_level`
        will be iterated before advancing deeper in the tree.

        If *breadth_first* is ``False`` (default), all descendants will be
        iterated to their deepest :attr:`nest_level` before advancing to the
        next branch (depth-first).

        Example:
            >>> paths = ['a/a/a', 'a/a/b', 'a/b/a', 'a/b/b', 'a/c']
            >>> root = PathNode.create_from_paths(*paths, folder_cache={})

            >>> for node in root.walk():
            ...     print(node)
            a
            a/a
            a/a/a
            a/a/b
            a/b
            a/b/a
            a/b/b
            a/c

            >>> for node in root.walk(breadth_first=True):
            ...     print(node)
            a
            a/a
            a/b
            a/c
            a/a/a
            a/a/b
            a/b/a
            a/b/b

        """
        def get_breadth_iters(node: PathNode) -> dict[NodePos, PathNode]:
            node_iters: dict[NodePos, PathNode] = {}
            node_iters[node.node_pos] = node
            for child in node:
                node_iters.update(get_breadth_iters(child))
            return node_iters

        if breadth_first:
            node_iters = get_breadth_iters(self)
            cur_level = self.nest_level
            keys = list(node_iters.keys())
            while len(keys):
                lvl_keys = [key for key in keys if key.level == cur_level]
                if not len(lvl_keys):
                    assert not len(keys)
                for key in lvl_keys:
                    node = node_iters[key]
                    yield node
                    keys.remove(key)
                cur_level += 1
        else:
            yield self
            for child in self:
                yield from child.walk()

    def __repr__(self) -> str:
        return f'<PathNode: "{self}">'

    def __str__(self) -> str:
        return str(self.full_path)

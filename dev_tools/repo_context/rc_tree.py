"""Deterministic repository_tree.txt generation."""
from __future__ import annotations

from rc_common import atomic_write_text


def _sort_key(name: str):
    return (name.lower(), name)


def _human_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


class _Node:
    __slots__ = ("name", "children", "file", "excluded_reason")

    def __init__(self, name: str):
        self.name = name
        self.children: dict = {}
        self.file = None
        self.excluded_reason = None


def _build_tree(files, dir_exclusions, show_excluded_dirs: bool) -> _Node:
    root = _Node("")
    for f in files:
        if not f.included:
            continue
        parts = f.relative_path.split("/")
        node = root
        for part in parts[:-1]:
            node = node.children.setdefault(part, _Node(part))
        leaf = node.children.setdefault(parts[-1], _Node(parts[-1]))
        leaf.file = f
    if show_excluded_dirs:
        for rel_posix, name, reason in dir_exclusions:
            parts = rel_posix.split("/")
            node = root
            for part in parts[:-1]:
                node = node.children.setdefault(part, _Node(part))
            leaf = node.children.setdefault(parts[-1], _Node(parts[-1]))
            if leaf.file is None:
                leaf.excluded_reason = reason
    return root


def _render(node: _Node, prefix: str, depth: int, max_depth, lines: list) -> None:
    names = sorted(node.children.keys(), key=_sort_key)
    for name in names:
        child = node.children[name]
        if child.excluded_reason is not None:
            lines.append(f"{prefix}{name}/  [excluded: {child.excluded_reason}]")
            continue
        if child.file is not None:
            f = child.file
            if f.line_count is not None:
                lines.append(f"{prefix}{name}  ({_human_size(f.size_bytes)}, {f.line_count} lines)")
            else:
                lines.append(f"{prefix}{name}  ({_human_size(f.size_bytes)}, binary)")
            continue
        # directory
        lines.append(f"{prefix}{name}/")
        if max_depth is not None and depth + 1 >= max_depth:
            if child.children:
                lines.append(f"{prefix}  ... (max depth reached, contents omitted)")
            continue
        _render(child, prefix + "  ", depth + 1, max_depth, lines)


def generate_tree_text(result, root_name: str, max_depth=None, show_excluded_dirs: bool = False) -> str:
    tree = _build_tree(result.files, result.dir_exclusions, show_excluded_dirs)
    lines = [f"{root_name}/"]
    _render(tree, "  ", 0, max_depth, lines)
    return "\n".join(lines) + "\n"


def write_tree(output_dir, result, root_name: str, max_depth=None, show_excluded_dirs: bool = False) -> None:
    text = generate_tree_text(result, root_name, max_depth, show_excluded_dirs)
    atomic_write_text(output_dir / "repository_tree.txt", text)

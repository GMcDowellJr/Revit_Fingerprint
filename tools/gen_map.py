#!/usr/bin/env python3
"""Generate deterministic Python navigation maps for any repository or folder."""

from __future__ import annotations

import argparse
import ast
import os
import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


EXCLUDE_DIRS = frozenset({
    ".git", ".hg", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".svn",
    ".tox", ".venv", "__pycache__", "build", "dist", "env", "node_modules",
    "venv",
})
ENTRYPOINT_NAMES = ("main", "cli", "run", "app")


@dataclass(frozen=True, order=True)
class DefInfo:
    file_rel: str
    lineno: int
    qualname: str
    kind: str

    @property
    def name(self) -> str:
        return self.qualname.rsplit(".", 1)[-1]


@dataclass(frozen=True, order=True)
class CallSite:
    file_rel: str
    lineno: int
    caller: str
    callee: str


@dataclass
class FileInfo:
    imports: list[str]
    definitions: list[DefInfo]
    calls: list[CallSite]


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "python"


def _excluded_dir(name: str, extra: set[str]) -> bool:
    return name in EXCLUDE_DIRS or name in extra or name.startswith(".")


def iter_py_files(root: Path, extra_excludes: Iterable[str] = ()) -> list[Path]:
    """Return stable Python paths without assuming a package layout."""
    root = root.resolve()
    extra = set(extra_excludes)
    paths: list[Path] = []
    for current, dirs, files in os.walk(root):
        dirs[:] = sorted(d for d in dirs if not _excluded_dir(d, extra))
        for name in sorted(files):
            if name.endswith(".py"):
                paths.append(Path(current, name))
    return sorted(paths, key=lambda path: path.relative_to(root).as_posix())


def _dotted_name(node: ast.AST) -> str | None:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


class _Analyzer(ast.NodeVisitor):
    def __init__(self, file_rel: str) -> None:
        self.file_rel = file_rel
        self.scope: list[str] = []
        self.definitions: list[DefInfo] = []
        self.calls: list[CallSite] = []

    def _visit_definition(self, node: ast.FunctionDef | ast.AsyncFunctionDef, kind: str) -> None:
        self.scope.append(node.name)
        self.definitions.append(DefInfo(self.file_rel, node.lineno, ".".join(self.scope), kind))
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_definition(node, "method" if self.scope else "function")

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_definition(node, "method" if self.scope else "function")

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.definitions.append(DefInfo(self.file_rel, node.lineno, ".".join(self.scope), "class"))
        self.generic_visit(node)
        self.scope.pop()

    def visit_Call(self, node: ast.Call) -> None:
        callee = _dotted_name(node.func)
        if callee:
            self.calls.append(CallSite(
                self.file_rel, getattr(node, "lineno", 0),
                ".".join(self.scope) or "<module>", callee,
            ))
        self.generic_visit(node)


def _imports(module: ast.Module) -> list[str]:
    values: list[str] = []
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            values.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module_name = "." * node.level + (node.module or "")
            values.append(f"{module_name}:{','.join(alias.name for alias in node.names)}")
    return sorted(values)


def build_index(root: Path, extra_excludes: Iterable[str] = ()) -> tuple[dict[str, FileInfo], list[str]]:
    files: dict[str, FileInfo] = {}
    warnings: list[str] = []
    for path in iter_py_files(root, extra_excludes):
        relative = path.relative_to(root.resolve()).as_posix()
        try:
            module = ast.parse(path.read_text(encoding="utf-8-sig"), filename=relative)
        except (OSError, UnicodeError, SyntaxError) as exc:
            warnings.append(f"{relative}: {type(exc).__name__}: {exc}")
            continue
        analyzer = _Analyzer(relative)
        analyzer.visit(module)
        files[relative] = FileInfo(_imports(module), sorted(analyzer.definitions), sorted(analyzer.calls))
    return files, sorted(warnings)


def _definitions(files: dict[str, FileInfo]) -> dict[str, list[DefInfo]]:
    result: dict[str, list[DefInfo]] = defaultdict(list)
    for info in files.values():
        for definition in info.definitions:
            result[definition.name].append(definition)
    return {name: sorted(defs) for name, defs in sorted(result.items())}


def _calls(files: dict[str, FileInfo]) -> dict[str, list[CallSite]]:
    result: dict[str, list[CallSite]] = defaultdict(list)
    for info in files.values():
        for call in info.calls:
            result[call.callee.rsplit(".", 1)[-1]].append(call)
    return {name: sorted(calls) for name, calls in sorted(result.items())}


def _write(path: Path, lines: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_code_map(path: Path, root_name: str, files: dict[str, FileInfo], warnings: list[str]) -> None:
    lines = [f"# {root_name} — authoritative Python code map", "", "## Scope", "",
             "Deterministic AST inventory of Python imports and definitions.", "", "## Files", ""]
    for file_rel, info in sorted(files.items()):
        lines.append(f"### `{file_rel}`")
        if info.imports:
            lines.extend(["", "**Imports**", *[f"- `{value}`" for value in info.imports]])
        if info.definitions:
            lines.extend(["", "**Definitions**"])
            lines.extend(f"- `{item.qualname}` ({item.kind}, L{item.lineno})" for item in info.definitions)
        if not info.imports and not info.definitions:
            lines.extend(["", "- No imports or definitions."])
        lines.append("")
    if warnings:
        lines.extend(["## Parse warnings", "", *[f"- `{warning}`" for warning in warnings], ""])
    _write(path, lines)


def write_symbol_index(path: Path, root_name: str, files: dict[str, FileInfo]) -> None:
    definitions, calls = _definitions(files), _calls(files)
    lines = [f"# {root_name} — Python symbol index", "",
             "Definitions and name-based callsites; duplicate symbol names are retained.", "",
             "## Definitions", ""]
    for name, defs in definitions.items():
        for definition in defs:
            lines.append(f"- `{name}` — `{definition.file_rel}:L{definition.lineno}` ({definition.qualname})")
    lines.extend(["", "## Callsites", ""])
    for name in sorted(set(definitions) & set(calls)):
        lines.append(f"### `{name}`")
        lines.extend(f"- `{call.file_rel}:L{call.lineno}` from `{call.caller}`" for call in calls[name])
        lines.append("")
    _write(path, lines)


def _definition_id(definition: DefInfo) -> tuple[str, str]:
    return definition.file_rel, definition.qualname


def _display_definition(definition: DefInfo) -> str:
    return f"{definition.file_rel}:{definition.qualname}"


def _trace_roots(
    definitions: dict[str, list[DefInfo]], calls: dict[str, list[CallSite]]
) -> list[DefInfo]:
    preferred = [definition for name in ENTRYPOINT_NAMES for definition in definitions.get(name, ())]
    if preferred:
        return sorted(preferred)[:20]
    ranked_names = sorted(definitions, key=lambda name: (-len(calls.get(name, ())), name))[:10]
    return [definition for name in ranked_names for definition in definitions[name]][:20]


def write_trace_map(path: Path, root_name: str, files: dict[str, FileInfo], max_depth: int) -> None:
    definitions, calls = _definitions(files), _calls(files)
    edges: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
    by_file_qualname = {
        _definition_id(definition): definition
        for candidates in definitions.values()
        for definition in candidates
    }
    for callee, sites in calls.items():
        # A simple AST name cannot safely distinguish duplicate definitions.
        # Only resolve a target when the repository has exactly one candidate.
        targets = definitions.get(callee, ())
        if len(targets) != 1:
            continue
        target_id = _definition_id(targets[0])
        for site in sites:
            caller_id = (site.file_rel, site.caller)
            if caller_id in by_file_qualname and caller_id != target_id:
                edges[caller_id].add(target_id)

    lines = [f"# {root_name} — approximate Python trace map", "",
             "Name-based static call relationships; this is not a runtime call graph.", ""]
    for root in _trace_roots(definitions, calls):
        root_id = _definition_id(root)
        lines.extend([f"## Trace: `{_display_definition(root)}`", ""])
        queue = deque([(root_id, 0)])
        seen = {root_id}
        while queue:
            caller, depth = queue.popleft()
            for callee in sorted(edges.get(caller, ())):
                caller_label = _display_definition(by_file_qualname[caller])
                callee_label = _display_definition(by_file_qualname[callee])
                lines.append(f"{'  ' * depth}- `{caller_label}` → `{callee_label}`")
                if callee not in seen and depth + 1 < max_depth:
                    seen.add(callee)
                    queue.append((callee, depth + 1))
        if len(seen) == 1:
            lines.append("- No calls to indexed symbols found.")
        lines.append("")
    _write(path, lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=".", help="Folder to scan (default: current directory)")
    parser.add_argument("--output-dir", help="Output folder (default: scan root)")
    parser.add_argument("--prefix", help="Output filename prefix (default: sanitized root folder name)")
    parser.add_argument("--exclude-dir", action="append", default=[], help="Extra directory name to prune")
    parser.add_argument("--max-depth", type=int, default=4, help="Maximum trace depth (default: 4)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).resolve()
    if not root.is_dir():
        print(f"error: root is not a directory: {root}", file=sys.stderr)
        return 2
    if args.max_depth < 1:
        print("error: --max-depth must be at least 1", file=sys.stderr)
        return 2
    output = Path(args.output_dir).resolve() if args.output_dir else root
    prefix = _slug(args.prefix or root.name)
    files, warnings = build_index(root, args.exclude_dir)
    outputs = {
        "code map": output / f"{prefix}_code_map_authoritative.md",
        "trace map": output / f"{prefix}_trace_map.md",
        "symbol index": output / f"{prefix}_symbol_index.md",
    }
    write_code_map(outputs["code map"], root.name, files, warnings)
    write_trace_map(outputs["trace map"], root.name, files, args.max_depth)
    write_symbol_index(outputs["symbol index"], root.name, files)
    for label, path in outputs.items():
        print(f"Wrote {label}: {path}")
    if warnings:
        print(f"Completed with {len(warnings)} parse warning(s).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

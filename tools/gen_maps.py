#!/usr/bin/env python3
"""
gen_maps.py

Generate deterministic navigation artifacts for a Python source tree:
- <project>_code_map.md
- <project>_trace_map.md
- <project>_symbol_index.md

The script is repository and folder-structure agnostic. It can be launched
from any working directory and can scan a local folder, mapped drive,
network share, server path, or CI workspace.

Root resolution priority:
1. --root / positional root argument
2. GEN_MAPS_ROOT environment variable
3. Folder containing this script

Output resolution priority:
1. --output / positional output argument
2. GEN_MAPS_OUTPUT environment variable
3. Scan root

Examples:
    python gen_maps.py
    python gen_maps.py D:\\Projects\\Guardian
    python gen_maps.py D:\\Projects\\Guardian D:\\Reports
    python gen_maps.py --root D:\\Projects\\Guardian --output D:\\Reports
    python gen_maps.py --root "\\\\server\\share\\project"

Design goals:
- Deterministic output: stable ordering and no timestamps
- No third-party dependencies
- No assumptions about repository name or folder layout
- Output files are excluded automatically when output is inside the scan root
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


EXCLUDE_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".git",
    ".github",
    ".idea",
    ".vscode",
    "build",
    "dist",
    "venv",
    ".venv",
    "env",
    ".env",
    "node_modules",
    "legacy",
    "archive",
    "_legacy",
    "_archive",
}

EXCLUDE_FILES = {
    "__init__.py",
}

# Project-specific symbols can remain here. Symbols not found in the scanned
# source are ignored. Generic entry points are used as a fallback.
HIGH_SIGNAL = [
    "run_vop_pipeline",
    "run_vop_pipeline_with_png",
    "run_vop_pipeline_with_csv",
    "run_vop_pipeline_streaming",
    "run_pipeline_from_dynamo_input",
    "process_document_views",
    "process_document_views_streaming",
    "render_model_front_to_back",
    "init_view_raster",
    "_view_signature",
    "resolve_view_bounds",
    "resolve_annotation_only_bounds",
    "rasterize_annotations",
    "collect_view_elements",
    "get_element_silhouette",
]

FALLBACK_ROOTS = (
    "main",
    "run",
    "cli",
    "execute",
    "start",
)


@dataclass(frozen=True)
class DefInfo:
    name: str
    kind: str  # function | async function | class | method | async method
    file_rel: str
    lineno: int


@dataclass(frozen=True)
class ScanError:
    file_rel: str
    error_type: str
    message: str


def safe_project_name(root_dir: Path) -> str:
    """Return a filesystem-safe output prefix derived from the root name."""
    raw = root_dir.name or root_dir.anchor or "python_source"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._-")
    return cleaned or "python_source"


def output_filenames(root_dir: Path) -> Tuple[str, str, str]:
    project = safe_project_name(root_dir)
    return (
        f"{project}_code_map.md",
        f"{project}_trace_map.md",
        f"{project}_symbol_index.md",
    )


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic code, trace, and symbol maps for a Python source tree."
    )
    parser.add_argument(
        "root_positional",
        nargs="?",
        help="Source root to scan. Defaults to GEN_MAPS_ROOT or the script folder.",
    )
    parser.add_argument(
        "output_positional",
        nargs="?",
        help="Output folder. Defaults to GEN_MAPS_OUTPUT or the source root.",
    )
    parser.add_argument(
        "--root",
        dest="root_option",
        help="Source root to scan. Overrides the positional root.",
    )
    parser.add_argument(
        "--output",
        dest="output_option",
        help="Output folder. Overrides the positional output folder.",
    )
    parser.add_argument(
        "--include-init",
        action="store_true",
        help="Include __init__.py files in the scan.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="Maximum depth for the approximate trace map. Default: 4.",
    )
    return parser.parse_args(argv[1:])


def resolve_root(args: argparse.Namespace) -> Path:
    value = (
        args.root_option
        or args.root_positional
        or os.environ.get("GEN_MAPS_ROOT")
    )
    if value:
        return Path(value).expanduser().resolve()
    return Path(__file__).resolve().parent


def resolve_output_dir(args: argparse.Namespace, root_dir: Path) -> Path:
    value = (
        args.output_option
        or args.output_positional
        or os.environ.get("GEN_MAPS_OUTPUT")
    )
    if value:
        return Path(value).expanduser().resolve()
    return root_dir


def is_relative_to(path: Path, parent: Path) -> bool:
    """Compatibility helper for Python versions before Path.is_relative_to."""
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def is_excluded_dir(dirname: str) -> bool:
    return dirname in EXCLUDE_DIRS or dirname.startswith(".")


def iter_py_files(
    root_dir: Path,
    output_dir: Path,
    include_init: bool = False,
) -> List[Path]:
    """Return a stable list of Python files beneath root_dir."""
    files_out: List[Path] = []
    excluded_output_dir: Optional[Path] = None

    # Exclude the output subtree only when it is beneath the scan root and is
    # not the root itself. Generated .md files do not matter, but this avoids
    # scanning unrelated Python tooling placed in a dedicated output folder.
    if output_dir != root_dir and is_relative_to(output_dir, root_dir):
        excluded_output_dir = output_dir

    for current, dirs, files in os.walk(str(root_dir)):
        current_path = Path(current).resolve()

        pruned_dirs: List[str] = []
        for dirname in dirs:
            candidate = (current_path / dirname).resolve()
            if is_excluded_dir(dirname):
                continue
            if excluded_output_dir is not None and (
                candidate == excluded_output_dir
                or is_relative_to(candidate, excluded_output_dir)
            ):
                continue
            pruned_dirs.append(dirname)
        dirs[:] = sorted(pruned_dirs)

        for filename in sorted(files):
            if not filename.lower().endswith(".py"):
                continue
            if not include_init and filename in EXCLUDE_FILES:
                continue
            files_out.append(current_path / filename)

    return sorted(files_out, key=lambda p: relpath_from_root(p, root_dir))


def relpath_from_root(path: Path, root_dir: Path) -> str:
    return path.resolve().relative_to(root_dir.resolve()).as_posix()


def parse_imports(mod: ast.Module) -> List[str]:
    imports: List[str] = []
    for node in mod.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                rendered = alias.name
                if alias.asname:
                    rendered += f" as {alias.asname}"
                imports.append(rendered)
        elif isinstance(node, ast.ImportFrom):
            module_name = node.module or ""
            level = "." * (node.level or 0)
            names = []
            for alias in node.names:
                rendered = alias.name
                if alias.asname:
                    rendered += f" as {alias.asname}"
                names.append(rendered)
            imports.append(f"{level}{module_name}:{','.join(names)}")
    return sorted(imports)


def parse_defs(mod: ast.Module, file_rel: str) -> List[DefInfo]:
    defs: List[DefInfo] = []

    for node in mod.body:
        if isinstance(node, ast.FunctionDef):
            defs.append(DefInfo(node.name, "function", file_rel, node.lineno))
        elif isinstance(node, ast.AsyncFunctionDef):
            defs.append(DefInfo(node.name, "async function", file_rel, node.lineno))
        elif isinstance(node, ast.ClassDef):
            defs.append(DefInfo(node.name, "class", file_rel, node.lineno))
            for sub in node.body:
                if isinstance(sub, ast.FunctionDef):
                    defs.append(
                        DefInfo(f"{node.name}.{sub.name}", "method", file_rel, sub.lineno)
                    )
                elif isinstance(sub, ast.AsyncFunctionDef):
                    defs.append(
                        DefInfo(
                            f"{node.name}.{sub.name}",
                            "async method",
                            file_rel,
                            sub.lineno,
                        )
                    )

    return sorted(defs, key=lambda d: (d.file_rel, d.lineno, d.kind, d.name))


def call_name(expr: ast.AST) -> Optional[str]:
    """Return the terminal name of a callable expression."""
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return None


def parse_callsites(
    mod: ast.Module,
    file_rel: str,
) -> Dict[str, List[Tuple[str, int]]]:
    """Return callee name -> [(relative file, line number), ...]."""
    out: Dict[str, List[Tuple[str, int]]] = {}
    for node in ast.walk(mod):
        if not isinstance(node, ast.Call):
            continue
        callee = call_name(node.func)
        if not callee:
            continue
        out.setdefault(callee, []).append((file_rel, getattr(node, "lineno", 0) or 0))

    for callee in out:
        out[callee] = sorted(set(out[callee]), key=lambda item: (item[0], item[1]))
    return out


def called_names_in_node(node: ast.AST) -> Set[str]:
    names: Set[str] = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            name = call_name(sub.func)
            if name:
                names.add(name)
    return names


def parse_definition_calls(mod: ast.Module) -> Dict[str, Set[str]]:
    """Return a best-effort mapping of definition name -> names it calls."""
    out: Dict[str, Set[str]] = {}
    for node in mod.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out[node.name] = called_names_in_node(node)
        elif isinstance(node, ast.ClassDef):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    out[f"{node.name}.{sub.name}"] = called_names_in_node(sub)
                    # Also expose the simple method name for name-based tracing.
                    out.setdefault(sub.name, set()).update(called_names_in_node(sub))
    return out


def load_ast(path: Path) -> ast.Module:
    with path.open("r", encoding="utf-8-sig") as stream:
        text = stream.read()
    return ast.parse(text, filename=str(path))


def build_index(
    root_dir: Path,
    output_dir: Path,
    include_init: bool = False,
) -> Tuple[
    Dict[str, List[str]],
    Dict[str, List[DefInfo]],
    Dict[str, List[DefInfo]],
    Dict[str, Set[str]],
    Dict[str, List[Tuple[str, int]]],
    Dict[str, Set[str]],
    List[ScanError],
]:
    file_imports: Dict[str, List[str]] = {}
    file_defs: Dict[str, List[DefInfo]] = {}
    name_to_defs: Dict[str, List[DefInfo]] = {}
    callee_to_files: Dict[str, Set[str]] = {}
    callee_to_calls: Dict[str, List[Tuple[str, int]]] = {}
    definition_calls: Dict[str, Set[str]] = {}
    errors: List[ScanError] = []

    for path in iter_py_files(root_dir, output_dir, include_init=include_init):
        file_rel = relpath_from_root(path, root_dir)
        try:
            mod = load_ast(path)
        except (OSError, UnicodeError, SyntaxError) as exc:
            errors.append(
                ScanError(file_rel, type(exc).__name__, str(exc).replace("\n", " "))
            )
            continue

        imports = parse_imports(mod)
        defs = parse_defs(mod, file_rel)
        callsites = parse_callsites(mod, file_rel)
        local_definition_calls = parse_definition_calls(mod)

        file_imports[file_rel] = imports
        file_defs[file_rel] = defs

        for info in defs:
            # Index both qualified method names and simple method names.
            name_to_defs.setdefault(info.name, []).append(info)
            if "." in info.name:
                name_to_defs.setdefault(info.name.rsplit(".", 1)[1], []).append(info)

        for callee, locations in callsites.items():
            callee_to_files.setdefault(callee, set()).add(file_rel)
            callee_to_calls.setdefault(callee, []).extend(locations)

        for definition, callees in local_definition_calls.items():
            definition_calls.setdefault(definition, set()).update(callees)

    for name in name_to_defs:
        name_to_defs[name] = sorted(
            set(name_to_defs[name]),
            key=lambda d: (d.file_rel, d.lineno, d.kind, d.name),
        )
    for callee in callee_to_calls:
        callee_to_calls[callee] = sorted(
            set(callee_to_calls[callee]), key=lambda item: (item[0], item[1])
        )

    return (
        file_imports,
        file_defs,
        name_to_defs,
        callee_to_files,
        callee_to_calls,
        definition_calls,
        sorted(errors, key=lambda e: e.file_rel),
    )


def write_lines(path: Path, lines: Iterable[str]) -> None:
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8", newline="\n")


def write_code_map(
    root_dir: Path,
    output_path: Path,
    file_imports: Dict[str, List[str]],
    file_defs: Dict[str, List[DefInfo]],
    errors: List[ScanError],
) -> None:
    lines: List[str] = [
        f"# {safe_project_name(root_dir)} code map",
        "",
        "_Generated deterministically from the current Python source snapshot._",
        "",
        f"- Source root: `{root_dir}`",
        f"- Parsed Python files: {len(file_imports)}",
        f"- Files skipped due to parse/read errors: {len(errors)}",
        "",
    ]

    for file_rel in sorted(file_imports):
        lines.extend([f"## `{file_rel}`", ""])
        imports = file_imports[file_rel]
        defs = file_defs.get(file_rel, [])

        lines.append("### Imports")
        if imports:
            lines.extend(f"- `{item}`" for item in imports)
        else:
            lines.append("- None")
        lines.append("")

        lines.append("### Definitions")
        if defs:
            for info in defs:
                lines.append(f"- `{info.name}` | {info.kind} | line {info.lineno}")
        else:
            lines.append("- None")
        lines.append("")

    if errors:
        lines.extend(["# Scan errors", ""])
        for error in errors:
            lines.append(
                f"- `{error.file_rel}` | {error.error_type} | {error.message}"
            )
        lines.append("")

    write_lines(output_path, lines)


def ordered_symbol_names(name_to_defs: Dict[str, List[DefInfo]]) -> List[str]:
    all_names = sorted(name_to_defs)
    high = [name for name in HIGH_SIGNAL if name in name_to_defs]
    high_set = set(high)
    return high + [name for name in all_names if name not in high_set]


def write_symbol_index(
    root_dir: Path,
    output_path: Path,
    name_to_defs: Dict[str, List[DefInfo]],
    callee_to_files: Dict[str, Set[str]],
    callee_to_calls: Dict[str, List[Tuple[str, int]]],
) -> None:
    lines: List[str] = [
        f"# {safe_project_name(root_dir)} symbol index",
        "",
        "_Definitions and name-based callsites generated from the current Python source snapshot._",
        "",
    ]

    for name in ordered_symbol_names(name_to_defs):
        lines.extend([f"## `{name}`", ""])
        lines.append("Definitions:")
        for info in name_to_defs[name]:
            lines.append(
                f"- `{info.file_rel}:{info.lineno}` | {info.kind} | `{info.name}`"
            )
        lines.append("")

        callsites = callee_to_calls.get(name, [])
        lines.append(f"Callsites: {len(callsites)}")
        if callsites:
            for file_rel, lineno in callsites:
                lines.append(f"- `{file_rel}:{lineno}`")
        else:
            lines.append("- None found by terminal-name AST matching")
        lines.append("")

    write_lines(output_path, lines)


def select_trace_roots(name_to_defs: Dict[str, List[DefInfo]]) -> List[str]:
    roots = [name for name in HIGH_SIGNAL if name in name_to_defs]
    if roots:
        return roots
    return [name for name in FALLBACK_ROOTS if name in name_to_defs]


def trace_symbol(
    symbol: str,
    name_to_defs: Dict[str, List[DefInfo]],
    definition_calls: Dict[str, Set[str]],
    max_depth: int,
    depth: int = 0,
    ancestry: Optional[Set[str]] = None,
) -> List[str]:
    ancestry = set() if ancestry is None else set(ancestry)
    indent = "  " * depth

    definitions = name_to_defs.get(symbol, [])
    locations = ", ".join(
        f"{info.file_rel}:{info.lineno}" for info in definitions
    ) or "definition not indexed"
    lines = [f"{indent}- `{symbol}` | {locations}"]

    if depth >= max_depth:
        return lines
    if symbol in ancestry:
        lines[-1] += " | cycle"
        return lines

    ancestry.add(symbol)
    callees = set(definition_calls.get(symbol, set()))

    # A method may be indexed under Class.method while its calls are also
    # recorded under the simple method name.
    for info in definitions:
        callees.update(definition_calls.get(info.name, set()))
        if "." in info.name:
            callees.update(definition_calls.get(info.name.rsplit(".", 1)[1], set()))

    internal_callees = sorted(name for name in callees if name in name_to_defs)
    for callee in internal_callees:
        lines.extend(
            trace_symbol(
                callee,
                name_to_defs,
                definition_calls,
                max_depth,
                depth + 1,
                ancestry,
            )
        )
    return lines


def write_trace_map(
    root_dir: Path,
    output_path: Path,
    name_to_defs: Dict[str, List[DefInfo]],
    definition_calls: Dict[str, Set[str]],
    max_depth: int,
) -> None:
    roots = select_trace_roots(name_to_defs)
    lines: List[str] = [
        f"# {safe_project_name(root_dir)} trace map",
        "",
        "_Approximate definition-level call tree generated from AST call names._",
        "",
        "Notes:",
        "- This is a static, name-based approximation, not a runtime call graph.",
        "- Attribute calls are matched by terminal name.",
        "- Dynamic dispatch, aliases, decorators, and runtime imports may not resolve precisely.",
        f"- Maximum expansion depth: {max_depth}",
        "",
    ]

    if not roots:
        lines.extend(
            [
                "No configured high-signal or generic entry-point symbols were found.",
                "",
                "Add project entry points to `HIGH_SIGNAL` if a trace root is required.",
                "",
            ]
        )
    else:
        for root in roots:
            lines.extend([f"## `{root}`", ""])
            lines.extend(trace_symbol(root, name_to_defs, definition_calls, max_depth))
            lines.append("")

    write_lines(output_path, lines)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    root_dir = resolve_root(args)
    output_dir = resolve_output_dir(args, root_dir)

    if not root_dir.exists():
        print(f"ERROR: source root does not exist: {root_dir}", file=sys.stderr)
        return 2
    if not root_dir.is_dir():
        print(f"ERROR: source root is not a directory: {root_dir}", file=sys.stderr)
        return 2
    if args.max_depth < 0:
        print("ERROR: --max-depth must be zero or greater", file=sys.stderr)
        return 2

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"ERROR: cannot create output directory {output_dir}: {exc}", file=sys.stderr)
        return 2

    code_name, trace_name, symbol_name = output_filenames(root_dir)
    code_path = output_dir / code_name
    trace_path = output_dir / trace_name
    symbol_path = output_dir / symbol_name

    print(f"Source root: {root_dir}")
    print(f"Output directory: {output_dir}")

    (
        file_imports,
        file_defs,
        name_to_defs,
        callee_to_files,
        callee_to_calls,
        definition_calls,
        errors,
    ) = build_index(root_dir, output_dir, include_init=args.include_init)

    try:
        write_code_map(
            root_dir,
            code_path,
            file_imports,
            file_defs,
            errors,
        )
        write_symbol_index(
            root_dir,
            symbol_path,
            name_to_defs,
            callee_to_files,
            callee_to_calls,
        )
        write_trace_map(
            root_dir,
            trace_path,
            name_to_defs,
            definition_calls,
            args.max_depth,
        )
    except OSError as exc:
        print(f"ERROR: failed to write output: {exc}", file=sys.stderr)
        return 3

    print(f"Parsed Python files: {len(file_imports)}")
    print(f"Skipped files: {len(errors)}")
    print(f"Wrote: {code_path}")
    print(f"Wrote: {trace_path}")
    print(f"Wrote: {symbol_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

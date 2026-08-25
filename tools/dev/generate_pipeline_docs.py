#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generic Markdown generator for "--- trace ---" docstring annotations.

Scans one or more .py files for functions (top-level or nested, at any depth) whose
docstring contains a "--- trace ---" marker, parses the reads/calls/thresholds/returns
fields that follow it, and emits one Markdown section per function with a link back to its
source line.

This script is deliberately generic: it contains no knowledge of any particular pipeline
stage, domain, or module. It only understands the trace-block convention itself:

    --- trace ---
    reads: <prose>
    calls: <prose>
    thresholds: <prose>
    returns: <prose>

Two optional extensions to the convention are also recognized, each as its own
label-prefixed section using the same rules as the four required fields above:
  - A `note:`/`notes:` field for free-form caveats (e.g. something that breaks the
    "mechanically extractable" assumption for a naive parser).
  - A `stub:` or `archived path:` field marking a function as a genuine no-op or as
    otherwise mischaracterized by its location; rendered as a warning badge.

To point this generator at a new file or stage, pass different file paths on the command
line -- nothing else about the script needs to change.

Usage:
    python tools/dev/generate_pipeline_docs.py FILE.py [FILE2.py ...] [--out OUT.md]
        [--title TITLE] [--prepend-file PREAMBLE.md]
"""
from __future__ import annotations

import argparse
import ast
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

TRACE_MARKER_RE = re.compile(r"^-{2,}\s*trace\s*-{2,}$", re.IGNORECASE)
FIELD_LABEL_RE = re.compile(
    r"^(reads|calls|thresholds|returns|notes?|stub|archived\s+path)\s*:\s*(.*)$",
    re.IGNORECASE,
)
_FIELD_CANONICAL = {
    "reads": "reads",
    "calls": "calls",
    "thresholds": "thresholds",
    "returns": "returns",
    "note": "notes",
    "notes": "notes",
    "stub": "stub",
    "archived path": "archived_path",
}
FIELD_ORDER = ["reads", "calls", "thresholds", "returns", "notes", "stub", "archived_path"]
FIELD_TITLES = {
    "reads": "Reads",
    "calls": "Calls",
    "thresholds": "Thresholds",
    "returns": "Returns",
    "notes": "Notes",
    "stub": "STUB",
    "archived_path": "ARCHIVED PATH",
}


def parse_trace_block(docstring: str) -> Optional[Dict[str, str]]:
    """Extract the reads/calls/thresholds/returns(/notes/stub/archived_path) fields from a
    cleaned function docstring, if it contains a "--- trace ---" marker line.

    --- trace ---
    reads: `docstring` -- a single already-dedented function docstring (from
        ast.get_docstring(node, clean=True)), passed in by iter_traced_functions().
    calls: none (re, str methods only).
    thresholds: FIELD_LABEL_RE / TRACE_MARKER_RE (module-level compiled regexes) define
        what counts as a field-start line versus a continuation line.
    returns: dict[str, str] mapping canonical field name -> joined prose, or None if no
        trace marker is present; consumed by iter_traced_functions() to build each
        function's record.
    """
    lines = docstring.splitlines()
    marker_idx = None
    for i, line in enumerate(lines):
        if TRACE_MARKER_RE.match(line.strip()):
            marker_idx = i
            break
    if marker_idx is None:
        return None

    fields: Dict[str, List[str]] = {}
    current: Optional[str] = None
    for raw_line in lines[marker_idx + 1:]:
        stripped = raw_line.strip()
        if not stripped:
            continue
        m = FIELD_LABEL_RE.match(stripped)
        if m:
            label = _FIELD_CANONICAL[re.sub(r"\s+", " ", m.group(1).lower())]
            current = label
            fields.setdefault(current, [])
            rest = m.group(2).strip()
            if rest:
                fields[current].append(rest)
        elif current is not None:
            fields[current].append(stripped)
        # else: stray text before any recognized field label -- ignored.

    return {k: " ".join(v).strip() for k, v in fields.items() if " ".join(v).strip()}


def render_signature(node: ast.AST) -> str:
    """Render a function's parameter list and return annotation as source text.

    --- trace ---
    reads: `node` -- an ast.FunctionDef/AsyncFunctionDef, passed in by
        iter_traced_functions().
    calls: ast.unparse() (stdlib, Python 3.9+).
    thresholds: none.
    returns: str like "(a: int, b: str = 'x') -> bool"; consumed by
        iter_traced_functions() to build each function's displayed signature.
    """
    args_src = ast.unparse(node.args)
    ret_src = f" -> {ast.unparse(node.returns)}" if node.returns is not None else ""
    return f"({args_src}){ret_src}"


def iter_traced_functions(source_path: Path) -> List[Dict[str, Any]]:
    """Parse one .py file and return a record for every function (any nesting depth)
    whose docstring carries a "--- trace ---" block.

    --- trace ---
    reads: `source_path` -- a .py file path, from main()'s CLI file-path arguments; the
        file's own source text (read here).
    calls: ast.parse(); parse_trace_block(); render_signature(); a local recursive
        AST-walking helper (_walk) that tracks a dotted qualified-name path so nested
        helpers are reported as `outer.inner`, not just `inner`.
    thresholds: none.
    returns: list[dict] with keys name, qualname, signature, lineno, fields; consumed by
        render_markdown(). Functions without a "--- trace ---" marker are silently
        omitted -- this generator documents only what has been explicitly annotated.
    """
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))

    records: List[Dict[str, Any]] = []

    def _walk(node: ast.AST, scope_path: List[str]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qualname_parts = scope_path + [child.name]
                docstring = ast.get_docstring(child, clean=True) or ""
                fields = parse_trace_block(docstring)
                if fields:
                    records.append({
                        "name": child.name,
                        "qualname": ".".join(qualname_parts),
                        "signature": render_signature(child),
                        "lineno": child.lineno,
                        "fields": fields,
                    })
                _walk(child, qualname_parts)
            elif isinstance(child, ast.ClassDef):
                _walk(child, scope_path + [child.name])
            else:
                _walk(child, scope_path)

    _walk(tree, [])
    records.sort(key=lambda r: r["lineno"])
    return records


def _relative_link(source_path: Path, out_dir: Path) -> str:
    try:
        rel = Path(os.path.relpath(source_path.resolve(), out_dir.resolve()))
    except ValueError:
        rel = source_path.resolve()
    return rel.as_posix()


def render_markdown(files_records: List[Dict[str, Any]], out_dir: Path) -> str:
    """Render the full function-reference Markdown body (no title/preamble) for every
    scanned file's traced functions.

    --- trace ---
    reads: `files_records` -- list of {path, records} produced by main() from
        iter_traced_functions() per input file; `out_dir` -- directory the output Markdown
        will live in, used to compute relative file:line links.
    calls: _relative_link().
    thresholds: none.
    returns: Markdown string; consumed by main(), which writes it to --out (or stdout).
    """
    lines: List[str] = []
    for entry in files_records:
        path: Path = entry["path"]
        records: List[Dict[str, Any]] = entry["records"]
        rel = _relative_link(path, out_dir)
        lines.append(f"## `{rel}`")
        lines.append("")
        if not records:
            lines.append("_No `--- trace ---` blocks found in this file._")
            lines.append("")
            continue
        for rec in records:
            link = f"{rel}#L{rec['lineno']}"
            lines.append(f"### `{rec['qualname']}{rec['signature']}` — [{path.name}:L{rec['lineno']}]({link})")
            lines.append("")
            fields = rec["fields"]
            if "stub" in fields:
                lines.append(f"> ⚠️ **STUB** — {fields['stub']}")
                lines.append("")
            if "archived_path" in fields:
                lines.append(f"> ⚠️ **ARCHIVED PATH** — {fields['archived_path']}")
                lines.append("")
            for key in ["reads", "calls", "thresholds", "returns", "notes"]:
                if key in fields:
                    lines.append(f"- **{FIELD_TITLES[key]}:** {fields[key]}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    """CLI entry point: scan the given files, render the Markdown reference, optionally
    prepend a caller-supplied preamble, and write it to --out or stdout.

    --- trace ---
    reads: CLI positional `files` (one or more .py paths); --out (output .md path,
        optional -- stdout if omitted); --title (optional H1 heading); --prepend-file
        (optional path to arbitrary preamble Markdown, copied verbatim before the
        generated sections).
    calls: iter_traced_functions() (once per input file); render_markdown().
    thresholds: none.
    returns: writes to --out if given, else prints to stdout; prints a per-file function
        count summary to stderr-equivalent (stdout, prefixed) for operator visibility.
        Returns None.
    """
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", help="One or more .py files to scan for --- trace --- blocks.")
    ap.add_argument("--out", default=None, help="Output Markdown file path (default: print to stdout).")
    ap.add_argument("--title", default=None, help="Optional H1 title for the generated document.")
    ap.add_argument("--prepend-file", default=None, help="Optional Markdown file whose content is copied verbatim before the generated sections.")
    args = ap.parse_args()

    out_path = Path(args.out) if args.out else None
    out_dir = out_path.parent if out_path else Path.cwd()
    out_dir.mkdir(parents=True, exist_ok=True) if out_path else None

    files_records = []
    for f in args.files:
        p = Path(f)
        records = iter_traced_functions(p)
        files_records.append({"path": p, "records": records})
        print(f"[generate_pipeline_docs] {p}: {len(records)} traced function(s)")

    body_parts: List[str] = []
    if args.title:
        body_parts.append(f"# {args.title}\n")
    if args.prepend_file:
        body_parts.append(Path(args.prepend_file).read_text(encoding="utf-8").rstrip() + "\n")
    body_parts.append(render_markdown(files_records, out_dir))
    full_text = "\n".join(body_parts)

    if out_path:
        out_path.write_text(full_text, encoding="utf-8")
        print(f"[generate_pipeline_docs] wrote -> {out_path}")
    else:
        print(full_text)


if __name__ == "__main__":
    main()

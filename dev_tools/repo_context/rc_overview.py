"""repository_overview.md and README.md generation."""
from __future__ import annotations

from collections import Counter, defaultdict

from rc_common import TOOL_VERSION, atomic_write_text

LARGE_MODULE_LINE_THRESHOLD = 800
LARGE_MODULE_COMPLEXITY_THRESHOLD = 15
TOP_N = 15


def _top_dir(rel_path: str) -> str:
    parts = rel_path.split("/")
    return parts[0] if len(parts) > 1 else "(repository root)"


def generate_overview_md(result, root_name: str, scan_time_iso: str) -> str:
    included = [f for f in result.files if f.included]
    excluded = [f for f in result.files if not f.included]

    by_ext = Counter(f.extension or "(none)" for f in included)
    by_category = Counter(f.category for f in included)

    largest_text = sorted(
        (f for f in included if not f.is_binary), key=lambda f: f.size_bytes, reverse=True
    )[:TOP_N]
    largest_py = sorted(
        (f for f in included if f.extension == ".py" and f.line_count),
        key=lambda f: f.line_count, reverse=True,
    )[:TOP_N]
    chunked_files = sorted((f for f in included if f.chunked), key=lambda f: f.relative_path)

    dirs_by_category = defaultdict(set)
    for f in included:
        dirs_by_category[f.category].add(_top_dir(f.relative_path))

    complexity_by_file = defaultdict(int)
    for s in result.symbols:
        if s.symbol_type != "module":
            complexity_by_file[s.relative_path] = max(complexity_by_file[s.relative_path], s.complexity_approx)
    large_modules = sorted(
        (
            f for f in included
            if f.extension == ".py" and (
                (f.line_count or 0) > LARGE_MODULE_LINE_THRESHOLD
                or complexity_by_file.get(f.relative_path, 0) > LARGE_MODULE_COMPLEXITY_THRESHOLD
            )
        ),
        key=lambda f: f.relative_path,
    )

    lines = []
    lines.append(f"# Repository Overview: {root_name}\n")
    lines.append(f"- Scan time (UTC): {scan_time_iso}")
    lines.append(f"- Generator: repo_context.py v{TOOL_VERSION}")
    lines.append(f"- Total files considered: {len(result.files)}")
    lines.append(f"- Included: {len(included)}")
    lines.append(f"- Excluded: {len(excluded)}")
    lines.append("")

    lines.append("## Files by extension\n")
    for ext, n in sorted(by_ext.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"- `{ext}`: {n}")
    lines.append("")

    lines.append("## Files by classification\n")
    for cat, n in sorted(by_category.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"- {cat}: {n}")
    lines.append("")

    lines.append("## Largest text files\n")
    for f in largest_text:
        lines.append(f"- `{f.relative_path}` — {f.size_bytes} bytes, {f.line_count or 0} lines")
    if not largest_text:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Largest Python files by line count\n")
    for f in largest_py:
        lines.append(f"- `{f.relative_path}` — {f.line_count} lines")
    if not largest_py:
        lines.append("- (none)")
    lines.append("")

    lines.append(f"## Files that required chunking ({len(chunked_files)})\n")
    for f in chunked_files[:TOP_N]:
        lines.append(f"- `{f.relative_path}`")
    if len(chunked_files) > TOP_N:
        lines.append(f"- ... and {len(chunked_files) - TOP_N} more (see chunk_manifest.csv)")
    if not chunked_files:
        lines.append("- (none)")
    lines.append("")

    lines.append(f"## Python parse failures ({len(result.parse_warnings)})\n")
    for rel, ln, col, msg in sorted(result.parse_warnings, key=lambda t: t[0]):
        lines.append(f"- `{rel}` line {ln}, col {col}: {msg}")
    if not result.parse_warnings:
        lines.append("- (none)")
    lines.append("")

    lines.append(f"## Likely entry points ({len(result.entrypoints)})\n")
    for rel, reason in sorted(result.entrypoints, key=lambda t: t[0]):
        lines.append(f"- `{rel}` — {reason}")
    if not result.entrypoints:
        lines.append("- (none detected)")
    lines.append("")

    lines.append("## Directories by role (heuristic, top-level only)\n")
    lines.append("_Derived only from file classification counts in each top-level directory; not a claim about architecture._\n")
    for cat in ("test", "documentation", "configuration", "json_schema", "script_shell",
                "script_powershell", "script_batch", "python_source"):
        dirs = sorted(dirs_by_category.get(cat, []))
        if dirs:
            lines.append(f"- {cat}: {', '.join(dirs)}")
    lines.append("")

    lines.append(f"## Unusually large or structurally complex Python modules ({len(large_modules)})\n")
    lines.append(
        f"_Heuristic only: line count > {LARGE_MODULE_LINE_THRESHOLD} or any symbol with "
        f"approximate cyclomatic complexity > {LARGE_MODULE_COMPLEXITY_THRESHOLD}._\n"
    )
    for f in large_modules[:TOP_N]:
        lines.append(
            f"- `{f.relative_path}` — {f.line_count} lines, "
            f"max symbol complexity ~{complexity_by_file.get(f.relative_path, 0)}"
        )
    if len(large_modules) > TOP_N:
        lines.append(f"- ... and {len(large_modules) - TOP_N} more")
    if not large_modules:
        lines.append("- (none)")
    lines.append("")

    return "\n".join(lines) + "\n"


def generate_readme_md() -> str:
    return f"""# repo_context output

Generated by `repo_context.py` (v{TOOL_VERSION}), a local, read-only tool that
scans a repository or folder and prepares it for a document-grounded LLM
(such as Microsoft 365 Copilot) without uploading, transmitting, executing,
importing, or modifying any source code.

## What each file contains

- `repository_overview.md` — concise statistics and navigation info (file
  counts, largest files, likely entry points, parse failures, etc.).
- `repository_tree.txt` — a deterministic directory tree of included files,
  annotated with size and line count.
- `file_inventory.csv` / `file_inventory.jsonl` — one row per considered
  file: path, extension, category, size, text/binary, line count, SHA-256,
  included/excluded (+ reason), chunked, parse status, generated/vendor
  flag.
- `python_symbols.csv` / `python_symbols.jsonl` — one row per Python module,
  class, function, async function, method, or nested function: qualified
  name, line range, parent, decorators, parameters, docstring presence and
  first line (not the full docstring), an **approximate** cyclomatic
  complexity, and nested symbol names.
- `python_imports.csv` — every `import` / `from ... import ...` statement,
  with a best-effort, conservative resolution to a repository file
  (`resolution_status` is `resolved` only when exactly one candidate file
  matches; otherwise `ambiguous` or `unresolved_external_or_missing`).
- `python_calls.csv` — syntactic call sites found via AST analysis, with a
  best-effort candidate target and a `confidence` of `high`, `medium`,
  `low`, or `unresolved`, plus a plain-language `explanation`.
  **These are static call-expression candidates, not proof of runtime
  dispatch** — dynamic dispatch, monkey-patching, reflection, dependency
  injection, and polymorphism are never guessed at.
- `entrypoint_candidates.csv` — Python files with a
  `if __name__ == "__main__":` guard or a conventional entrypoint filename.
- `parse_warnings.csv` — Python files that failed to parse (e.g. syntax
  errors), with location and message; the rest of the scan still completes.
- `chunk_manifest.csv` — one row per chunk in `chunks/`: source file, chunk
  file, original line range, overlap, symbols present, source/chunk
  SHA-256, character count, and a rough estimated token count.
- `chunks/` — line-numbered Markdown chunks of files too large to hand to
  an LLM directly. Python files are split on class/function boundaries
  where possible; oversized single symbols are split by line range and
  labeled. Other text files split by line range with a small overlap.
  Lines that look like secrets are redacted by default.
- `packets/` — targeted context packets produced by the `packet` command
  (see below).
- `generation_manifest.json` — exactly what was scanned, excluded,
  generated, or could not be parsed, plus the tool version and options
  used for this run.

## How the data was produced

Everything is derived from a single read-only filesystem walk plus Python
`ast`-based static analysis. No scanned file is ever imported or executed.
Hashing is streamed (SHA-256). Import and call resolution is deliberately
conservative: a target is only marked `resolved` when exactly one
repository file matches; ambiguous or external targets are reported as
such rather than guessed.

## Important limitations

- Call-graph and import resolution are **best-effort static analysis**,
  not a substitute for actually running the code. Dynamic imports, runtime
  `getattr`/reflection dispatch, monkey-patching, and polymorphic method
  calls are reported as `unresolved` rather than guessed.
  cyclomatic-complexity numbers are **approximations** (they don't model
  `match` statement exhaustiveness, decorators that alter control flow,
  etc.).
- Only Python gets symbol/import/call analysis. Other languages are still
  inventoried, classified, and chunked, but not parsed for symbols.
- Directory-role labels in `repository_overview.md` are purely a
  by-file-classification-count heuristic, not an architectural claim.

## How to regenerate

```
python repo_context.py scan <ROOT> --output <OUTPUT_DIR>
```

Add `--force` to force a full rebuild (bypassing chunk-output reuse from a
previous run). Add `--verbose` for progress output.

## How to create a targeted packet

```
python repo_context.py packet <ROOT> --output <OUTPUT_DIR> --file path/to/file.py
python repo_context.py packet <ROOT> --output <OUTPUT_DIR> --symbol some_function
python repo_context.py packet <ROOT> --output <OUTPUT_DIR> --search some_text
python repo_context.py packet <ROOT> --output <OUTPUT_DIR> --line path/to/file.py:123
```

A packet requires a prior `scan` of the same output directory — it reads
the generated indexes rather than rescanning. Packets are written to
`packets/` as compact Markdown files with exact source excerpts and
original line numbers, plus explicit notes on anything omitted or left
unresolved.

## Giving this to Microsoft 365 Copilot

Attach `repository_overview.md` and `repository_tree.txt` for orientation
questions. Attach a specific chunk from `chunks/` (found via
`chunk_manifest.csv`) or a generated packet from `packets/` when asking
about a specific file, function, or bug. Avoid attaching the entire
`chunks/` directory at once — packets are the compact, targeted option.

**Generated context may contain proprietary source code and must be
handled under the same security rules as the repository it came from.**
"""

"""Targeted context packet generation.

Reads the indexes already produced by a prior `scan` (never re-scans), and
reads exact source excerpts directly from ROOT (read-only). Never dumps an
entire repository into a packet; every omission is stated explicitly.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from rc_common import atomic_write_text, sanitize_stem, redact_secrets


def _load_csv(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _norm_rel(path_str: str) -> str:
    return path_str.replace("\\", "/").lstrip("/")


def _safe_excerpt(root: Path, rel_path: str, start: int, end: int) -> Optional[list]:
    abs_path = (root / rel_path)
    try:
        resolved = abs_path.resolve()
        resolved.relative_to(root.resolve())
    except (OSError, ValueError):
        return None
    if not resolved.exists():
        return None
    try:
        with open(resolved, "r", encoding="utf-8", errors="replace") as fh:
            out = []
            for i, line in enumerate(fh, start=1):
                if i > end:
                    break
                if i >= start:
                    out.append((i, line.rstrip("\n")))
            return out
    except OSError:
        return None


def _symbol_matches(name: str, row: dict) -> bool:
    qn = row["qualified_name"]
    return qn == name or qn.split(".")[-1] == name or qn.endswith("." + name)


def _find_symbol_candidates(name: str, symbols: list, file_constraint: Optional[str]) -> list:
    out = [r for r in symbols if _symbol_matches(name, r)]
    if file_constraint:
        out = [r for r in out if r["relative_path"] == file_constraint]
    return out


@dataclass
class PacketOptions:
    root: Path
    output_dir: Path
    file: Optional[str] = None
    symbol: Optional[str] = None
    search: Optional[str] = None
    line: Optional[str] = None
    changed: list = field(default_factory=list)
    caller_depth: int = 1
    callee_depth: int = 1
    max_files: int = 8
    max_lines: int = 1200
    max_characters: int = 60_000
    all_matches: bool = False
    name: Optional[str] = None  # override output filename stem


class Budget:
    def __init__(self, max_lines: int, max_characters: int):
        self.max_lines = max_lines
        self.max_characters = max_characters
        self.lines_used = 0
        self.chars_used = 0
        self.omissions: list = []

    def allow(self, text: str, n_lines: int) -> bool:
        return (self.lines_used + n_lines <= self.max_lines) and (self.chars_used + len(text) <= self.max_characters)

    def spend(self, text: str, n_lines: int) -> None:
        self.lines_used += n_lines
        self.chars_used += len(text)


def _callers_of(symbol_qn: str, symbol_file: str, calls: list) -> list:
    return [c for c in calls if c["candidate_symbol"] == symbol_qn and c["candidate_file"] == symbol_file]


def _callees_of(symbol_qn: str, symbol_file: str, calls: list) -> list:
    return [c for c in calls if c["caller_symbol"] == symbol_qn and c["caller_file"] == symbol_file]


def _bfs_callers(start_file: str, start_qn: str, calls: list, depth: int) -> list:
    frontier = [(start_file, start_qn)]
    seen = {(start_file, start_qn)}
    edges = []
    for _ in range(max(0, depth)):
        next_frontier = []
        for f, qn in frontier:
            for c in _callers_of(qn, f, calls):
                edges.append(c)
                key = (c["caller_file"], c["caller_symbol"])
                if key not in seen:
                    seen.add(key)
                    next_frontier.append(key)
        frontier = next_frontier
        if not frontier:
            break
    return edges


def _bfs_callees(start_file: str, start_qn: str, calls: list, depth: int) -> list:
    frontier = [(start_file, start_qn)]
    seen = {(start_file, start_qn)}
    edges = []
    for _ in range(max(0, depth)):
        next_frontier = []
        for f, qn in frontier:
            for c in _callees_of(qn, f, calls):
                edges.append(c)
                if c["candidate_file"] and c["candidate_symbol"]:
                    key = (c["candidate_file"], c["candidate_symbol"])
                    if key not in seen:
                        seen.add(key)
                        next_frontier.append(key)
        frontier = next_frontier
        if not frontier:
            break
    return edges


def _enclosing_class_or_func(row: dict, symbols_by_file: dict) -> Optional[dict]:
    parent = row.get("parent_symbol")
    if not parent or parent == "<module>":
        return None
    for r in symbols_by_file.get(row["relative_path"], []):
        if r["qualified_name"] == parent:
            return r
    return None


def _candidate_tests_for_file(target_file: str, imports: list, calls: list, files_by_path: dict) -> list:
    out = set()
    for imp in imports:
        if imp["resolved_file"] == target_file:
            src = imp["source_file"]
            if files_by_path.get(src, {}).get("category") == "test":
                out.add(src)
    for c in calls:
        if c["candidate_file"] == target_file:
            src = c["caller_file"]
            if files_by_path.get(src, {}).get("category") == "test":
                out.add(src)
    return sorted(out)


def _render_symbol_block(root: Path, row: dict, budget: Budget, out: list, note_prefix: str = "") -> None:
    start, end = int(row["start_line"]), int(row["end_line"])
    excerpt = _safe_excerpt(root, row["relative_path"], start, end)
    header = f"\n### {note_prefix}`{row['qualified_name']}` ({row['symbol_type']}) — `{row['relative_path']}:{start}-{end}`\n"
    out.append(header)
    if excerpt is None:
        out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
        return
    body_lines = [f"{ln:>6}| {text}" for ln, text in excerpt]
    body = "\n".join(body_lines)
    if not budget.allow(body, len(body_lines)):
        budget.omissions.append(
            f"Excerpt for `{row['qualified_name']}` in `{row['relative_path']}` omitted (packet size limit reached); "
            f"see chunk_manifest.csv / python_symbols.csv for `{row['relative_path']}`."
        )
        return
    body = redact_secrets(body)
    out.append("```\n" + body + "\n```\n")
    budget.spend(body, len(body_lines))


def generate_packet(opts: PacketOptions) -> Path:
    files_rows = _load_csv(opts.output_dir / "file_inventory.csv")
    symbols_rows = _load_csv(opts.output_dir / "python_symbols.csv")
    imports_rows = _load_csv(opts.output_dir / "python_imports.csv")
    calls_rows = _load_csv(opts.output_dir / "python_calls.csv")

    files_by_path = {r["relative_path"]: r for r in files_rows}
    symbols_by_file: dict = {}
    for r in symbols_rows:
        symbols_by_file.setdefault(r["relative_path"], []).append(r)

    out: list = []
    budget = Budget(opts.max_lines, opts.max_characters)
    focus_files: list = []
    stem_parts = []

    out.append("# Repo Context Packet\n")
    out.append(f"- Root: `{opts.root.resolve().name}`")
    if opts.file:
        out.append(f"- Requested file: `{opts.file}`")
    if opts.symbol:
        out.append(f"- Requested symbol: `{opts.symbol}`")
    if opts.search:
        out.append(f"- Requested search text: `{opts.search}`")
    if opts.line:
        out.append(f"- Requested line: `{opts.line}`")
    if opts.changed:
        out.append(f"- Requested changed files: {', '.join('`' + c + '`' for c in opts.changed)}")
    out.append(f"- caller_depth={opts.caller_depth}, callee_depth={opts.callee_depth}, "
               f"max_files={opts.max_files}, max_lines={opts.max_lines}, max_characters={opts.max_characters}\n")

    def add_file_section(rel_path: str) -> None:
        rel_path = _norm_rel(rel_path)
        stem_parts.append(rel_path)
        frow = files_by_path.get(rel_path)
        out.append(f"\n## File: `{rel_path}`\n")
        if frow is None:
            out.append("_Not found in file_inventory.csv (not scanned, or excluded). Run `scan` first if needed._\n")
            budget.omissions.append(f"`{rel_path}` was not found in the inventory.")
            return
        if frow.get("included") != "true":
            out.append(f"_File was excluded from the scan (reason: {frow.get('exclusion_reason')})._\n")
            budget.omissions.append(f"`{rel_path}` was excluded from the scan: {frow.get('exclusion_reason')}.")
            return
        if len(focus_files) >= opts.max_files:
            budget.omissions.append(f"`{rel_path}` omitted: max_files limit ({opts.max_files}) reached.")
            return
        focus_files.append(rel_path)

        file_symbols = sorted(symbols_by_file.get(rel_path, []), key=lambda r: int(r["start_line"]))
        top_level = [r for r in file_symbols if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"]
        if top_level:
            out.append("Top-level symbols:\n")
            for r in top_level:
                out.append(f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})")
            out.append("")

        file_imports = [i for i in imports_rows if i["source_file"] == rel_path]
        if file_imports:
            out.append("Imports:\n")
            for i in file_imports:
                target = f" -> `{i['resolved_file']}`" if i["resolved_file"] else f" ({i['resolution_status']})"
                name = i["imported_name"] or i["imported_module"]
                out.append(f"- line {i['line']}: `{name}`{target}")
            out.append("")

        tests = _candidate_tests_for_file(rel_path, imports_rows, calls_rows, files_by_path)
        if tests:
            out.append("Candidate tests referencing this file:\n")
            for t in tests:
                out.append(f"- `{t}`")
            out.append("")

        for r in top_level:
            _render_symbol_block(opts.root, r, budget, out)

    def add_symbol_section(row: dict) -> None:
        stem_parts.append(row["qualified_name"])
        out.append(f"\n## Symbol: `{row['qualified_name']}` — `{row['relative_path']}`\n")
        enclosing = _enclosing_class_or_func(row, symbols_by_file)
        if enclosing:
            out.append(f"Enclosing scope: `{enclosing['qualified_name']}` ({enclosing['symbol_type']}, "
                        f"lines {enclosing['start_line']}-{enclosing['end_line']})\n")
        _render_symbol_block(opts.root, row, budget, out)

        callers = _bfs_callers(row["relative_path"], row["qualified_name"], calls_rows, opts.caller_depth)
        callees = _bfs_callees(row["relative_path"], row["qualified_name"], calls_rows, opts.callee_depth)
        resolved_callers = [c for c in callers if c["confidence"] != "unresolved"]
        resolved_callees = [c for c in callees if c["confidence"] != "unresolved"]

        if resolved_callers:
            out.append(f"\nCallers (statically resolved, depth {opts.caller_depth}):\n")
            for c in resolved_callers:
                out.append(f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
                            f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']})")
        if resolved_callees:
            out.append(f"\nCallees (statically resolved, depth {opts.callee_depth}):\n")
            for c in resolved_callees:
                out.append(f"- `{c['call_expression']}` at line {c['line']} "
                            f"-> `{c['candidate_symbol']}` in `{c['candidate_file']}` "
                            f"({c['confidence']}: {c['explanation']})")

        unresolved = [c for c in (callers + callees) if c["confidence"] == "unresolved"]
        if unresolved:
            out.append("\nUnresolved relationships (not statically provable):\n")
            for c in unresolved[:20]:
                out.append(f"- `{c['call_expression']}` at `{c['caller_file']}`:{c['line']} — {c['explanation']}")
            if len(unresolved) > 20:
                out.append(f"- ... and {len(unresolved) - 20} more (see python_calls.csv)")

    # --- dispatch on request type ---
    if opts.file:
        add_file_section(_norm_rel(opts.file))

    if opts.changed:
        for c in opts.changed:
            add_file_section(_norm_rel(c))

    if opts.symbol:
        candidates = _find_symbol_candidates(opts.symbol, symbols_rows, _norm_rel(opts.file) if opts.file else None)
        if len(candidates) == 0:
            out.append(f"\n_No symbol matching `{opts.symbol}` was found in python_symbols.csv._\n")
        elif len(candidates) > 1 and not opts.all_matches:
            out.append(f"\n## Ambiguous symbol: `{opts.symbol}` ({len(candidates)} candidates)\n")
            out.append("Refine with a fully qualified name, `--file`, or pass `--all-matches`:\n")
            for r in candidates:
                out.append(f"- `{r['qualified_name']}` in `{r['relative_path']}`:{r['start_line']}-{r['end_line']}")
        else:
            for row in candidates:
                add_symbol_section(row)

    if opts.line:
        try:
            file_part, line_part = opts.line.rsplit(":", 1)
            target_line = int(line_part)
        except ValueError:
            out.append(f"\n_Could not parse `--line` value `{opts.line}`; expected RELATIVE_PATH:LINE._\n")
        else:
            target_file = _norm_rel(file_part)
            enclosing = [
                r for r in symbols_by_file.get(target_file, [])
                if int(r["start_line"]) <= target_line <= int(r["end_line"]) and r["symbol_type"] != "module"
            ]
            if not enclosing:
                out.append(f"\n_No symbol in `{target_file}` encloses line {target_line}. "
                            f"Showing raw excerpt instead._\n")
                excerpt = _safe_excerpt(opts.root, target_file, max(1, target_line - 10), target_line + 10)
                if excerpt:
                    body = "\n".join(f"{ln:>6}| {t}" for ln, t in excerpt)
                    out.append("```\n" + redact_secrets(body) + "\n```\n")
                stem_parts.append(f"{target_file}_{target_line}")
            else:
                enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
                add_symbol_section(enclosing[0])

    if opts.search:
        matches = []
        for frow in files_rows:
            if frow.get("included") != "true" or frow.get("text_or_binary") == "binary":
                continue
            if len(matches) >= opts.max_files * 5:
                break
            excerpt = _safe_excerpt(opts.root, frow["relative_path"], 1, 10_000_000)
            if excerpt is None:
                continue
            for ln, text in excerpt:
                if opts.search in text:
                    matches.append((frow["relative_path"], ln, text))
        out.append(f"\n## Search results for `{opts.search}` ({len(matches)} match(es))\n")
        files_shown = []
        for rel_path, ln, text in matches:
            if rel_path not in files_shown:
                if len(files_shown) >= opts.max_files:
                    budget.omissions.append(
                        f"Additional search matches omitted beyond max_files={opts.max_files}; "
                        f"see file_inventory.csv and re-run with a narrower --search or higher --max-files."
                    )
                    break
                files_shown.append(rel_path)
            line_text = redact_secrets(text)
            out.append(f"- `{rel_path}:{ln}` — `{line_text.strip()[:200]}`")
            enclosing = [
                r for r in symbols_by_file.get(rel_path, [])
                if int(r["start_line"]) <= ln <= int(r["end_line"]) and r["symbol_type"] != "module"
            ]
            if enclosing:
                enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
                out.append(f"  (within `{enclosing[0]['qualified_name']}`)")
        stem_parts.append(f"search_{opts.search}")

    if budget.omissions:
        out.append("\n## Omitted / unresolved\n")
        for o in budget.omissions:
            out.append(f"- {o}")

    out.append(f"\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
               f"dispatch. See README.md in this output directory for full limitations._\n")

    text = "\n".join(out) + "\n"

    if opts.name:
        stem = sanitize_stem(opts.name)
    elif stem_parts:
        stem = sanitize_stem("_".join(stem_parts)[:80])
    else:
        stem = "packet"
    packet_path = opts.output_dir / "packets" / f"packet_{stem}.md"
    atomic_write_text(packet_path, text)
    return packet_path

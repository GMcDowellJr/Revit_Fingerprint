"""`validate` command: sanity-check a previously generated output directory."""
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path

from rc_common import CSV_SCHEMAS

REQUIRED_FILES = [
    "README.md", "repository_overview.md", "repository_tree.txt",
    "file_inventory.csv", "file_inventory.jsonl",
    "python_symbols.csv", "python_symbols.jsonl",
    "python_imports.csv", "python_calls.csv",
    "entrypoint_candidates.csv", "parse_warnings.csv",
    "chunk_manifest.csv", "generation_manifest.json",
]
# "packets" is deliberately not required: `scan` creates it, but it's
# typically empty right after a scan (packets are only written by the
# `packet` command), and git does not track empty directories -- requiring
# it would make validate fail on a freshly cloned/checked-out output
# directory that happens to have no packets yet.
REQUIRED_DIRS = ["chunks"]

ABS_PATH_PATTERN = re.compile(
    r"[A-Za-z]:\\[^\s`\"']+|/(?:home|Users|root|usr|var|opt|mnt|tmp)/[^\s`\"']+"
)

# Free-text files that are entirely our own generated prose/paths (never a
# verbatim reproduction of scanned source), so a full-text scan for a
# leaked absolute path is meaningful and safe from false positives caused
# by source content (shebangs, escaped strings, Windows-path examples in
# docstrings, etc.).
PROSE_FILES_TO_SCAN = ["repository_tree.txt", "repository_overview.md", "README.md",
                       "generation_manifest.json"]

# For CSVs we only check columns that are supposed to hold clean,
# repo-relative, forward-slash paths -- not free-text columns (docstrings,
# call expressions, parse messages) that legitimately reproduce arbitrary
# source content.
PATH_COLUMNS = {
    "file_inventory.csv": ["relative_path"],
    "python_symbols.csv": ["relative_path"],
    "python_imports.csv": ["source_file", "resolved_file"],
    "python_calls.csv": ["caller_file", "candidate_file"],
    "entrypoint_candidates.csv": ["relative_path"],
    "parse_warnings.csv": ["relative_path"],
    "chunk_manifest.csv": ["source_relative_path", "chunk_relative_path"],
}


def _looks_absolute_or_backslashed(value: str) -> bool:
    if not value:
        return False
    return value.startswith("/") or "\\" in value or bool(re.match(r"^[A-Za-z]:[\\/]", value))


class ValidationResult:
    def __init__(self):
        self.errors: list = []
        self.warnings: list = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    @property
    def ok(self) -> bool:
        return not self.errors


def _read_csv_rows(path: Path):
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    return rows


def validate_output_dir(output_dir: Path, allow_absolute_paths: bool = False) -> ValidationResult:
    res = ValidationResult()

    if not output_dir.exists() or not output_dir.is_dir():
        res.error(f"Output directory does not exist: {output_dir}")
        return res

    for name in REQUIRED_FILES:
        p = output_dir / name
        if not p.exists():
            res.error(f"Required output file missing: {name}")
        elif not p.is_file():
            res.error(f"Required output path is not a file: {name}")
        else:
            try:
                p.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                res.error(f"Required output file not readable: {name} ({exc})")

    for name in REQUIRED_DIRS:
        p = output_dir / name
        if not p.exists() or not p.is_dir():
            res.error(f"Required output directory missing: {name}/")

    if not res.ok:
        return res  # can't meaningfully continue without the basics

    bad_header_files: set = set()  # a mismatched/missing header makes named-column access unsafe
    for filename, header in CSV_SCHEMAS.items():
        rows = _read_csv_rows(output_dir / filename)
        if not rows:
            res.error(f"{filename}: missing header row")
            bad_header_files.add(filename)
            continue
        if tuple(rows[0]) != tuple(header):
            res.error(f"{filename}: header mismatch.\n  expected: {header}\n  actual:   {tuple(rows[0])}")
            bad_header_files.add(filename)

    try:
        manifest = json.loads((output_dir / "generation_manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        res.error(f"generation_manifest.json is not valid JSON: {exc}")
        manifest = {}

    if "file_inventory.csv" in bad_header_files:
        res.error("file_inventory.csv header is malformed; skipping all checks that depend on its columns")
        return res  # nearly everything below depends on relative_path/sha256/etc. by name

    inventory_rows = list(csv.DictReader(open(output_dir / "file_inventory.csv", encoding="utf-8", newline="")))
    inventory_by_path = {r["relative_path"]: r for r in inventory_rows}

    symbol_rows = []
    if "python_symbols.csv" in bad_header_files:
        res.error("python_symbols.csv header is malformed; skipping symbol-reference checks")
    else:
        symbol_rows = list(csv.DictReader(open(output_dir / "python_symbols.csv", encoding="utf-8", newline="")))
        for r in symbol_rows:
            inv = inventory_by_path.get(r["relative_path"])
            if inv is None:
                res.error(f"python_symbols.csv references unknown file: {r['relative_path']}")
            elif inv["included"] != "true":
                res.error(f"python_symbols.csv references an excluded file: {r['relative_path']}")

    by_source: dict = {}
    malformed_sources: set = set()  # sources with an unparseable row -- coverage can't be verified for them
    if "chunk_manifest.csv" in bad_header_files:
        res.error("chunk_manifest.csv header is malformed; skipping chunk coverage checks")
        chunk_rows = []
    else:
        chunk_rows = list(csv.DictReader(open(output_dir / "chunk_manifest.csv", encoding="utf-8", newline="")))
    for r in chunk_rows:
        src = r["source_relative_path"]
        inv = inventory_by_path.get(src)
        if inv is None:
            res.error(f"chunk_manifest.csv references unknown source file: {src}")
            continue
        if inv["included"] != "true":
            res.error(f"chunk_manifest.csv references an excluded source file: {src}")
        if inv["sha256"] != r["source_sha256"]:
            res.error(f"chunk_manifest.csv source_sha256 stale for {src} "
                      f"(chunk: {r['source_sha256']}, current: {inv['sha256']})")

        chunk_path = output_dir / r["chunk_relative_path"]
        if not chunk_path.exists():
            res.error(f"Chunk file missing: {r['chunk_relative_path']}")
            malformed_sources.add(src)
            continue
        actual_hash = hashlib.sha256(chunk_path.read_bytes()).hexdigest()
        if actual_hash != r["chunk_sha256"]:
            res.error(f"Chunk hash mismatch for {r['chunk_relative_path']} "
                      f"(manifest: {r['chunk_sha256']}, actual: {actual_hash})")

        try:
            start, end, _chunk_number = int(r["start_line"]), int(r["end_line"]), int(r["chunk_number"])
        except (ValueError, TypeError):
            res.error(f"chunk_manifest.csv row for {r['chunk_relative_path']} has a malformed numeric "
                      f"field (start_line={r.get('start_line')!r}, end_line={r.get('end_line')!r}, "
                      f"chunk_number={r.get('chunk_number')!r})")
            malformed_sources.add(src)  # can't safely order/verify this file's coverage anymore
            continue
        if start < 1 or end < start:
            res.error(f"Invalid chunk line range for {r['chunk_relative_path']}: {start}-{end}")
        line_count = inv.get("line_count")
        if line_count and line_count.isdigit() and end > int(line_count):
            res.error(f"Chunk {r['chunk_relative_path']} end line {end} exceeds source line count {line_count}")

        by_source.setdefault(src, []).append(r)

    for src, rows in by_source.items():
        if src in malformed_sources:
            continue
        rows_sorted = sorted(rows, key=lambda r: int(r["chunk_number"]))
        if int(rows_sorted[0]["start_line"]) != 1:
            res.error(f"Chunked file {src} does not start coverage at line 1 "
                      f"(first chunk starts at {rows_sorted[0]['start_line']})")
        for a, b in zip(rows_sorted, rows_sorted[1:]):
            a_end, b_start = int(a["end_line"]), int(b["start_line"])
            if b_start > a_end + 1:
                res.error(f"Chunked file {src} has a coverage gap between chunk "
                          f"{a['chunk_number']} (ends {a_end}) and chunk {b['chunk_number']} (starts {b_start})")
        line_count = inventory_by_path.get(src, {}).get("line_count")
        if line_count and line_count.isdigit():
            last_end = int(rows_sorted[-1]["end_line"])
            if last_end != int(line_count):
                res.error(f"Chunked file {src} coverage ends at line {last_end}, "
                          f"expected {line_count} (full source line count)")

    # A file marked chunked=true in the inventory must have *some* row in
    # chunk_manifest.csv -- if every row for it was deleted (not just
    # malformed), the loop above never sees it at all and would otherwise
    # silently report zero errors.
    for rel_path, inv in inventory_by_path.items():
        if inv.get("chunked") == "true" and rel_path not in by_source and rel_path not in malformed_sources:
            res.error(f"{rel_path} is marked chunked=true in file_inventory.csv but has no rows in "
                      f"chunk_manifest.csv")

    packets_dir = output_dir / "packets"
    symbol_names = {r["qualified_name"] for r in symbol_rows}
    for packet_file in sorted(packets_dir.glob("*.md")):
        try:
            text = packet_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            res.error(f"Packet file not readable: {packet_file.name} ({exc})")
            continue
        if not text.strip():
            res.error(f"Packet file is empty: {packet_file.name}")
            continue
        m = re.search(r"Requested file: `([^`]+)`", text)
        if m and m.group(1) not in inventory_by_path:
            res.error(f"Packet {packet_file.name} references unknown file: {m.group(1)}")
        m = re.search(r"Requested symbol: `([^`]+)`", text)
        if m:
            sym = m.group(1)
            if not any(_symbol_name_matches(sym, name) for name in symbol_names):
                res.warn(f"Packet {packet_file.name} references a symbol not found in python_symbols.csv: {sym}")

    if not allow_absolute_paths:
        for name in PROSE_FILES_TO_SCAN:
            path = output_dir / name
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            m = ABS_PATH_PATTERN.search(text)
            if m:
                res.error(f"Absolute path leaked into output: {name} "
                          f"(matched `{m.group(0)[:60]}`); pass --allow-absolute-paths to permit this diagnostically")

        for filename, columns in PATH_COLUMNS.items():
            path = output_dir / filename
            if not path.exists():
                continue
            with open(path, "r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    for col in columns:
                        val = row.get(col, "")
                        if _looks_absolute_or_backslashed(val):
                            res.error(f"{filename}: column '{col}' contains a non-portable path "
                                      f"(expected repo-relative, forward-slash): `{val[:80]}`; "
                                      f"pass --allow-absolute-paths to permit this diagnostically")

    return res


def _symbol_name_matches(requested: str, qualified: str) -> bool:
    return qualified == requested or qualified.split(".")[-1] == requested or qualified.endswith("." + requested)


def format_report(res: ValidationResult, output_dir: Path) -> str:
    lines = [f"Validation report for {output_dir}"]
    lines.append(f"  Errors:   {len(res.errors)}")
    lines.append(f"  Warnings: {len(res.warnings)}")
    for e in res.errors:
        lines.append(f"  [ERROR] {e}")
    for w in res.warnings:
        lines.append(f"  [WARN]  {w}")
    lines.append("PASS" if res.ok else "FAIL")
    return "\n".join(lines)

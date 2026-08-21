"""Filesystem walk + inventory + Python analysis orchestration.

Read-only: this module opens files for reading only (never writes into the
scanned tree), never imports or executes scanned source, and never follows
a symlink that points outside the selected root.
"""
from __future__ import annotations

import ast
import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from rc_common import (
    FileRecord, ChunkRecord, DEFAULT_EXCLUDE_DIRS, DEFAULT_EXCLUDE_FILE_GLOBS,
    BINARY_EXTENSIONS, match_any_glob, sniff_binary, sha256_file, count_lines_streaming,
)
import rc_classify
import rc_pyanalysis
import rc_chunking

MAX_TEXT_READ_BYTES = 30_000_000


@dataclass
class ScanOptions:
    root: Path
    output_dir: Path
    extra_exclude_dirs: set = field(default_factory=set)
    output_exclude_rel_path: Optional[str] = None  # exact repo-relative posix path, not a bare name
    exclude_globs: list = field(default_factory=list)
    include_globs: list = field(default_factory=list)
    include_secrets: bool = False
    chunk_line_threshold: int = 1000
    chunk_char_threshold: int = 80_000
    chunk_target_lines: int = 400
    chunk_overlap_lines: int = 10
    show_excluded_dirs: bool = False
    verbose: bool = False
    force: bool = False
    redact_secrets: bool = True
    chunk_reuse_provider: Optional[object] = None  # callable(rel_path, sha256) -> Optional[list[ChunkRecord]]


@dataclass
class ScanResult:
    files: list = field(default_factory=list)          # FileRecord
    symbols: list = field(default_factory=list)         # SymbolRecord
    imports: list = field(default_factory=list)         # ImportRecord
    calls: list = field(default_factory=list)           # CallRecord
    parse_warnings: list = field(default_factory=list)  # (rel, line, col, msg)
    entrypoints: list = field(default_factory=list)     # (rel, reason)
    chunks: list = field(default_factory=list)           # ChunkRecord
    dir_exclusions: list = field(default_factory=list)  # (rel, name, reason)
    reused_count: int = 0
    regenerated_count: int = 0
    stale_chunks_removed: int = 0


def _sort_key(name: str):
    return (name.lower(), name)


def _should_exclude_file(rel_posix: str, filename: str, options: ScanOptions) -> Optional[str]:
    if options.include_globs and match_any_glob(rel_posix, options.include_globs):
        return None
    if not options.include_secrets:
        hit = match_any_glob(filename, DEFAULT_EXCLUDE_FILE_GLOBS)
        if hit:
            return f"secret_pattern:{hit}"
    if options.exclude_globs:
        hit = match_any_glob(rel_posix, options.exclude_globs)
        if hit:
            return f"user_exclude_glob:{hit}"
    return None


def _walk(root: Path, exclude_dir_names: set, result: ScanResult, verbose: bool,
          output_exclude_rel_path: Optional[str] = None):
    """Yield (abs_path, rel_posix) for every file found, honoring hard
    directory exclusions and symlink-escape safety. Directories themselves
    are reported via result.dir_exclusions when skipped."""
    root_real = root.resolve()
    visited_real_dirs = {str(root_real)}

    def recurse(dir_path: Path, rel_dir: str):
        try:
            entries = sorted(os.scandir(dir_path), key=lambda e: _sort_key(e.name))
        except OSError:
            return
        for entry in entries:
            name = entry.name
            entry_path = Path(entry.path)
            rel_posix = f"{rel_dir}/{name}" if rel_dir else name

            is_symlink = entry.is_symlink()
            if is_symlink:
                try:
                    real = entry_path.resolve()
                except OSError:
                    continue
                try:
                    real.relative_to(root_real)
                    inside = True
                except ValueError:
                    inside = False
                if not inside:
                    if entry.is_dir(follow_symlinks=True):
                        result.dir_exclusions.append((rel_posix, name, "symlink_outside_root"))
                        continue
                    yield (entry_path, rel_posix, "symlink_outside_root")
                    continue
                if entry.is_dir(follow_symlinks=True):
                    if str(real) in visited_real_dirs:
                        result.dir_exclusions.append((rel_posix, name, "symlink_cycle"))
                        continue
                    visited_real_dirs.add(str(real))

            try:
                is_dir = entry.is_dir(follow_symlinks=True)
            except OSError:
                continue

            if is_dir:
                if name in exclude_dir_names:
                    result.dir_exclusions.append((rel_posix, name, "excluded_directory_name"))
                    continue
                if output_exclude_rel_path is not None and rel_posix == output_exclude_rel_path:
                    result.dir_exclusions.append((rel_posix, name, "output_directory"))
                    continue
                yield from recurse(entry_path, rel_posix)
            else:
                yield (entry_path, rel_posix, None)

    yield from recurse(root, "")


def scan_repository(options: ScanOptions) -> ScanResult:
    result = ScanResult()
    exclude_dir_names = set(DEFAULT_EXCLUDE_DIRS) | set(options.extra_exclude_dirs)

    py_analyses: dict[str, rc_pyanalysis.PyFileAnalysis] = {}

    for abs_path, rel_posix, forced_reason in _walk(
        options.root, exclude_dir_names, result, options.verbose, options.output_exclude_rel_path,
    ):
        filename = abs_path.name
        ext = abs_path.suffix.lower()

        if options.verbose:
            print(f"scan: {rel_posix}")

        try:
            size_bytes = abs_path.stat().st_size
        except OSError:
            result.files.append(FileRecord(
                relative_path=rel_posix, filename=filename, extension=ext,
                category="unknown", size_bytes=0, is_binary=False, line_count=None,
                sha256="", included=False, exclusion_reason="unreadable_io_error",
            ))
            continue

        exclusion_reason = forced_reason or _should_exclude_file(rel_posix, filename, options)
        included = exclusion_reason is None

        try:
            sha = sha256_file(abs_path)
        except OSError:
            result.files.append(FileRecord(
                relative_path=rel_posix, filename=filename, extension=ext,
                category="unknown", size_bytes=size_bytes, is_binary=False, line_count=None,
                sha256="", included=False, exclusion_reason="unreadable_io_error",
            ))
            continue

        is_bin = ext in BINARY_EXTENSIONS
        sample_text = None
        full_text = None
        line_count = None
        char_count = 0
        parse_status = "n/a"

        if not is_bin:
            try:
                with open(abs_path, "rb") as fh:
                    sample = fh.read(8192)
            except OSError:
                sample = b""
            if sniff_binary(sample):
                is_bin = True

        if not is_bin and included and size_bytes <= MAX_TEXT_READ_BYTES:
            try:
                raw = abs_path.read_bytes()
                full_text = raw.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    full_text = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    full_text = raw.decode("latin-1", errors="replace")
            except OSError:
                full_text = None
            if full_text is not None:
                line_count = full_text.count("\n") + (0 if full_text.endswith("\n") or full_text == "" else 1)
                char_count = len(full_text)
                sample_text = full_text[:2000]
        elif not is_bin and not included:
            # Still classify excluded files without reading their full content.
            try:
                with open(abs_path, "rb") as fh:
                    sample_text = fh.read(2000).decode("utf-8", errors="replace")
            except OSError:
                sample_text = None
            line_count = None
        elif not is_bin and included:
            # size_bytes > MAX_TEXT_READ_BYTES: too large to safely hold in
            # memory whole. Still stream a small sample for classification
            # and a full line count without loading the whole file, but
            # skip symbol/import/call analysis and chunking -- and say so
            # explicitly via parse_status rather than looking like an
            # ordinary fully-processed file.
            parse_status = "skipped_too_large"
            try:
                with open(abs_path, "rb") as fh:
                    sample_text = fh.read(2000).decode("utf-8", errors="replace")
            except OSError:
                sample_text = None
            try:
                line_count = count_lines_streaming(abs_path)
            except OSError:
                line_count = None

        category, class_reason = rc_classify.classify_file(rel_posix, filename, ext, sample_text)
        gen_vendor = rc_classify.detect_generated_or_vendor(rel_posix, sample_text)

        chunked = False

        if included and ext == ".py" and full_text is not None:
            try:
                analysis = rc_pyanalysis.analyze_python_source(rel_posix, full_text)
                py_analyses[rel_posix] = analysis
                parse_status = "ok"
                reason = rc_classify.detect_entrypoint_reason(rel_posix, analysis.has_main_guard)
                if reason:
                    result.entrypoints.append((rel_posix, reason))
            except SyntaxError as exc:
                parse_status = "failed"
                result.parse_warnings.append((rel_posix, exc.lineno or 0, exc.offset or 0, str(exc.msg)))
            except (RecursionError, ValueError) as exc:
                parse_status = "failed"
                result.parse_warnings.append((rel_posix, 0, 0, f"{type(exc).__name__}: {exc}"))

        if included and full_text is not None:
            needs_chunk = line_count is not None and (
                line_count > options.chunk_line_threshold or char_count > options.chunk_char_threshold
            )
            if needs_chunk:
                reused_chunks = None
                if options.chunk_reuse_provider is not None:
                    reused_chunks = options.chunk_reuse_provider(rel_posix, sha)
                if reused_chunks is not None:
                    result.chunks.extend(reused_chunks)
                    chunked = bool(reused_chunks)
                    result.reused_count += 1
                else:
                    symbols_for_file = py_analyses[rel_posix].symbols if rel_posix in py_analyses else None
                    chunk_records = rc_chunking.chunk_file(
                        rel_posix, full_text, sha, symbols_for_file, options.output_dir, options,
                    )
                    result.chunks.extend(chunk_records)
                    chunked = bool(chunk_records)
                    if chunk_records:
                        result.regenerated_count += 1

        result.files.append(FileRecord(
            relative_path=rel_posix, filename=filename, extension=ext,
            category=category, size_bytes=size_bytes, is_binary=is_bin,
            line_count=line_count, sha256=sha, included=included,
            exclusion_reason=exclusion_reason or "", chunked=chunked,
            parse_status=parse_status, generated_or_vendor=gen_vendor,
            classification_reason=class_reason,
        ))

    _resolve_python_relationships(py_analyses, result)
    result.stale_chunks_removed = _cleanup_stale_chunks(options.output_dir, result)
    return result


def _cleanup_stale_chunks(output_dir: Path, result: ScanResult) -> int:
    """Remove chunk files left over from a prior run that this run's final
    chunk_manifest no longer references (source deleted, excluded, or no
    longer large enough to need chunking)."""
    chunks_dir = output_dir / "chunks"
    if not chunks_dir.exists():
        return 0
    referenced = {(output_dir / c.chunk_relative_path).resolve() for c in result.chunks}
    removed = 0
    for entry in chunks_dir.iterdir():
        if entry.is_file() and entry.resolve() not in referenced:
            try:
                entry.unlink()
                removed += 1
            except OSError:
                pass
    return removed


def _resolve_python_relationships(py_analyses: dict, result: ScanResult) -> None:
    module_index: dict[str, list] = {}
    for rel_path in py_analyses:
        dotted = rc_pyanalysis.dotted_module_path(rel_path)
        module_index.setdefault(dotted, []).append(rel_path)

    all_top_level_index = {rel: a.top_level_index for rel, a in py_analyses.items()}
    all_class_info = {rel: a.class_info for rel, a in py_analyses.items()}

    for rel_path, analysis in py_analyses.items():
        for imp in analysis.imports:
            resolved_file, status = rc_pyanalysis.resolve_import_record(imp, module_index, all_top_level_index)
            imp.resolved_file = resolved_file
            imp.resolution_status = status
        result.imports.extend(analysis.imports)
        result.symbols.extend(analysis.symbols)

    for rel_path, analysis in py_analyses.items():
        bindings = rc_pyanalysis.build_import_bindings(analysis.imports)
        calls = rc_pyanalysis.resolve_calls(
            analysis.raw_calls, rel_path, analysis.top_level_index, analysis.class_info,
            bindings, all_top_level_index, all_class_info,
        )
        result.calls.extend(calls)

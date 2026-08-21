"""Incremental reuse support + generation_manifest.json.

Correctness beats speed: only chunk *output* (a pure function of a file's
own content plus the chunking options) is ever reused between runs. File
inventory, Python symbol/import/call analysis, and the aggregate index
files are always recomputed fresh on every run.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from rc_common import ChunkRecord, TOOL_VERSION, sha256_file, atomic_write_text


def chunking_signature(options) -> dict:
    return {
        "chunk_line_threshold": options.chunk_line_threshold,
        "chunk_char_threshold": options.chunk_char_threshold,
        "chunk_target_lines": options.chunk_target_lines,
        "chunk_overlap_lines": options.chunk_overlap_lines,
        "redact_secrets": options.redact_secrets,
    }


def load_previous_state(output_dir: Path):
    """Returns (prev_hash_by_path, prev_chunks_by_path, prev_signature) or
    (None, None, None) if no usable previous run is present."""
    manifest_path = output_dir / "generation_manifest.json"
    inventory_path = output_dir / "file_inventory.csv"
    chunk_manifest_path = output_dir / "chunk_manifest.csv"
    if not (manifest_path.exists() and inventory_path.exists() and chunk_manifest_path.exists()):
        return None, None, None

    try:
        prev_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, None, None
    prev_signature = prev_manifest.get("chunking_options")
    if prev_signature is None:
        return None, None, None

    prev_hash_by_path = {}
    try:
        with open(inventory_path, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                prev_hash_by_path[row["relative_path"]] = row["sha256"]
    except OSError:
        return None, None, None

    prev_chunks_by_path: dict[str, list] = {}
    try:
        with open(chunk_manifest_path, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                rec = ChunkRecord(
                    source_relative_path=row["source_relative_path"],
                    chunk_relative_path=row["chunk_relative_path"],
                    chunk_number=int(row["chunk_number"]),
                    start_line=int(row["start_line"]),
                    end_line=int(row["end_line"]),
                    overlap_lines=int(row["overlap_lines"]),
                    symbols=row["symbols"],
                    source_sha256=row["source_sha256"],
                    chunk_sha256=row["chunk_sha256"],
                    char_count=int(row["char_count"]),
                    estimated_tokens=int(row["estimated_tokens"]),
                )
                prev_chunks_by_path.setdefault(rec.source_relative_path, []).append(rec)
    except OSError:
        return None, None, None

    return prev_hash_by_path, prev_chunks_by_path, prev_signature


def make_chunk_reuse_provider(output_dir: Path, options, force: bool):
    if force:
        return None
    prev_hash_by_path, prev_chunks_by_path, prev_signature = load_previous_state(output_dir)
    if prev_hash_by_path is None:
        return None
    if prev_signature != chunking_signature(options):
        return None

    def provider(rel_path: str, current_sha: str):
        if prev_hash_by_path.get(rel_path) != current_sha:
            return None
        records = prev_chunks_by_path.get(rel_path)
        if not records:
            return None
        for rec in records:
            chunk_path = output_dir / rec.chunk_relative_path
            if not chunk_path.exists():
                return None
            try:
                data = chunk_path.read_bytes()
            except OSError:
                return None
            import hashlib
            if hashlib.sha256(data).hexdigest() != rec.chunk_sha256:
                return None
        return records

    return provider


def write_manifest(output_dir: Path, options, result, root: Path, started_at_utc: str,
                    reuse_active: bool, extra: dict) -> None:
    counts = {
        "files_included": sum(1 for f in result.files if f.included),
        "files_excluded": sum(1 for f in result.files if not f.included),
        "python_files": sum(1 for f in result.files if f.included and f.extension == ".py"),
        "symbols": len(result.symbols),
        "imports": len(result.imports),
        "calls": len(result.calls),
        "chunks": len(result.chunks),
        "chunked_files": sum(1 for f in result.files if f.chunked),
        "parse_failures": len(result.parse_warnings),
        "entrypoint_candidates": len(result.entrypoints),
        "excluded_directories": len(result.dir_exclusions),
    }
    manifest = {
        "tool_version": TOOL_VERSION,
        "generated_at_utc": started_at_utc,
        "root_name": root.resolve().name,
        "chunking_options": chunking_signature(options),
        "exclude_dir_names_default_count": None,
        "options": {
            "extra_exclude_dirs": sorted(options.extra_exclude_dirs),
            "exclude_globs": options.exclude_globs,
            "include_globs": options.include_globs,
            "include_secrets": options.include_secrets,
            "show_excluded_dirs": options.show_excluded_dirs,
        },
        "counts": counts,
        "incremental": {
            "reuse_active": reuse_active,
            "chunks_reused_for_files": result.reused_count,
            "chunks_regenerated_for_files": result.regenerated_count,
            "stale_chunks_removed": result.stale_chunks_removed,
        },
    }
    manifest.update(extra)
    atomic_write_text(output_dir / "generation_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

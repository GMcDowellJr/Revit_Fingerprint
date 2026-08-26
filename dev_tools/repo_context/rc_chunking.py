"""Line-numbered chunk generation for oversized text files.

Python files are split on symbol boundaries where possible; other text
files fall back to line-range splitting with a small overlap. Original
line numbers are always preserved and no source line is ever silently
dropped.
"""
from __future__ import annotations

from pathlib import Path

from rc_common import (
    ChunkRecord, stable_path_id, sanitize_stem, sha256_text, estimate_tokens,
    redact_secrets, atomic_write_text,
)

HEADING_HINT_PREFIXES = ("#", "##", "###")


def _build_python_units(total_lines: int, symbols):
    """Return ordered (start, end, label_or_None) covering 1..total_lines,
    using only top-level (module-child) symbols as named units."""
    top_level = sorted(
        (s for s in symbols if s.parent_symbol == "<module>" and s.symbol_type != "module"),
        key=lambda s: s.start_line,
    )
    units = []
    cursor = 1
    for sym in top_level:
        if sym.start_line > cursor:
            units.append((cursor, sym.start_line - 1, None))
        start = max(sym.start_line, cursor)
        units.append((start, sym.end_line, sym.qualified_name))
        cursor = sym.end_line + 1
    if cursor <= total_lines:
        units.append((cursor, total_lines, None))
    return [u for u in units if u[0] <= u[1]]


def _pack_units(units, target_lines: int):
    """Group small consecutive units into chunks near target_lines; split any
    single unit that alone exceeds ~1.5x target into line-range parts."""
    chunks = []  # list of (start, end, [labels])
    current_start = None
    current_end = None
    current_labels = []

    def flush():
        nonlocal current_start, current_end, current_labels
        if current_start is not None:
            chunks.append((current_start, current_end, current_labels))
        current_start = None
        current_end = None
        current_labels = []

    for start, end, label in units:
        length = end - start + 1
        if length > int(target_lines * 1.5):
            flush()
            part_start = start
            while part_start <= end:
                part_end = min(part_start + target_lines - 1, end)
                chunks.append((part_start, part_end, [label] if label else []))
                part_start = part_end + 1
            continue
        if current_start is None:
            current_start, current_end = start, end
            current_labels = [label] if label else []
            continue
        if (current_end - current_start + 1) + length <= int(target_lines * 1.3):
            current_end = end
            if label:
                current_labels.append(label)
        else:
            flush()
            current_start, current_end = start, end
            current_labels = [label] if label else []
    flush()
    return chunks


def _find_logical_boundary(lines: list, ideal_end: int, total_lines: int, window: int, min_end: int):
    """Search outward from ideal_end for a blank or heading line. Returns the
    adjusted end line, or None if no boundary was found in the window.
    Never returns a value below min_end (the current chunk's start) --
    otherwise the caller could produce an inverted (end < start) range
    that never advances, hanging on e.g. --chunk-target-lines 1."""
    for offset in range(0, window + 1):
        candidates = [ideal_end] if offset == 0 else [ideal_end - offset, ideal_end + offset]
        for cand in candidates:
            if min_end <= cand <= total_lines:
                text = lines[cand - 1]
                if text.strip() == "" or text.strip().startswith(HEADING_HINT_PREFIXES):
                    return cand
    return None


def _pack_generic_lines(total_lines: int, target_lines: int, overlap: int, lines: list):
    chunks = []
    start = 1
    while start <= total_lines:
        ideal_end = min(start + target_lines - 1, total_lines)
        end = ideal_end
        boundary_found = True
        if end < total_lines:
            window = min(15, max(1, target_lines // 4))
            adjusted = _find_logical_boundary(lines, ideal_end, total_lines, window, start)
            if adjusted is not None:
                end = adjusted
            else:
                boundary_found = False
        chunks.append((start, end, []))
        if end >= total_lines:
            break
        start = (end + 1) if boundary_found else max(chunks[-1][0] + 1, end + 1 - overlap)
    return chunks


def chunk_file(rel_path: str, text: str, source_sha: str, symbols, output_dir: Path, options) -> list:
    lines = text.split("\n")
    if lines and lines[-1] == "" and text.endswith("\n"):
        lines = lines[:-1]
    total_lines = len(lines)
    if total_lines == 0:
        return []

    is_python = rel_path.endswith(".py") and symbols is not None
    if is_python:
        units = _build_python_units(total_lines, symbols)
        raw_chunks = _pack_units(units, options.chunk_target_lines)
        overlap_per_chunk = [0] * len(raw_chunks)
    else:
        raw_chunks = _pack_generic_lines(total_lines, options.chunk_target_lines, options.chunk_overlap_lines, lines)
        overlap_per_chunk = []
        for i, (s, e, labels) in enumerate(raw_chunks):
            if i == 0:
                overlap_per_chunk.append(0)
            else:
                prev_end = raw_chunks[i - 1][1]
                overlap_per_chunk.append(max(0, prev_end - s + 1))

    total = len(raw_chunks)
    stable_id = stable_path_id(rel_path)
    stem = sanitize_stem(Path(rel_path).name)
    chunk_dir = output_dir / "chunks"

    all_symbols = symbols or []
    records = []
    for idx, (start, end, labels) in enumerate(raw_chunks, start=1):
        overlapping = [
            s for s in all_symbols
            if s.symbol_type != "module" and s.start_line <= end and s.end_line >= start
        ]
        overlapping.sort(key=lambda s: s.start_line)
        symbol_names = [s.qualified_name for s in overlapping]

        starts_inside = next(
            (s.qualified_name for s in overlapping if s.start_line < start <= s.end_line), None
        )
        ends_inside = next(
            (s.qualified_name for s in overlapping if s.start_line <= end < s.end_line), None
        )

        body_lines = [f"{ln:>6}| {lines[ln - 1]}" for ln in range(start, end + 1)]
        body = "\n".join(body_lines)
        if options.redact_secrets:
            body = redact_secrets(body)

        header = (
            f"# Chunk of {rel_path}\n\n"
            f"- Source relative path: `{rel_path}`\n"
            f"- Chunk: {idx} of {total}\n"
            f"- Original line range: {start}-{end}\n"
            f"- Overlap lines with previous chunk: {overlap_per_chunk[idx - 1]}\n"
            f"- Symbols fully or partially present: {', '.join(symbol_names) if symbol_names else 'none'}\n"
            f"- Source SHA-256: {source_sha}\n"
            f"- Starts inside symbol: {starts_inside or 'no'}\n"
            f"- Ends inside symbol: {ends_inside or 'no'}\n\n"
            f"```\n"
        )
        footer = "\n```\n"
        chunk_text = header + body + footer

        chunk_filename = f"{stable_id}__{stem}__part{idx:04d}.md"
        chunk_rel = f"chunks/{chunk_filename}"
        chunk_path = chunk_dir / chunk_filename
        atomic_write_text(chunk_path, chunk_text)

        records.append(ChunkRecord(
            source_relative_path=rel_path,
            chunk_relative_path=chunk_rel,
            chunk_number=idx,
            start_line=start,
            end_line=end,
            overlap_lines=overlap_per_chunk[idx - 1],
            symbols=";".join(symbol_names),
            source_sha256=source_sha,
            chunk_sha256=sha256_text(chunk_text),
            char_count=len(chunk_text),
            estimated_tokens=estimate_tokens(len(chunk_text)),
        ))
    return records

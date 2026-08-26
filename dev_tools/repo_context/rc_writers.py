"""CSV / JSONL table writers, sharing schemas with rc_validate.py."""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from rc_common import CSV_SCHEMAS, atomic_write_text


def _bool_str(v) -> str:
    return "true" if v else "false"


def _rows_to_csv_text(header, rows) -> str:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])
    return buf.getvalue()


def file_record_to_row(f) -> tuple:
    return (
        f.relative_path, f.filename, f.extension, f.category, f.size_bytes,
        "binary" if f.is_binary else "text", f.line_count if f.line_count is not None else "",
        f.sha256, _bool_str(f.included), f.exclusion_reason, _bool_str(f.chunked),
        f.parse_status, f.generated_or_vendor,
    )


def file_record_to_dict(f) -> dict:
    return {
        "relative_path": f.relative_path, "filename": f.filename, "extension": f.extension,
        "category": f.category, "size_bytes": f.size_bytes,
        "text_or_binary": "binary" if f.is_binary else "text",
        "line_count": f.line_count, "sha256": f.sha256, "included": f.included,
        "exclusion_reason": f.exclusion_reason, "chunked": f.chunked,
        "parse_status": f.parse_status, "generated_or_vendor": f.generated_or_vendor,
        "classification_reason": f.classification_reason,
    }


def symbol_record_to_row(s) -> tuple:
    return (
        s.relative_path, s.qualified_name, s.symbol_type, s.start_line, s.end_line,
        s.parent_symbol, s.decorators, s.parameters, s.base_classes, s.return_annotation,
        _bool_str(s.has_docstring), s.docstring_first_line, s.line_count,
        s.complexity_approx, s.nested_symbols,
    )


def symbol_record_to_dict(s) -> dict:
    return {
        "relative_path": s.relative_path, "qualified_name": s.qualified_name,
        "symbol_type": s.symbol_type, "start_line": s.start_line, "end_line": s.end_line,
        "parent_symbol": s.parent_symbol, "decorators": s.decorators,
        "parameters": s.parameters, "base_classes": s.base_classes,
        "return_annotation": s.return_annotation, "has_docstring": s.has_docstring,
        "docstring_first_line": s.docstring_first_line, "line_count": s.line_count,
        "complexity_approx": s.complexity_approx, "nested_symbols": s.nested_symbols,
    }


def import_record_to_row(i) -> tuple:
    return (
        i.source_file, i.source_module, i.line, i.import_type, i.imported_module,
        i.imported_name, i.alias, i.level, i.resolved_file, i.resolution_status,
    )


def call_record_to_row(c) -> tuple:
    return (
        c.caller_file, c.caller_symbol, c.line, c.call_expression, c.callee_simple_name,
        c.candidate_file, c.candidate_symbol, c.confidence, c.explanation,
    )


def chunk_record_to_row(c) -> tuple:
    return (
        c.source_relative_path, c.chunk_relative_path, c.chunk_number, c.start_line,
        c.end_line, c.overlap_lines, c.symbols, c.source_sha256, c.chunk_sha256,
        c.char_count, c.estimated_tokens,
    )


def write_all_tables(output_dir: Path, result) -> None:
    files_sorted = sorted(result.files, key=lambda f: f.relative_path)
    symbols_sorted = sorted(result.symbols, key=lambda s: (s.relative_path, s.start_line, s.qualified_name))
    imports_sorted = sorted(result.imports, key=lambda i: (i.source_file, i.line, i.imported_name))
    calls_sorted = sorted(result.calls, key=lambda c: (c.caller_file, c.line, c.call_expression))
    chunks_sorted = sorted(result.chunks, key=lambda c: (c.source_relative_path, c.chunk_number))
    entrypoints_sorted = sorted(result.entrypoints, key=lambda t: t[0])
    warnings_sorted = sorted(result.parse_warnings, key=lambda t: t[0])

    atomic_write_text(
        output_dir / "file_inventory.csv",
        _rows_to_csv_text(CSV_SCHEMAS["file_inventory.csv"], (file_record_to_row(f) for f in files_sorted)),
    )
    atomic_write_text(
        output_dir / "file_inventory.jsonl",
        "\n".join(json.dumps(file_record_to_dict(f), sort_keys=True) for f in files_sorted) + ("\n" if files_sorted else ""),
    )
    atomic_write_text(
        output_dir / "python_symbols.csv",
        _rows_to_csv_text(CSV_SCHEMAS["python_symbols.csv"], (symbol_record_to_row(s) for s in symbols_sorted)),
    )
    atomic_write_text(
        output_dir / "python_symbols.jsonl",
        "\n".join(json.dumps(symbol_record_to_dict(s), sort_keys=True) for s in symbols_sorted) + ("\n" if symbols_sorted else ""),
    )
    atomic_write_text(
        output_dir / "python_imports.csv",
        _rows_to_csv_text(CSV_SCHEMAS["python_imports.csv"], (import_record_to_row(i) for i in imports_sorted)),
    )
    atomic_write_text(
        output_dir / "python_calls.csv",
        _rows_to_csv_text(CSV_SCHEMAS["python_calls.csv"], (call_record_to_row(c) for c in calls_sorted)),
    )
    atomic_write_text(
        output_dir / "entrypoint_candidates.csv",
        _rows_to_csv_text(CSV_SCHEMAS["entrypoint_candidates.csv"], entrypoints_sorted),
    )
    atomic_write_text(
        output_dir / "parse_warnings.csv",
        _rows_to_csv_text(CSV_SCHEMAS["parse_warnings.csv"], warnings_sorted),
    )
    atomic_write_text(
        output_dir / "chunk_manifest.csv",
        _rows_to_csv_text(CSV_SCHEMAS["chunk_manifest.csv"], (chunk_record_to_row(c) for c in chunks_sorted)),
    )

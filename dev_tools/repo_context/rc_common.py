"""Shared constants, dataclasses, and small utilities for repo_context.

Pure stdlib. No repository code is ever imported or executed by this
module or anything it is used from.
"""
from __future__ import annotations

import fnmatch
import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

TOOL_VERSION = "0.1.0"

DEFAULT_EXCLUDE_DIRS = {
    ".git", ".hg", ".svn",
    "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    ".tox", ".nox",
    ".venv", "venv", "env",
    "node_modules",
    "dist", "build",
    "coverage", "htmlcov",
    ".idea", ".vscode",
    "repo_context", "_copilot_context",
}

DEFAULT_EXCLUDE_FILE_GLOBS = [
    ".env", ".env.*",
    "*.pem", "*.key", "*.pfx", "*.p12",
    "id_rsa", "id_ed25519",
    "credentials.json", "secrets.*",
]

# Extensions that are essentially always binary; used as a fast path before
# content sniffing.
BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico", ".webp", ".tiff",
    ".pdf", ".zip", ".tar", ".gz", ".7z", ".rar", ".whl",
    ".exe", ".dll", ".so", ".dylib", ".pyc", ".pyo", ".o", ".a", ".lib",
    ".class", ".jar",
    ".mp3", ".mp4", ".avi", ".mov", ".wav", ".flac",
    ".db", ".sqlite", ".sqlite3",
    ".ttf", ".otf", ".woff", ".woff2",
    ".dyn",
}

SECRET_LINE_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|secret|token|access[_-]?key|password|passwd)\s*[:=]\s*['\"]?[A-Za-z0-9/+_\-\.]{12,}['\"]?"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]
REDACTION_TEXT = "[REDACTED-POSSIBLE-SECRET]"


def redact_secrets(text: str) -> str:
    out_lines = []
    changed = False
    for line in text.split("\n"):
        new_line = line
        for pat in SECRET_LINE_PATTERNS:
            if pat.search(new_line):
                new_line = pat.sub(REDACTION_TEXT, new_line)
                changed = True
        out_lines.append(new_line)
    return "\n".join(out_lines) if changed else text


def to_posix_rel(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def count_lines_streaming(path: Path, chunk_size: int = 1 << 20) -> int:
    """Count newline-terminated lines without holding the file in memory."""
    count = 0
    last_byte = b""
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            count += block.count(b"\n")
            last_byte = block[-1:]
    if last_byte and last_byte != b"\n":
        count += 1
    return count


def stable_path_id(rel_path: str, length: int = 12) -> str:
    return hashlib.sha1(rel_path.encode("utf-8")).hexdigest()[:length]


def sanitize_stem(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    return stem[:60] if stem else "file"


def match_any_glob(name: str, patterns) -> Optional[str]:
    for pat in patterns:
        if fnmatch.fnmatch(name, pat):
            return pat
    return None


def sniff_binary(sample: bytes) -> bool:
    if b"\x00" in sample:
        return True
    if not sample:
        return False
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7F})
    nontext = sum(1 for b in sample if b not in text_chars)
    return (nontext / len(sample)) > 0.30


def read_text_best_effort(path: Path, max_bytes: int = 20_000_000):
    """Return (text, encoding, encoding_fallback: bool, is_binary: bool)."""
    try:
        raw = path.read_bytes()
    except OSError:
        return None, None, False, False
    if len(raw) > max_bytes:
        raw = raw[:max_bytes]
    sample = raw[:8192]
    if sniff_binary(sample):
        return None, None, False, True
    try:
        text = raw.decode("utf-8")
        return text, "utf-8", False, False
    except UnicodeDecodeError:
        pass
    try:
        text = raw.decode("utf-8-sig")
        return text, "utf-8-sig", True, False
    except UnicodeDecodeError:
        pass
    text = raw.decode("latin-1", errors="replace")
    return text, "latin-1-fallback", True, False


@dataclass
class FileRecord:
    relative_path: str
    filename: str
    extension: str
    category: str
    size_bytes: int
    is_binary: bool
    line_count: Optional[int]
    sha256: str
    included: bool
    exclusion_reason: str
    chunked: bool = False
    parse_status: str = "n/a"
    generated_or_vendor: str = "no"
    classification_reason: str = ""


@dataclass
class SymbolRecord:
    relative_path: str
    qualified_name: str
    symbol_type: str
    start_line: int
    end_line: int
    parent_symbol: str
    decorators: str
    parameters: str
    base_classes: str
    return_annotation: str
    has_docstring: bool
    docstring_first_line: str
    line_count: int
    complexity_approx: int
    nested_symbols: str


@dataclass
class ImportRecord:
    source_file: str
    source_module: str
    line: int
    import_type: str
    imported_module: str
    imported_name: str
    alias: str
    level: int
    resolved_file: str
    resolution_status: str


@dataclass
class CallRecord:
    caller_file: str
    caller_symbol: str
    line: int
    call_expression: str
    callee_simple_name: str
    candidate_file: str
    candidate_symbol: str
    confidence: str
    explanation: str


@dataclass
class ChunkRecord:
    source_relative_path: str
    chunk_relative_path: str
    chunk_number: int
    start_line: int
    end_line: int
    overlap_lines: int
    symbols: str
    source_sha256: str
    chunk_sha256: str
    char_count: int
    estimated_tokens: int


CSV_SCHEMAS = {
    "file_inventory.csv": (
        "relative_path", "filename", "extension", "category", "size_bytes",
        "text_or_binary", "line_count", "sha256", "included",
        "exclusion_reason", "chunked", "parse_status", "generated_or_vendor",
    ),
    "python_symbols.csv": (
        "relative_path", "qualified_name", "symbol_type", "start_line",
        "end_line", "parent_symbol", "decorators", "parameters",
        "base_classes", "return_annotation", "has_docstring",
        "docstring_first_line", "line_count", "complexity_approx",
        "nested_symbols",
    ),
    "python_imports.csv": (
        "source_file", "source_module", "line", "import_type",
        "imported_module", "imported_name", "alias", "level",
        "resolved_file", "resolution_status",
    ),
    "python_calls.csv": (
        "caller_file", "caller_symbol", "line", "call_expression",
        "callee_simple_name", "candidate_file", "candidate_symbol",
        "confidence", "explanation",
    ),
    "entrypoint_candidates.csv": ("relative_path", "reason"),
    "parse_warnings.csv": ("relative_path", "line", "column", "message"),
    "chunk_manifest.csv": (
        "source_relative_path", "chunk_relative_path", "chunk_number",
        "start_line", "end_line", "overlap_lines", "symbols",
        "source_sha256", "chunk_sha256", "char_count", "estimated_tokens",
    ),
}


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{__import__('os').getpid()}")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    tmp.replace(path)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{__import__('os').getpid()}")
    with open(tmp, "wb") as fh:
        fh.write(data)
    tmp.replace(path)


def estimate_tokens(char_count: int) -> int:
    return max(1, round(char_count / 4))

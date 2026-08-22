"""Hierarchical routing-catalog generation.

Produces `routing/index.md` (a small, persistent top-level guide) plus one
detailed catalog per partition (`routing/<key>.md`) so an LLM can go from a
natural-language question to a small set of file/symbol selectors without
already knowing an exact symbol or line number.

Deterministic, evidence-based, and read-only: everything here is derived
from the same in-memory `ScanResult` that `scan` already produced (plus an
optional, clearly-labeled Graphify adapter). No new filesystem walk, no
scoring, no invented architectural claims.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import rc_classify
import rc_graphify
from rc_common import (
    TOOL_VERSION, atomic_write_text, generated_output_exclude_paths, get_git_info, redact_secrets, sha256_text,
    stable_path_id,
)
from rc_packet import _safe_excerpt

DEFAULT_MAX_FILES_PER_CATALOG = 60
DEFAULT_MAX_CATALOG_CHARS = 24_000
DEFAULT_MAX_INDEX_CHARS = 10_000
DEFAULT_MAX_SYMBOLS_PER_FILE_ENTRY = 25
MAX_PARTITION_DEPTH = 4


@dataclass
class RoutingOptions:
    enabled: bool = True
    max_files_per_catalog: int = DEFAULT_MAX_FILES_PER_CATALOG
    max_catalog_chars: int = DEFAULT_MAX_CATALOG_CHARS
    max_index_chars: int = DEFAULT_MAX_INDEX_CHARS
    max_symbols_per_file_entry: int = DEFAULT_MAX_SYMBOLS_PER_FILE_ENTRY
    allow_stale_graphify: bool = False


def _sort_key(rel_path: str):
    return (rel_path.lower(), rel_path)


def _partition_files(files_with_role: list, max_files: int) -> dict:
    """files_with_role: [(FileRecord, role), ...]. Returns
    {catalog_key: [FileRecord, ...]}, sorted-deterministic, every input
    file assigned to exactly one key.

    Bucketing rule: test-classified and archived/legacy files always get
    their own dedicated catalog (so they never dilute or dominate an
    active-code catalog); everything else buckets by top-level directory
    (or "other" for loose files directly at the repository root), further
    split by additional path segments only when a bucket's file count
    exceeds `max_files` -- so the partition adapts to this repository's
    actual directory shape instead of a hardcoded domain list.
    """
    buckets: dict = defaultdict(list)
    for f, role in files_with_role:
        if role == "test_harness":
            key = "tests"
        elif role == "archived_or_legacy":
            key = "archived"
        else:
            parts = f.relative_path.split("/")
            key = parts[0] if len(parts) > 1 else "other"
        buckets[key].append(f)

    final: dict = {}

    def split(key: str, files: list, depth: int) -> None:
        if len(files) <= max_files or depth >= MAX_PARTITION_DEPTH:
            final.setdefault(key, []).extend(files)
            return
        depth_index = key.count("/") + 1
        direct, by_sub = [], defaultdict(list)
        for f in files:
            parts = f.relative_path.split("/")
            if len(parts) > depth_index + 1:
                by_sub[parts[depth_index]].append(f)
            else:
                direct.append(f)
        if not by_sub:
            # No deeper directory structure to split on -- accept the
            # oversized catalog rather than losing files or guessing.
            final.setdefault(key, []).extend(files)
            return
        if direct:
            final.setdefault(key, []).extend(direct)
        for sub, sub_files in sorted(by_sub.items()):
            split(f"{key}/{sub}", sub_files, depth + 1)

    for key in sorted(buckets):
        split(key, buckets[key], 1)
    return final


_MAX_CATALOG_FILENAME_BYTES = 150  # well under typical filesystem name-length limits (~255 bytes,
# which are themselves a byte count, not a character count), leaving headroom for
# atomic_write_text's own temp-file suffix.


def _byte_len(s: str) -> int:
    return len(s.encode("utf-8"))


def _truncate_to_byte_limit(s: str, max_bytes: int) -> str:
    """Truncate `s` so its UTF-8 encoding is at most `max_bytes` long,
    without splitting a multi-byte character in half (which would produce
    a mangled/invalid filename component). A plain `s[:n]` slices by code
    point, not by encoded byte -- for a multi-byte alphabet (e.g. CJK,
    3 bytes/char in UTF-8) that undercounts real filesystem cost by up to
    3x, so a filename that looked comfortably under the cap by character
    count could still exceed the actual byte limit."""
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _catalog_filenames(keys: list) -> dict:
    """Injective key -> filename mapping. A naive "/" -> "_" substitution
    can collide (partition key "a/b" and a separate top-level partition
    "a_b" would both naively become "a_b.md", silently overwriting one),
    so any keys that collide under that substitution get a short stable
    hash suffix appended -- deterministic, and only paid for the
    (uncommon) colliding keys, so the common case stays readable.

    Also caps the readable portion when the naive filename alone would
    exceed the filesystem's per-component name limit -- an oversized
    partition split through several long directory segments (e.g. four
    70-character directory names) can otherwise flatten into a filename
    long enough to make the write itself fail with OSError: File name too
    long, independent of whether it collides with anything. The cap is
    measured and enforced in encoded UTF-8 *bytes*, not Python string
    length (code points) -- filesystem name limits are a byte count, and
    a partition key built from multi-byte characters (e.g. CJK path
    segments) can look well under a character-count cap while its UTF-8
    encoding is 2-3x that many bytes."""
    naive = {key: key.replace("/", "_") + ".md" for key in keys}
    by_filename: dict = defaultdict(list)
    for key, filename in naive.items():
        by_filename[filename].append(key)
    result = {}
    for filename, colliding_keys in by_filename.items():
        if len(colliding_keys) == 1 and _byte_len(filename) <= _MAX_CATALOG_FILENAME_BYTES:
            result[colliding_keys[0]] = filename
            continue
        for key in colliding_keys:
            flat = key.replace("/", "_")
            suffix = f"__{stable_path_id(key, length=6)}.md"
            prefix_budget_bytes = max(1, _MAX_CATALOG_FILENAME_BYTES - _byte_len(suffix))
            result[key] = f"{_truncate_to_byte_limit(flat, prefix_budget_bytes)}{suffix}"
    return result


def _paged_filename(filename: str, page_num: int, key: str, used_filenames: set) -> str:
    """Filename for the Nth page of a catalog whose first page is
    `filename` (already collision/byte-cap-safe, from _catalog_filenames()).
    page_num 1 returns `filename` unchanged -- the common case, where a
    partition's files fit on a single page, produces exactly the same
    filenames as before this existed. page_num >= 2 appends a `__pageN`
    suffix, re-truncating (preserving byte-cap safety) if `filename` was
    already close to _MAX_CATALOG_FILENAME_BYTES.

    _catalog_filenames() only reasons about collisions among partition
    keys' own (unpaged) filenames -- it has no way to know a *different*
    key might later collide with one key's *paged* name. Concretely: key
    "a" overflowing to page 2 naively produces "a__page2.md", which
    collides outright with a distinct top-level partition key literally
    named "a__page2" (whose own unpaged filename is also "a__page2.md")
    -- the later write would silently overwrite the earlier one on disk
    while both manifest entries still pointed at the same path. Checking
    every candidate against `used_filenames` (seeded by the caller with
    every key's already-assigned base filename, and grown here as pages
    are handed out) catches that -- and any subsequent collision a page
    name might cause against another page name -- falling back to the
    same stable per-key hash suffix _catalog_filenames() uses for its own
    base-name collisions, so the result stays deterministic.
    """
    if page_num <= 1:
        candidate = filename
    else:
        assert filename.endswith(".md")
        stem = filename[:-3]
        candidate = f"{stem}__page{page_num}.md"
        if _byte_len(candidate) > _MAX_CATALOG_FILENAME_BYTES or candidate in used_filenames:
            suffix = f"__page{page_num}__{stable_path_id(key, length=6)}.md"
            prefix_budget_bytes = max(1, _MAX_CATALOG_FILENAME_BYTES - _byte_len(suffix))
            candidate = f"{_truncate_to_byte_limit(stem, prefix_budget_bytes)}{suffix}"
    used_filenames.add(candidate)
    return candidate


def _top_level_symbols(rel_path: str, symbols_by_file: dict) -> list:
    rows = symbols_by_file.get(rel_path, [])
    top = [s for s in rows if s.parent_symbol == "<module>" and s.symbol_type != "module"]
    return sorted(top, key=lambda s: s.start_line)


def _module_docstring(rel_path: str, symbols_by_file: dict) -> str:
    for s in symbols_by_file.get(rel_path, []):
        if s.symbol_type == "module":
            return s.docstring_first_line or ""
    return ""


def _filename_terms(rel_path: str) -> str:
    stem = Path(rel_path).stem
    words = [w for w in stem.replace("-", "_").split("_") if w]
    return " ".join(words)


_MAX_CONTENT_TITLE_CHARS = 120
_MARKDOWN_TITLE_SCAN_LINES = 40


def _markdown_title(root: Path, rel_path: str) -> str:
    """First real content clue for a markdown file: its first `#` heading
    (any level) if it has one, else its first non-empty line -- both
    genuinely describe the file's topic, unlike a module docstring (which
    doesn't exist for non-Python files) or filename tokens alone (which is
    all the routing catalog previously had to go on for a .md file).
    Bounded to the first few dozen lines and a short excerpt -- this is a
    routing hint, not a summary. Returns "" if the file is unreadable, has
    no content, or (rare) sits outside root.

    This is content pulled directly from the file (unlike filename
    terms), so it goes through the same redact_secrets() pass as
    excerpts/chunks before being returned -- a markdown file that happens
    to start with a secret-shaped line (e.g. a stray `token = "..."`)
    must not have that value land unredacted in a routing catalog meant
    for attachment to an LLM."""
    excerpt = _safe_excerpt(root, rel_path, 1, _MARKDOWN_TITLE_SCAN_LINES)
    if not excerpt:
        return ""
    title = ""
    first_line = ""
    for _, text in excerpt:
        stripped = text.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            title = stripped.lstrip("#").strip()
            break
        if not first_line:
            first_line = stripped
    if not title:
        title = first_line
    if not title:
        return ""
    # Redact the *full* line before truncating it, not after -- a secret-
    # shaped value whose closing quote falls beyond the truncation point
    # would otherwise have that quote cut off first, breaking
    # _SECRET_ASSIGNMENT_PATTERN's closing-quote backreference so
    # redact_secrets() never matches at all and the (truncated) secret
    # prefix leaks into the catalog unredacted.
    return redact_secrets(title)[:_MAX_CONTENT_TITLE_CHARS]


def _docstring_title(rel_path: str, symbols_by_file: dict) -> str:
    """Same redact-then-truncate content-derived title as _markdown_title(),
    but drawn from a Python file's own module docstring -- used for
    __init__.py, which (unlike an arbitrary non-Python file) has no
    filename worth extracting terms from but often has a real one-line
    docstring describing the package."""
    docstring = _module_docstring(rel_path, symbols_by_file)
    if not docstring:
        return ""
    return redact_secrets(docstring)[:_MAX_CONTENT_TITLE_CHARS]


def _render_file_entry(f, symbols_by_file: dict, role: str, role_evidence: str,
                        entrypoint_reason: Optional[str], internal_deps: list,
                        called_by: list, related_tests: list, communities: list,
                        max_symbols: int) -> str:
    lines = [f"### `{f.relative_path}`", f"- Role: `{role}` (evidence: {role_evidence})"]

    docstring = _module_docstring(f.relative_path, symbols_by_file)
    terms = _filename_terms(f.relative_path)
    lines.append("- Purpose clues:")
    if docstring:
        # Content-derived, unlike filename terms below -- a docstring
        # beginning with a secret-shaped line (e.g. a stray `token = "..."`
        # left at module scope) must go through the same redaction as
        # excerpts/chunks before landing in a routing catalog meant for
        # attachment to an LLM.
        lines.append(f"  - module docstring: {redact_secrets(docstring)}")
    lines.append(f"  - filename/path terms: {terms}")

    top_syms = _top_level_symbols(f.relative_path, symbols_by_file)
    lines.append(f"- Important symbols ({len(top_syms)} total):")
    if top_syms:
        for s in top_syms[:max_symbols]:
            lines.append(f"  - `{s.qualified_name}` ({s.symbol_type}) — line {s.start_line}")
        if len(top_syms) > max_symbols:
            lines.append(f"  - ... and {len(top_syms) - max_symbols} more (see python_symbols.csv)")
    else:
        lines.append("  - (none)")

    lines.append(f"- Entrypoint evidence: {entrypoint_reason or 'none'}")

    lines.append("- Internal dependencies (resolved imports within this repository):")
    if internal_deps:
        for dep in internal_deps[:15]:
            lines.append(f"  - imports `{dep}`")
        if len(internal_deps) > 15:
            lines.append(f"  - ... and {len(internal_deps) - 15} more (see python_imports.csv)")
    else:
        lines.append("  - (none resolved; see python_imports.csv for unresolved/external imports)")

    lines.append("- Called by (high/medium-confidence static callers):")
    if called_by:
        for c in called_by[:15]:
            lines.append(f"  - `{c}`")
        if len(called_by) > 15:
            lines.append(f"  - ... and {len(called_by) - 15} more (see python_calls.csv)")
    else:
        lines.append("  - (none resolved statically; see python_calls.csv)")

    lines.append("- Related tests:")
    if related_tests:
        for t in related_tests[:10]:
            lines.append(f"  - `{t}`")
    else:
        lines.append("  - (none found via resolved imports/calls)")

    if communities:
        lines.append(f"- Graph community: {rc_graphify.format_communities(communities)}")

    lines.append(f"- Retrieval identity: sha256=`{f.sha256[:16]}…`, chunked={'yes' if f.chunked else 'no'} "
                 f"(see chunk_manifest.csv / file_inventory.csv for `{f.relative_path}`)")
    return "\n".join(lines) + "\n"


def _catalog_page_header(key: str, page_num: int, cat_hash: str, started_at_utc: str, files_covered) -> str:
    page_label = key if page_num <= 1 else f"{key} (page {page_num})"
    return (
        f"# Routing catalog: `{page_label}`\n\n"
        f"- Generated (UTC): {started_at_utc}\n"
        f"- Tool version: {TOOL_VERSION}\n"
        f"- Files covered (this page): {files_covered}\n"
        f"- Catalog source hash (sha256 of sorted `path:sha256` pairs for the full `{key}` partition): `{cat_hash}`\n"
        f"- If this hash differs from a previous copy of this file, the underlying source changed "
        f"and this catalog should be regenerated via `scan`.\n\n"
    )


def _render_catalog_page(files: list, key: str, page_num: int, cat_hash: str, started_at_utc: str, root: Path,
                          symbols_by_file: dict, roles: dict, role_evidence: dict,
                          entrypoint_reason_by_path: dict, internal_deps_by_file: dict, called_by_file: dict,
                          tests_by_target: dict, communities_by_file: dict, routing_opts: "RoutingOptions") -> tuple:
    """Renders as many of `files` (processed strictly in the given order)
    as fit within routing_opts.max_catalog_chars for one catalog page.

    Returns (page_text, next_index, rendered_files, hard_omitted_paths):
    - `next_index` is how many of `files` -- a prefix, by *position* --
      this page examined, whether a given file was rendered or hard-
      omitted. The caller advances to the next page starting at
      `files[next_index:]`. This is deliberately tracked separately from
      how many files were *rendered*: a hard omission doesn't stop the
      walk (a later, smaller file may still fit on this same page), so a
      page can hard-omit file 0, render file 1, and hit its cap at file 2
      -- `next_index` is then 2 (both 0 and 1 were examined), not 1 (the
      render count). Conflating the two previously let the caller slice
      `files[:consumed]` as "the rendered files" when position 0 (the
      omitted one, not the rendered one) landed there instead -- silently
      crediting an omitted file as covered while reprocessing the
      actually-rendered file again on the next page.
    - `rendered_files` is the actual FileRecord list placed on this page
      (full block, minimal stub, or table row) -- what the caller's
      manifest entry (roles/symbol_count/sample_paths) should reflect.
    - `hard_omitted_paths` names files that could not be represented at
      all -- not even a minimal path+role stub fits alongside this page's
      own header, which only happens when max_catalog_chars is smaller
      than that fixed framing. Since every page has the identical budget,
      such a file could never fit on *any* page, paged or not; it is
      genuinely omitted (tracked here and in the manifest), not deferred.
      Whenever `rendered_files` ends up empty, every file in `files` hit
      this case and `next_index` equals `len(files)` -- the walk never
      breaks early without having rendered at least one file (see the
      `if rendered_files:` guards below) -- so the caller's `while
      remaining:` loop terminates on its own without a special "nothing
      rendered" case.

    The exact character-budget used to decide what fits is computed
    against a worst-case (6-digit) placeholder for "Files covered" in the
    header, since the true count isn't known until this walk finishes --
    the real header (built afterward, once the render count is known) is
    therefore always <= the size already budgeted for, never larger.
    """
    header_placeholder = _catalog_page_header(key, page_num, cat_hash, started_at_utc, "999999")
    body_len = len(header_placeholder)

    blocks = []
    other_rows = []
    table_header_added = False
    hard_omitted_paths = []
    rendered_files = []
    table_header = "\n## Other files (non-Python / boilerplate)\n\n| Path | Title/summary | Role |\n|---|---|---|\n"
    next_index = len(files)
    for idx, f in enumerate(files):
        rel = f.relative_path
        if f.extension != ".py" or f.filename == "__init__.py":
            if f.extension == ".md":
                title = _markdown_title(root, rel)
            elif f.filename == "__init__.py":
                title = _docstring_title(rel, symbols_by_file)
            else:
                title = ""
            if not title:
                title = _filename_terms(rel) or "(no title)"
            title = title.replace("|", "\\|").replace("\n", " ")
            row = f"| `{rel}` | {title} | `{roles[rel]}` |\n"
            extra_header_cost = 0 if table_header_added else len(table_header)
            if body_len + extra_header_cost + len(row) > routing_opts.max_catalog_chars:
                if rendered_files:
                    next_index = idx
                    break  # this file (and everything after it) starts the next page
                # First entry attempted on a fresh page and even a single
                # lightweight row doesn't fit -- this page's own header
                # framing alone already consumes the whole budget. No
                # page (paged or not) could ever fit this file; skip it
                # and keep trying the rest of `files`, which may be
                # smaller (or may all be equally unfittable, in which
                # case rendered_files stays empty and next_index reaches
                # len(files) naturally).
                hard_omitted_paths.append(rel)
                continue
            if not table_header_added:
                other_rows.append(table_header)
                body_len += len(table_header)
                table_header_added = True
            other_rows.append(row)
            body_len += len(row)
            rendered_files.append(f)
            continue

        block = _render_file_entry(
            f, symbols_by_file, roles[rel], role_evidence[rel],
            entrypoint_reason_by_path.get(rel), sorted(internal_deps_by_file.get(rel, ())),
            sorted(called_by_file.get(rel, ())), sorted(tests_by_target.get(rel, ())),
            communities_by_file.get(rel, []), routing_opts.max_symbols_per_file_entry,
        )
        # Blocks are joined with "\n".join() below, which inserts one
        # extra separator character before every block after the first --
        # account for that here too, or body_len silently undercounts the
        # real assembled length by (len(blocks) - 1) characters.
        join_cost = 1 if blocks else 0
        if body_len + join_cost + len(block) > routing_opts.max_catalog_chars:
            if rendered_files:
                next_index = idx
                break
            # The very first entry attempted on a fresh page already
            # exceeds the limit on its own -- fall back to a minimal stub
            # (path + role only) so the page still names this file
            # directly instead of skipping straight to the next page.
            minimal = f"### `{rel}`\n- Role: `{roles[rel]}` (evidence: {role_evidence[rel]})\n"
            if body_len + len(minimal) > routing_opts.max_catalog_chars:
                hard_omitted_paths.append(rel)
                continue
            blocks.append(minimal)
            body_len += len(minimal)
            rendered_files.append(f)
            continue
        blocks.append(block)
        body_len += join_cost + len(block)
        rendered_files.append(f)

    header = _catalog_page_header(key, page_num, cat_hash, started_at_utc, len(rendered_files))
    text = header + "\n".join(blocks)
    if other_rows:
        text += "".join(other_rows)
    # The header above is fixed, mandatory framing (revision/hash
    # provenance) that always renders in full regardless of the
    # configured cap -- but the appendix below is optional annotation and
    # must respect whatever budget is actually left. Split into a short,
    # near-mandatory notice (just the count and where to look) and an
    # optional, further-bounded detailed sample list -- a full
    # explanatory paragraph for *every* page with a hard omission would
    # itself often not fit within a realistic (not absurdly tiny) cap,
    # silently losing even the fact that omission happened.
    if hard_omitted_paths:
        remaining = routing_opts.max_catalog_chars - len(text)
        short_notice = (
            f"\n## Omitted from this catalog (size limit reached)\n\n"
            f"{len(hard_omitted_paths)} file(s) omitted; see `file_inventory.csv` / "
            f"`routing/routing_manifest.json` for the complete list.\n"
        )
        if len(short_notice) <= remaining:
            text += short_notice
            remaining -= len(short_notice)
            # Bounded regardless of how many files were omitted -- listing
            # every single one here (a partition can have hundreds) would
            # itself defeat --routing-max-catalog-chars. A capped sample
            # is enough to show the shape; the full list already lives in
            # file_inventory.csv (filtered to this directory) and the
            # per-catalog manifest entry.
            omitted_sample_cap = 30
            sample = ""
            shown = 0
            for p in hard_omitted_paths[:omitted_sample_cap]:
                line = f"- `{p}`\n"
                if len(line) > remaining:
                    break
                sample += line
                remaining -= len(line)
                shown += 1
            not_shown = len(hard_omitted_paths) - shown
            if not_shown > 0 and shown > 0:
                trailer = f"- ... and {not_shown} more (not listed here)\n"
                if len(trailer) <= remaining:
                    sample += trailer
            if sample:
                text += f"\n{sample}"
        # else: not even the short notice fits -- omit the appendix
        # entirely rather than force it in over budget. The complete
        # omitted-path list is still in routing_manifest.json's per-page
        # omitted_paths.

    return text, next_index, rendered_files, hard_omitted_paths


def generate_routing(root: Path, output_dir: Path, result, routing_opts: RoutingOptions,
                      started_at_utc: str) -> Optional[dict]:
    """Generate routing/index.md, routing/<key>.md catalogs, and
    routing/routing_manifest.json. Returns the manifest dict, or None if
    routing generation is disabled."""
    routing_dir = output_dir / "routing"
    if not routing_opts.enabled:
        # A stale routing/ from an earlier (routing-enabled) scan of this
        # same output directory must not linger once routing is disabled --
        # its catalogs/hashes would silently describe an out-of-date
        # source tree instead of reflecting "routing not generated".
        if routing_dir.exists():
            import shutil
            shutil.rmtree(routing_dir)
        return None


    routing_dir.mkdir(parents=True, exist_ok=True)
    # Remove stale catalog files from a prior run before writing the
    # current set, so a renamed/removed partition doesn't leave orphaned
    # content behind.
    for entry in routing_dir.iterdir():
        if entry.is_file():
            try:
                entry.unlink()
            except OSError:
                pass

    included = [f for f in result.files if f.included]
    files_by_path = {f.relative_path: f for f in result.files}

    symbols_by_file: dict = defaultdict(list)
    for s in result.symbols:
        symbols_by_file[s.relative_path].append(s)

    entrypoint_reason_by_path = dict(result.entrypoints)

    def has_main_guard(rel_path: str) -> bool:
        reason = entrypoint_reason_by_path.get(rel_path, "")
        return reason.startswith("contains `if __name__")

    internal_deps_by_file: dict = defaultdict(set)
    for imp in result.imports:
        if imp.resolved_file:
            internal_deps_by_file[imp.source_file].add(imp.resolved_file)

    called_by_file: dict = defaultdict(set)
    for c in result.calls:
        if c.candidate_file and c.confidence in ("high", "medium"):
            called_by_file[c.candidate_file].add(f"{c.caller_symbol} ({c.caller_file}:{c.line})")

    tests_by_target: dict = defaultdict(set)
    for imp in result.imports:
        if imp.resolved_file:
            src = files_by_path.get(imp.source_file)
            if src and src.category == "test":
                tests_by_target[imp.resolved_file].add(imp.source_file)
    for c in result.calls:
        if c.candidate_file:
            src = files_by_path.get(c.caller_file)
            if src and src.category == "test":
                tests_by_target[c.candidate_file].add(c.caller_file)

    git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
    communities_by_file, graphify_warnings = rc_graphify.load_graphify_communities(
        root, git_info.get("commit") if git_info.get("available") else None,
        allow_stale=routing_opts.allow_stale_graphify,
        current_dirty=git_info.get("dirty") if git_info.get("available") else None,
    )

    roles: dict = {}
    role_evidence: dict = {}
    for f in included:
        docstring = _module_docstring(f.relative_path, symbols_by_file)
        role, evidence = rc_classify.classify_operational_role(
            f.relative_path, f.filename, f.category, f.extension,
            has_main_guard(f.relative_path), docstring, f.generated_or_vendor,
        )
        roles[f.relative_path] = role
        role_evidence[f.relative_path] = evidence

    partitions = _partition_files([(f, roles[f.relative_path]) for f in included],
                                   routing_opts.max_files_per_catalog)

    source_manifest_hash = sha256_text(
        "\n".join(f"{f.relative_path}:{f.sha256}" for f in sorted(included, key=lambda f: _sort_key(f.relative_path)))
    )

    filenames_by_key = _catalog_filenames(list(partitions.keys()))
    # Every key's own base filename is "reserved" up front -- _paged_filename()
    # checks (and grows) this set so a key's overflow page can never
    # collide with another key's own filename (or another key's overflow
    # page); see _paged_filename()'s docstring for the concrete collision
    # shape this prevents.
    used_filenames = set(filenames_by_key.values())

    catalog_entries = []  # for index.md, in key-sorted order
    for key in sorted(partitions):
        cat_files = sorted(partitions[key], key=lambda f: _sort_key(f.relative_path))
        filename = filenames_by_key[key]
        cat_hash = sha256_text(
            "\n".join(f"{f.relative_path}:{f.sha256}" for f in cat_files)
        )

        # A partition that doesn't fit in one page (an oversized, flat
        # directory _partition_files() had no subdirectory left to split
        # by) spills into additional pages -- <filename>, then
        # <filename>__page2.md, __page3.md, and so on -- rather than
        # silently dropping every file past the first page's budget. Each
        # page is rendered strictly in order (cat_files sorted by path);
        # the loop stops once every file has been *examined* (rendered or
        # hard-omitted) by some page.
        remaining = cat_files
        page_num = 1
        while remaining:
            page_filename = _paged_filename(filename, page_num, key, used_filenames)
            text, next_index, rendered_files, hard_omitted_paths = _render_catalog_page(
                remaining, key, page_num, cat_hash, started_at_utc, root, symbols_by_file, roles,
                role_evidence, entrypoint_reason_by_path, internal_deps_by_file, called_by_file,
                tests_by_target, communities_by_file, routing_opts,
            )
            atomic_write_text(routing_dir / page_filename, text + "\n")

            by_role = Counter(roles[f.relative_path] for f in rendered_files)
            symbol_count = sum(len(_top_level_symbols(f.relative_path, symbols_by_file)) for f in rendered_files)
            catalog_entries.append({
                "key": key if page_num == 1 else f"{key} (page {page_num})",
                "path": f"routing/{page_filename}",
                "file_count": len(rendered_files),
                "symbol_count": symbol_count,
                "roles": dict(sorted(by_role.items())),
                "omitted_file_count": len(hard_omitted_paths),
                "source_hash": cat_hash,
                "sample_paths": [f.relative_path for f in rendered_files[:3]],
                # The page's own appendix only samples the first 30
                # omitted paths (keeping that file bounded); the manifest
                # is where a machine consumer can get the complete list.
                "omitted_paths": hard_omitted_paths,
            })

            # next_index == len(remaining) whenever rendered_files ended up
            # empty (every file on this page hard-omitted) -- see
            # _render_catalog_page()'s docstring -- so this always makes
            # forward progress and terminates without a separate
            # "nothing rendered" check.
            remaining = remaining[next_index:]
            page_num += 1

    index_text = _render_index(
        root, started_at_utc, git_info, source_manifest_hash, catalog_entries,
        routing_opts, graphify_warnings,
    )
    atomic_write_text(routing_dir / "index.md", index_text)

    manifest = {
        "tool_version": TOOL_VERSION,
        "generated_at_utc": started_at_utc,
        "git": git_info,
        "source_manifest_hash": source_manifest_hash,
        "options": {
            "max_files_per_catalog": routing_opts.max_files_per_catalog,
            "max_catalog_chars": routing_opts.max_catalog_chars,
            "max_index_chars": routing_opts.max_index_chars,
            "max_symbols_per_file_entry": routing_opts.max_symbols_per_file_entry,
            "allow_stale_graphify": routing_opts.allow_stale_graphify,
        },
        "graphify_warnings": graphify_warnings,
        "catalogs": catalog_entries,
    }
    atomic_write_text(routing_dir / "routing_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def _render_index(root: Path, started_at_utc: str, git_info: dict, source_manifest_hash: str,
                   catalog_entries: list, routing_opts: RoutingOptions, graphify_warnings: list) -> str:
    lines = [
        f"# Routing index: {root.resolve().name}\n",
        f"- Generated (UTC): {started_at_utc}",
        f"- Tool version: {TOOL_VERSION}",
    ]
    if git_info.get("available"):
        dirty = git_info.get("dirty")
        dirty_str = "dirty" if dirty else ("clean" if dirty is False else "unknown")
        lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty_str} worktree)")
    else:
        lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
    lines.append(f"- Source manifest hash (sha256 over every included file's `path:sha256`): `{source_manifest_hash}`")
    lines.append(
        "- Freshness rule: if this hash no longer matches a fresh `scan`, re-run `scan` before trusting "
        "any catalog below or building a `packet_request.json` from it."
    )
    for w in graphify_warnings:
        lines.append(f"- Graphify: {w}")
    lines.append("")

    lines.append("## How to use this index\n")
    lines.append(
        "**This index has counts, not names -- it is not enough on its own to pick good selectors.** "
        "Do not draft `packet_request.json` from this file alone. Follow these steps in order:\n\n"
        "1. Skim the catalog summaries below and pick the one(s) most likely to cover your question.\n"
        "2. **Ask for that catalog file (`routing/<name>.md`) and read it before writing a request.** "
        "It has the actual file paths, symbol names, and line numbers this index does not. If you have "
        "not been given a catalog yet, your next reply should ask for it -- not a `packet_request.json`.\n"
        "3. From the catalog, prefer `selectors.files` and/or `selectors.symbols` (an exact symbol name, "
        "optionally narrowed with `file`) that name the actual code involved. Use `selectors.search_terms` "
        "only for a short, distinctive phrase or identifier you expect to appear in very few places -- a "
        "single common word (e.g. \"blocked\", \"Model\", \"error\") can match hundreds of files across the "
        "whole repository and will get crowded out of the packet by `limits.max_files` before it reaches "
        "anything relevant. Treat search terms as a supplement to file/symbol selectors, never a substitute.\n"
        "4. Write a `packet_request.json` (schema: `schema/packet_request.schema.json`) naming those "
        "selectors, e.g.:\n"
    )
    lines.append(
        "```json\n"
        "{\n"
        '  "schema_version": "1.0",\n'
        '  "question": "<your question>",\n'
        '  "selectors": {"files": ["path/to/file.py"], "symbols": [{"name": "some_function"}], '
        '"search_terms": [], "lines": []},\n'
        '  "expansion": {"include_callers": true, "include_callees": true, "include_imports": true, '
        '"include_related_tests": true, "max_hops": 1},\n'
        '  "limits": {"max_estimated_tokens": 12000, "max_files": 12}\n'
        "}\n"
        "```\n"
        "5. Run:\n"
        "   `python repo_context.py packet ROOT --output OUT --request packet_request.json`\n"
        "6. Read the generated packet in `packets/`. If any selector came back `missing`/`ambiguous` in "
        "its resolution report, or search terms were reported omitted under `limits.max_files`, revise "
        "the request (narrow the term, add a `file` qualifier, raise the limit) and re-run -- do not "
        "answer from a packet that reports selectors it couldn't resolve.\n"
    )

    lines.append(f"## Catalogs ({len(catalog_entries)})\n")

    def entry_lines(e: dict, include_summary: bool) -> list:
        out = [f"### `{e['path']}`", f"- Covers: {e['file_count']} file(s), {e['symbol_count']} top-level symbol(s)"]
        if include_summary:
            role_summary = ", ".join(f"{k}: {v}" for k, v in e["roles"].items())
            out.append(f"- Roles present: {role_summary}")
            out.append(f"- Sample paths: {', '.join('`' + p + '`' for p in e['sample_paths'])}")
        if e["omitted_file_count"]:
            out.append(f"- Note: {e['omitted_file_count']} file(s) omitted from this catalog by its own size limit")
        return out

    body_lines = []
    for e in catalog_entries:
        body_lines.extend(entry_lines(e, include_summary=True))
        body_lines.append("")

    header_text = "\n".join(lines) + "\n"
    body_text = "\n".join(body_lines)
    if len(header_text) + len(body_text) > routing_opts.max_index_chars:
        # First reduction: drop the richer per-catalog summary lines.
        body_lines = []
        for e in catalog_entries:
            body_lines.extend(entry_lines(e, include_summary=False))
            body_lines.append("")
        body_text = "\n".join(body_lines)

    if len(header_text) + len(body_text) > routing_opts.max_index_chars:
        # Every catalog's bare entry (path + coverage counts) is always
        # listed here, even if that means running over
        # --routing-index-max-chars -- an index that silently drops a
        # catalog's *existence* is a worse failure than one that's a bit
        # oversized. A dropped catalog has no path/name anywhere for an
        # LLM to ask for; an oversized index is merely a bit more to read.
        #
        # NOTE for a future revisit: if this note starts firing routinely
        # (i.e. --routing-max-files-per-catalog partitions a repo into
        # more catalogs than fit even at this bare-entry density), the
        # next lever is *ranking*, not truncation -- sort catalog_entries
        # by an importance signal (e.g. file_count, or role-density like
        # how many operator_entrypoint/active_pipeline files it holds)
        # before deciding which catalogs get the richer include_summary=True
        # treatment above, instead of the current uniform all-or-nothing
        # (every catalog gets full summary, or every catalog gets bare).
        # That would make the *richer* detail biased toward the catalogs
        # most likely to matter, without ever reintroducing outright
        # omission of a catalog's name. Not implemented -- current corpus
        # doesn't need it and it adds a policy question (what counts as
        # "important") that's better decided against a real repeat case.
        body_text += (
            f"\n_Note: this index ({len(header_text) + len(body_text)} chars) exceeds "
            f"--routing-index-max-chars={routing_opts.max_index_chars} because every catalog listed above is "
            f"always included, even when doing so runs over the configured limit -- no catalog is ever omitted "
            f"from this list. See `routing/routing_manifest.json` for the same data in a more compact, "
            f"machine-readable form._\n"
        )

    return header_text + body_text

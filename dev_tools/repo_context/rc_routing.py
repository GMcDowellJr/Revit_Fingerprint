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
from rc_common import TOOL_VERSION, atomic_write_text, get_git_info, sha256_text, stable_path_id

DEFAULT_MAX_FILES_PER_CATALOG = 60
DEFAULT_MAX_CATALOG_CHARS = 24_000
DEFAULT_MAX_INDEX_CHARS = 6_000
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


def _catalog_filenames(keys: list) -> dict:
    """Injective key -> filename mapping. A naive "/" -> "_" substitution
    can collide (partition key "a/b" and a separate top-level partition
    "a_b" would both naively become "a_b.md", silently overwriting one),
    so any keys that collide under that substitution get a short stable
    hash suffix appended -- deterministic, and only paid for the
    (uncommon) colliding keys, so the common case stays readable."""
    naive = {key: key.replace("/", "_") + ".md" for key in keys}
    by_filename: dict = defaultdict(list)
    for key, filename in naive.items():
        by_filename[filename].append(key)
    result = {}
    for filename, colliding_keys in by_filename.items():
        if len(colliding_keys) == 1:
            result[colliding_keys[0]] = filename
        else:
            for key in colliding_keys:
                result[key] = f"{key.replace('/', '_')}__{stable_path_id(key, length=6)}.md"
    return result


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


def _render_file_entry(f, symbols_by_file: dict, role: str, role_evidence: str,
                        entrypoint_reason: Optional[str], internal_deps: list,
                        called_by: list, related_tests: list, communities: list,
                        max_symbols: int) -> str:
    lines = [f"### `{f.relative_path}`", f"- Role: `{role}` (evidence: {role_evidence})"]

    docstring = _module_docstring(f.relative_path, symbols_by_file)
    terms = _filename_terms(f.relative_path)
    lines.append("- Purpose clues:")
    if docstring:
        lines.append(f"  - module docstring: {docstring}")
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

    git_info = get_git_info(root)
    communities_by_file, graphify_warnings = rc_graphify.load_graphify_communities(
        root, git_info.get("commit") if git_info.get("available") else None,
        allow_stale=routing_opts.allow_stale_graphify,
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

    catalog_entries = []  # for index.md, in key-sorted order
    for key in sorted(partitions):
        cat_files = sorted(partitions[key], key=lambda f: _sort_key(f.relative_path))
        filename = filenames_by_key[key]
        cat_hash = sha256_text(
            "\n".join(f"{f.relative_path}:{f.sha256}" for f in cat_files)
        )

        blocks = []
        omitted_paths = []
        header = (
            f"# Routing catalog: `{key}`\n\n"
            f"- Generated (UTC): {started_at_utc}\n"
            f"- Tool version: {TOOL_VERSION}\n"
            f"- Files covered: {len(cat_files)}\n"
            f"- Catalog source hash (sha256 of sorted `path:sha256` pairs): `{cat_hash}`\n"
            f"- If this hash differs from a previous copy of this file, the underlying source changed "
            f"and this catalog should be regenerated via `scan`.\n\n"
        )
        body_len = len(header)
        for f in cat_files:
            rel = f.relative_path
            block = _render_file_entry(
                f, symbols_by_file, roles[rel], role_evidence[rel],
                entrypoint_reason_by_path.get(rel), sorted(internal_deps_by_file.get(rel, ())),
                sorted(called_by_file.get(rel, ())), sorted(tests_by_target.get(rel, ())),
                communities_by_file.get(rel, []), routing_opts.max_symbols_per_file_entry,
            )
            if body_len + len(block) > routing_opts.max_catalog_chars and blocks:
                omitted_paths.append(rel)
                continue
            blocks.append(block)
            body_len += len(block)

        text = header + "\n".join(blocks)
        if omitted_paths:
            text += (
                f"\n## Omitted from this catalog (size limit reached)\n\n"
                f"{len(omitted_paths)} file(s) in this partition are not detailed above because this "
                f"catalog reached its configured `--routing-max-catalog-chars` limit "
                f"({routing_opts.max_catalog_chars}). They are still covered by `file_inventory.csv` / "
                f"`python_symbols.csv`; request them directly by path in a `packet_request.json`:\n\n"
            )
            for p in omitted_paths:
                text += f"- `{p}`\n"
        atomic_write_text(routing_dir / filename, text + "\n")

        by_role = Counter(roles[f.relative_path] for f in cat_files)
        symbol_count = sum(len(_top_level_symbols(f.relative_path, symbols_by_file)) for f in cat_files)
        catalog_entries.append({
            "key": key,
            "path": f"routing/{filename}",
            "file_count": len(cat_files),
            "symbol_count": symbol_count,
            "roles": dict(sorted(by_role.items())),
            "omitted_file_count": len(omitted_paths),
            "source_hash": cat_hash,
            "sample_paths": [f.relative_path for f in cat_files[:3]],
        })

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
        "1. Skim the catalog summaries below and pick the one(s) most likely to cover your question.\n"
        "2. Open that catalog (`routing/<name>.md`) and identify candidate files/symbols/search terms.\n"
        "3. Write a `packet_request.json` (schema: `schema/packet_request.schema.json`) naming those "
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
        "4. Run:\n"
        "   `python repo_context.py packet ROOT --output OUT --request packet_request.json`\n"
        "5. Read the generated packet in `packets/`; it contains exact source excerpts, a "
        "selector-resolution report, and an explicit list of anything omitted or ambiguous.\n"
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
        # Second reduction: truncate the catalog list itself (stable,
        # alphabetical order), stating how many more exist.
        kept = []
        running = len(header_text)
        shown = 0
        for e in catalog_entries:
            block = "\n".join(entry_lines(e, include_summary=False)) + "\n"
            if running + len(block) > routing_opts.max_index_chars - 200:
                break
            kept.append(block)
            running += len(block)
            shown += 1
        remaining = len(catalog_entries) - shown
        body_text = "".join(kept)
        if remaining > 0:
            body_text += (
                f"\n_...and {remaining} more catalog(s) omitted from this index to stay within "
                f"--routing-index-max-chars={routing_opts.max_index_chars}. See `routing/routing_manifest.json` "
                f"for the complete list._\n"
            )

    return header_text + body_text

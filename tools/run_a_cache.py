#!/usr/bin/env python3
"""Per-export-file change-detection cache for Run A (corpus_update_runbook.ps1's
``sig_hash,flatten,apply,placeholders`` stage set).

Scope and rationale (see audit_results/audit_17_abc_reprocessing_scope_investigation.md
section 7 for the confirmed investigation this cache is built on): Run A's flatten
stage (`tools/extractor.py::_iter_export_files`) has always been a plain
`exports_dir.glob("*.json")` with zero mtime/hash/existing-output gating -- every
export file is fully re-parsed on every invocation, even when nothing changed.
This module is the "did this file actually change" oracle that `tools/extractor.py`
(flatten), `tools/run_extract_all.py` (sig_hash stage), and `tools/apply_join_policy.py`
(apply stage) all consult so that unchanged files' rows can be reused verbatim
instead of recomputed.

Cache-validity contract (deliberately coarse, not per-stage): this module treats
the *entire* cache as valid or stale as a single unit, keyed by
(cache schema version, sig_hash policy file content hash, join policy file content
hash, tool_version). If either policy file's content changes, this module's own
schema version is bumped, OR the extraction tool version changes (`tool_version`,
normally a git SHA -- see core/hashing conventions and `_get_tool_version()` in
tools/extractor.py), every file is treated as changed for this run -- there is no
partial "only sig_hash needs recompute" state. This mirrors flatten -> sig_hash ->
apply's shared dependency: sig_hash/join_hash are both pure per-record functions
of (that record's own extracted items, the relevant policy file, and the code that
computes them), so a policy OR code change invalidates every record's derived
value corpus-wide, not just some. Keying on `tool_version` (rather than relying
solely on a human remembering to bump RUN_A_CACHE_SCHEMA_VERSION by hand) means an
ordinary code change to `_flatten_one_file`/sig_hash/join-key logic automatically
invalidates stale cached derived values instead of silently reusing them.

RUN_A_CACHE_SCHEMA_VERSION is intentionally separate from
`tools/discovery_orchestrator.py`'s DISCOVERY_ENGINE_VERSION -- that constant is a
cache-semantic contract for the discovery-sweep engine (T1 candidate search) and
must not be bumped or read by this module; this file owns its own version marker
for Run A's own flatten/sig_hash/apply cache.

Per-file change detection prefers a cheap `(mtime_ns, size)` stat comparison over
unconditionally hashing file content: Revit export JSON files carry full
per-record item payloads and can run from single-digit MB to well over 100MB on
a large project, so hashing every file on every invocation (just to usually
conclude "unchanged") would burn a meaningful fraction of the very I/O cost this
cache exists to avoid. `(mtime_ns, size)` matching against the last recorded
value is treated as sufficient proof of "unchanged" (the fast path -- the common
case once a corpus is in steady state). Content hash (sha256, streamed in fixed
chunks so a single huge export file never needs to be held in memory)  remains
the authoritative signal and is stored alongside the stat pair for two reasons:
(a) it is what actually gets compared whenever `(mtime_ns, size)` do NOT match
(covering a touch-without-edit, a re-copy that preserves content but not mtime,
or any tool that doesn't preserve stat metadata) so a spurious stat change never
forces an unnecessary reparse, and (b) it is the value this module's own tests
assert against, independent of filesystem stat granularity.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

RUN_A_CACHE_SCHEMA_VERSION = 1
_HASH_CHUNK_SIZE = 1 << 20  # 1 MiB
_ABSENT_POLICY_SENTINEL = "<absent>"


def _sha256_stream(paths: List[Path], on_bytes: Optional[Callable[[int], None]] = None) -> str:
    """Content hash of one or more files, concatenated in the given order.

    Streamed in fixed-size chunks so a single large export file is never fully
    materialized in memory just to be hashed.
    """
    h = hashlib.sha256()
    for p in paths:
        with p.open("rb") as f:
            while True:
                chunk = f.read(_HASH_CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
                if on_bytes:
                    on_bytes(len(chunk))
        h.update(b"\x00")  # separator so (A,BC) != (AB,C) for split index/details pairs
    return h.hexdigest()


def hash_policy_file(path: Optional[Path]) -> str:
    """Content hash of a policy file, or a fixed sentinel when no policy is in use.

    Used as part of the global cache-validity fingerprint: any change to the
    sig_hash or join policy content must invalidate the whole cache (see module
    docstring) even if the policy file's path/mtime happens to be identical
    across runs (e.g. an in-place edit).
    """
    if path is None:
        return _ABSENT_POLICY_SENTINEL
    p = Path(path)
    if not p.is_file():
        return _ABSENT_POLICY_SENTINEL
    return _sha256_stream([p])


def cache_root(results_root: Path) -> Path:
    """Root cache directory for a Run A invocation, sibling to records/analysis/policies."""
    return Path(results_root) / ".run_a_cache"


def _manifest_path(cache_dir: Path) -> Path:
    return cache_dir / "manifest.json"


def _entry_path(cache_dir: Path, file_id: str) -> Path:
    digest = hashlib.sha1(file_id.encode("utf-8")).hexdigest()[:20]
    return cache_dir / "entries" / f"{digest}.json"


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        json.dump(payload, tmp, sort_keys=True)
    tmp_path.replace(path)


def load_manifest(cache_dir: Path) -> Optional[Dict[str, Any]]:
    p = _manifest_path(cache_dir)
    if not p.is_file():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return data


def save_manifest(cache_dir: Path, manifest: Dict[str, Any]) -> None:
    _atomic_write_json(_manifest_path(cache_dir), manifest)


def load_entry(cache_dir: Path, file_id: str) -> Optional[Dict[str, Any]]:
    return load_entry_diagnostic(cache_dir, file_id)[0]


def load_entry_diagnostic(cache_dir: Path, file_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """Load and validate one payload, returning a stable fallback reason."""
    p = _entry_path(cache_dir, file_id)
    if not p.is_file():
        return None, "missing"
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return None, "invalid_json"
    except UnicodeDecodeError:
        # The file was opened successfully but cannot be decoded as the cache's
        # required UTF-8 text format. Treat this as unreadable cache data and
        # retain the established safe-recompute fallback.
        return None, "unreadable"
    except OSError:
        return None, "unreadable"
    if not isinstance(data, dict) or data.get("file_id") != file_id:
        return None, "rejected_structure_or_identity"
    return data, "accepted"


def save_entry(cache_dir: Path, file_id: str, payload: Dict[str, Any]) -> None:
    payload = dict(payload)
    payload["file_id"] = file_id
    _atomic_write_json(_entry_path(cache_dir, file_id), payload)


def stat_signature_parts(paths: List[Path]) -> List[Dict[str, int]]:
    """Independent (mtime_ns, size) pairs, one per path, in the given order.

    Kept as separate per-path pairs -- never folded together (e.g. via XOR/sum)
    -- because a split index/details export's two files are frequently written
    with identical or near-identical timestamps by the exporting tool; folding
    them collapses real signal (two distinct mtimes can XOR to a value that
    coincidentally recurs, and summed sizes can coincidentally match across two
    genuinely different byte-for-byte contents) that a later run could rely on
    to wrongly skip the content-hash fallback and reuse stale cached rows.
    """
    return [{"mtime_ns": st.st_mtime_ns, "size": st.st_size} for st in (p.stat() for p in paths)]


class RunState:
    """Per-invocation decision of which export files can reuse cached rows.

    `unchanged_file_ids` is empty whenever the cache is globally invalid (first
    run, a policy changed, a forced-full run, or a schema version bump) -- in
    that case every file is (re)computed fresh and a brand-new cache is written,
    which is by construction byte-identical to a cold-cache run (there is no
    partial/best-effort reuse path).
    """

    def __init__(
        self,
        *,
        cache_dir: Path,
        cache_was_valid: bool,
        invalidation_reason: str,
        sig_hash_policy_hash: str,
        join_policy_hash: str,
        tool_version: str,
        domain_filter_key: str,
        reuse_candidate_file_ids: Set[str],
        fresh_signatures: Dict[str, Dict[str, Any]],
        source_files_hashed: int = 0,
        source_bytes_hashed: int = 0,
    ) -> None:
        self.cache_dir = cache_dir
        self.cache_was_valid = cache_was_valid
        self.invalidation_reason = invalidation_reason
        self.sig_hash_policy_hash = sig_hash_policy_hash
        self.join_policy_hash = join_policy_hash
        self.tool_version = tool_version
        self.domain_filter_key = domain_filter_key
        self.reuse_candidate_file_ids = reuse_candidate_file_ids
        # Compatibility alias; candidates have not yet had payload validation.
        self.unchanged_file_ids = reuse_candidate_file_ids
        self.source_files_hashed = source_files_hashed
        self.source_bytes_hashed = source_bytes_hashed
        # file_id -> {"parts": [{"mtime_ns": int, "size": int}, ...], "content_hash": str},
        # computed fresh for every file this run regardless of reuse decision, so
        # the manifest written at the end of a successful run is always accurate.
        self.fresh_signatures = fresh_signatures


def domain_filter_key(domains: Optional[Any]) -> str:
    """Stable string key for a sig_hash stage domain filter (None/empty = unfiltered).

    Mirrors `_apply_sig_hash_to_phase0`'s own `dom_filter = set(domains or [])`
    semantics -- an empty/None filter and a filter naming every domain are two
    different things there (empty means "no filtering", not "filter out
    everything"), so this key must distinguish "no filter" from any concrete
    filter set, including one that happens to be empty-after-narrowing.
    """
    if not domains:
        return "<none>"
    return "|".join(sorted(str(d) for d in domains))


def compute_run_state(
    *,
    cache_dir: Path,
    file_id_to_paths: Dict[str, List[Path]],
    sig_hash_policy_path: Optional[Path],
    join_policy_path: Optional[Path],
    tool_version: str,
    sig_hash_domains: Optional[Any] = None,
    force_full: bool,
    progress: Optional[Any] = None,
) -> RunState:
    """Decide, for every export file, whether its cached rows may be reused.

    `file_id_to_paths` maps file_id -> [primary_path] or [primary_path, secondary_path]
    (the split index/details pair), in whatever order flatten's own file
    discovery produced them -- content hashing concatenates in that order so a
    hash is comparable run over run only if the pairing itself is stable, which
    `_iter_export_files` already guarantees deterministically by filename.

    `tool_version` should be the same value the caller stamps into
    file_metadata.csv's own tool_version column (`extractor._get_tool_version()`,
    normally a git SHA) -- see the module docstring on why it participates in
    cache validity.

    `sig_hash_domains` should be the same domain filter the caller passes to the
    sig_hash stage (`active_domains or domains` in run_extract_all.py's
    non-incremental branch) -- it changes which domains' records get a
    policy-driven sig_hash at all, so it must invalidate the cache the same way
    a policy change does, or a narrower/wider `--domains` filter between runs
    could reuse entries computed under a different filter.
    """
    sig_hash_policy_hash = hash_policy_file(sig_hash_policy_path)
    join_policy_hash = hash_policy_file(join_policy_path)
    dom_filter_key = domain_filter_key(sig_hash_domains)

    manifest = None if force_full else load_manifest(cache_dir)
    cache_was_valid = True
    invalidation_reason = ""
    if force_full:
        cache_was_valid = False
        invalidation_reason = "force_full_requested"
    elif manifest is None:
        cache_was_valid = False
        invalidation_reason = "no_prior_manifest"
    elif manifest.get("schema_version") != RUN_A_CACHE_SCHEMA_VERSION:
        cache_was_valid = False
        invalidation_reason = "cache_schema_version_changed"
    elif manifest.get("sig_hash_policy_hash") != sig_hash_policy_hash:
        cache_was_valid = False
        invalidation_reason = "sig_hash_policy_changed"
    elif manifest.get("join_policy_hash") != join_policy_hash:
        cache_was_valid = False
        invalidation_reason = "join_policy_changed"
    elif manifest.get("tool_version") != tool_version:
        cache_was_valid = False
        invalidation_reason = "tool_version_changed"
    elif manifest.get("domain_filter_key") != dom_filter_key:
        cache_was_valid = False
        invalidation_reason = "domain_filter_changed"

    prior_files: Dict[str, Any] = {}
    if cache_was_valid and isinstance(manifest, dict) and isinstance(manifest.get("files"), dict):
        prior_files = manifest["files"]

    reuse_candidate_file_ids: Set[str] = set()
    fresh_signatures: Dict[str, Dict[str, Any]] = {}
    source_files_hashed = 0
    source_bytes_hashed = 0

    def counted_hash(paths: List[Path]) -> str:
        nonlocal source_files_hashed, source_bytes_hashed
        source_files_hashed += len(paths)
        return _sha256_stream(paths, lambda n: _add_bytes(n))

    def _add_bytes(n: int) -> None:
        nonlocal source_bytes_hashed
        source_bytes_hashed += n

    total = len(file_id_to_paths)
    for checked, (file_id, paths) in enumerate(file_id_to_paths.items(), 1):
        if progress:
            progress.update(phase="source_signature_scan", current=file_id, checked=checked - 1, total=total,
                            files_hashed=source_files_hashed, bytes_hashed=source_bytes_hashed)
        parts = stat_signature_parts(paths)

        prior = prior_files.get(file_id) if cache_was_valid else None
        content_hash: Optional[str] = None
        is_unchanged = False
        if prior is not None and prior.get("parts") == parts:
            # Fast path: every path's own (mtime_ns, size) pair matches, trust
            # it without reading file content.
            content_hash = prior.get("content_hash")
            is_unchanged = bool(content_hash)
        elif prior is not None:
            # Stat parts differ (or are absent) -- fall back to content hash so a
            # touch-without-edit or a re-copy that changed mtime but not bytes
            # still reuses cache instead of forcing an unnecessary reparse.
            content_hash = counted_hash(paths)
            is_unchanged = content_hash == prior.get("content_hash")
        # else: no prior entry for this file_id at all -> genuinely new, leave
        # content_hash unset for now; computed lazily below only if needed.

        if content_hash is None:
            content_hash = counted_hash(paths)

        fresh_signatures[file_id] = {"parts": parts, "content_hash": content_hash}
        if is_unchanged:
            reuse_candidate_file_ids.add(file_id)

    if progress:
        progress.event("source signature scan complete", phase="source_signature_scan", checked=total, total=total,
                       files_hashed=source_files_hashed, bytes_hashed=source_bytes_hashed)

    return RunState(
        cache_dir=cache_dir,
        cache_was_valid=cache_was_valid,
        invalidation_reason=invalidation_reason,
        sig_hash_policy_hash=sig_hash_policy_hash,
        join_policy_hash=join_policy_hash,
        tool_version=tool_version,
        domain_filter_key=dom_filter_key,
        reuse_candidate_file_ids=reuse_candidate_file_ids,
        fresh_signatures=fresh_signatures,
        source_files_hashed=source_files_hashed,
        source_bytes_hashed=source_bytes_hashed,
    )


def finalize_manifest(state: RunState) -> None:
    """Persist the updated manifest after a fully successful Run A invocation.

    Callers must only call this once flatten, sig_hash, and apply have all
    completed without error -- writing it earlier could record a file as
    "cached" while its sig_hash/apply cache entry is missing or inconsistent
    (e.g. a mid-run crash), corrupting reuse decisions on the next run.
    """
    manifest = {
        "schema_version": RUN_A_CACHE_SCHEMA_VERSION,
        "sig_hash_policy_hash": state.sig_hash_policy_hash,
        "join_policy_hash": state.join_policy_hash,
        "tool_version": state.tool_version,
        "domain_filter_key": state.domain_filter_key,
        "files": state.fresh_signatures,
        "updated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    save_manifest(state.cache_dir, manifest)

# -*- coding: utf-8 -*-
"""
Pure-Python reconstruction/validation/naming logic for the line_patterns
Revit mapping utility (see mapping/line_pattern_revit_apply.py for the
Revit-API-touching half, and mapping/create_line_pattern_mappings.py for the
Dynamo entry point).

No Revit dependency -- safe to unit test with plain pytest.

This module consumes the three CSVs produced by
tools/export_bundle_pattern_detail.py:
    bundle_pattern_inventory.csv
    pattern_settings.csv
    pattern_names.csv

and reconstructs an ordered line-pattern segment definition per requested
(domain="line_patterns", join_hash), validating evidence completeness BEFORE
any Revit mutation is attempted. Nothing here writes to a Revit document.

Reuse, not duplication
-----------------------
This module deliberately reuses the repository's authoritative
canonicalization/hashing/join-key primitives rather than re-deriving them:

  - core.hashing.make_hash / safe_str                      (hash primitive)
  - core.record_v2.canonicalize_float / ITEM_Q_OK / STATUS_* / make_identity_item
  - core.join_key_policy / core.join_key_builder            (join_hash computation,
    against policies/domain_join_key_policies.json's "line_patterns" entry --
    join_key_schema "line_patterns.join_key.v3", D-017)
  - domains.line_patterns._LP_SEG_TYPE_NAME                 (the "canonical,
    locked" kind<->name mapping domains/line_patterns.py itself documents:
    0=Dash, 1=Space, 2=Dot)

Do NOT substitute sig_hash for join_hash: domains/line_patterns.py's own
sig_hash is computed from line_pattern.segments_def_hash (exact scale
identity, see rec_v2["sig_basis"]), while the join_key_schema
"line_patterns.join_key.v3" policy computes join_hash from
line_pattern.segments_norm_hash (scale-invariant governance identity, D-017).
These are different questions with different hash values for the same
pattern; this module verifies against join_hash because that is what
bundle_pattern_inventory.csv/domain_patterns.csv clustering keys on
(tools/extractor.py's cluster_id = f"{domain}|{schema}|{join_hash}", sourced
from phase0_records.csv's join_hash column, itself written by
tools/apply_join_policy.py from build_join_key_from_policy()'s result -- see
tests/test_line_patterns_canonical_selectors.py for the same reuse pattern).

segments_norm_hash mirroring
-----------------------------
line_pattern.segments_norm_hash is computed synthetically by
tools/run_extract_all.py's _append_line_pattern_synthetic_norm_hash() during
the flatten stage (T0.5) -- it is not emitted by domains/line_patterns.py
itself and is not exposed as an importable, non-private function.
compute_segments_norm_hash() below is a deliberately independent
reimplementation of that same per-record algorithm, following the same
precedent as tools/pattern_id_utils.py mirroring tools/extractor.py's private
_stable_pattern_id() (see that module's docstring and CLAUDE.md's note on
it): keeping this Revit-writing utility from coupling to
tools/run_extract_all.py's CLI/orchestration machinery, while still reusing
the shared hashing primitive (core.hashing.make_hash) so only the
domain-specific token-construction logic is mirrored, not the hash itself.
tests/test_line_pattern_mapping_reconstruction.py cross-checks the two
implementations agree over a battery of synthetic segment lists -- closing
the same kind of gap CLAUDE.md flags as an open TODO for pattern_id_utils.py.
"""

from __future__ import annotations

import csv
import math
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.hashing import make_hash, safe_str
from core.record_v2 import (
    ITEM_Q_OK,
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    canonicalize_float,
    make_identity_item,
)
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from domains.line_patterns import _LP_SEG_TYPE_NAME

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DOMAIN_NAME = "line_patterns"

SEGMENT_COUNT_KEY = "line_pattern.segment_count"
SEGMENTS_DEF_HASH_KEY = "line_pattern.segments_def_hash"
SEGMENTS_NORM_HASH_KEY = "line_pattern.segments_norm_hash"
NO_ITEMS_MARKER_KEY = "__no_items__"

# Accepts both the current "seg[NNN]" key spelling and the legacy
# "segment[NNN]" spelling tools/run_extract_all.py also tolerates.
_SEGMENT_ITEM_KEY_RE = re.compile(r"^line_pattern\.(?:seg|segment)\[(\d+)\]\.(kind|length)$")

DOT_KIND = 2

MAPPING_NAME_PREFIX = "MAP__"
SHORT_JOIN_HASH_LEN = 12

# Characters Revit rejects in element/type names.
_INVALID_NAME_CHARS = set("\\:{}[]|;<>?'~\"")
_MAX_REVIT_NAME_LEN = 200

_POLICY_PATH = os.path.join(_REPO_ROOT, "policies", "domain_join_key_policies.json")
_STATUS_ORDER = {STATUS_OK: 0, STATUS_DEGRADED: 1, STATUS_BLOCKED: 2}

_policy_cache: Optional[Dict[str, Any]] = None


def dominant_status(statuses) -> str:
    """blocked > degraded > ok, mirroring record.v2's dominance-ordering convention."""
    worst = STATUS_OK
    for s in statuses:
        if _STATUS_ORDER.get(s, _STATUS_ORDER[STATUS_BLOCKED]) > _STATUS_ORDER[worst]:
            worst = s
    return worst


def get_line_patterns_join_key_policy() -> Optional[Dict[str, Any]]:
    """Load (and cache) the authoritative line_patterns join-key policy."""
    global _policy_cache
    if _policy_cache is None:
        policies = load_join_key_policies(_POLICY_PATH)
        _policy_cache = get_domain_join_key_policy(policies, DOMAIN_NAME)
    return _policy_cache


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

_REQUIRED_EXPORT_FILES = (
    "bundle_pattern_inventory.csv",
    "pattern_settings.csv",
    "pattern_names.csv",
)


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def load_bundle_pattern_detail_export(input_dir: str) -> Dict[str, List[Dict[str, str]]]:
    """Load the three CSVs produced by tools/export_bundle_pattern_detail.py.

    Raises FileNotFoundError (explicit, not silently degraded) if any file is missing.
    """
    paths = {name: os.path.join(input_dir, name) for name in _REQUIRED_EXPORT_FILES}
    missing = sorted(name for name, p in paths.items() if not os.path.isfile(p))
    if missing:
        raise FileNotFoundError(
            "Missing required export_bundle_pattern_detail.py output(s) in {}: {}".format(
                input_dir, ", ".join(missing)
            )
        )
    return {
        "inventory": read_csv_rows(paths["bundle_pattern_inventory.csv"]),
        "settings": read_csv_rows(paths["pattern_settings.csv"]),
        "names": read_csv_rows(paths["pattern_names.csv"]),
    }


# ---------------------------------------------------------------------------
# Grouping (bundle_pattern_inventory.csv -> requested join_hash set)
# ---------------------------------------------------------------------------

@dataclass
class SkippedRequest:
    segment_id: str
    pattern_id: str
    bundle_ids: List[str]
    reason: str


def group_requested_join_hashes(
    inventory_rows: List[Dict[str, str]]
) -> Tuple[Dict[str, Dict[str, Any]], List[SkippedRequest]]:
    """Dedupe bundle_pattern_inventory.csv rows into requested (domain, join_hash)
    configurations, per PR scope: only domain == "line_patterns" rows are in scope;
    everything else is silently out of scope (not reported), matching
    "Support only the line_patterns domain."

    Rows whose join_hash is blank cannot key a requested configuration at all (there
    is nothing to reconstruct or verify against) -- these are reported separately as
    action=skipped, grouped by pattern_id so distinct un-joined patterns don't
    collapse into a single row.
    """
    requested: Dict[str, Dict[str, Any]] = {}
    skipped_by_key: Dict[str, Dict[str, Any]] = {}

    for row in inventory_rows:
        if str(row.get("domain", "")).strip() != DOMAIN_NAME:
            continue

        segment_id = str(row.get("segment_id", "")).strip()
        bundle_id = str(row.get("bundle_id", "")).strip()
        pattern_id = str(row.get("pattern_id", "")).strip()
        join_hash = str(row.get("join_hash", "")).strip()

        if not join_hash:
            key = pattern_id or "{}::{}".format(segment_id, bundle_id)
            entry = skipped_by_key.setdefault(
                key,
                {"segment_id": segment_id, "pattern_id": pattern_id, "bundle_ids": set()},
            )
            if bundle_id:
                entry["bundle_ids"].add(bundle_id)
            continue

        entry = requested.setdefault(
            join_hash,
            {"segment_id": segment_id, "bundle_ids": set(), "pattern_ids": set()},
        )
        if not entry["segment_id"]:
            entry["segment_id"] = segment_id
        if bundle_id:
            entry["bundle_ids"].add(bundle_id)
        if pattern_id:
            entry["pattern_ids"].add(pattern_id)

    skipped = [
        SkippedRequest(
            segment_id=e["segment_id"],
            pattern_id=e["pattern_id"],
            bundle_ids=sorted(e["bundle_ids"]),
            reason="join_hash_missing",
        )
        for e in sorted(skipped_by_key.values(), key=lambda e: (e["segment_id"], e["pattern_id"]))
    ]
    return requested, skipped


def group_settings_by_join_hash(settings_rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in settings_rows:
        if str(row.get("domain", "")).strip() != DOMAIN_NAME:
            continue
        jh = str(row.get("join_hash", "")).strip()
        if not jh:
            continue
        out.setdefault(jh, []).append(row)
    return out


def group_names_by_join_hash(names_rows: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in names_rows:
        if str(row.get("domain", "")).strip() != DOMAIN_NAME:
            continue
        jh = str(row.get("join_hash", "")).strip()
        if not jh:
            continue
        out.setdefault(jh, []).append(row)
    return out


# ---------------------------------------------------------------------------
# Hash reconstruction (mirrors domains/line_patterns.py + run_extract_all.py)
# ---------------------------------------------------------------------------

def compute_segments_def_hash(segments: List[Tuple[int, int, float]]) -> str:
    """Mirror domains/line_patterns.py::_line_pattern_segments_def_hash's token
    format exactly, operating on (idx, kind, length) tuples instead of live Revit
    LinePatternSegment objects. Used only as an evidence cross-check, never as the
    join identity (segments_def_hash is explicitly_excluded_items in the
    line_patterns join-key policy)."""
    tokens: List[str] = []
    for idx, kind, length in segments:
        tokens.append("seg[{:03d}].kind={}".format(idx, safe_str(kind)))
        length_v, _length_q = canonicalize_float(length, nd=9)
        tokens.append("seg[{:03d}].length={}".format(idx, safe_str(length_v)))
    return make_hash(tokens)


def compute_segments_norm_hash(segments: List[Tuple[int, int, float]]) -> str:
    """Independent reimplementation of
    tools/run_extract_all.py::_append_line_pattern_synthetic_norm_hash's
    per-record normalization algorithm (D-017): ordered kind sequence + segment
    length normalized by ratio to non-dot total length, with dot segments using a
    relative-epsilon placeholder. See this module's docstring for why this is
    intentionally not imported from run_extract_all.py."""
    if not segments:
        return make_hash(["segment_count=0"])

    non_dot_total = sum(length for _, kind, length in segments if kind != DOT_KIND)
    has_non_dot = any(kind != DOT_KIND for _, kind, _ in segments)
    dot_count = sum(1 for _, kind, _ in segments if kind == DOT_KIND)
    eff_total = non_dot_total if has_non_dot else float(dot_count)

    tokens: List[str] = []
    for idx, kind, length in segments:
        if kind == DOT_KIND:
            eff_length = 0.0 if has_non_dot else 1.0
        else:
            eff_length = length
        norm = (eff_length / eff_total) if eff_total > 0 else 0.0
        tokens.append("seg[{:03d}].kind={}".format(idx, kind))
        tokens.append("seg[{:03d}].norm_length={:.6f}".format(idx, norm))
    return make_hash(tokens)


def compute_join_hash_for_segments(
    segments: List[Tuple[int, int, float]],
    *,
    domain_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Dict[str, Any], List[str]]:
    """Authoritative join_hash for a reconstructed/read-back segment list, computed
    via core.join_key_builder.build_join_key_from_policy against the real
    line_patterns join-key policy (line_patterns.join_key.v3). This is the single
    function used both for the pre-mutation evidence self-consistency check and for
    post-creation verification -- see mapping/line_pattern_revit_apply.py."""
    if domain_policy is None:
        domain_policy = get_line_patterns_join_key_policy()
    norm_hash = compute_segments_norm_hash(segments)
    items = [make_identity_item(SEGMENTS_NORM_HASH_KEY, norm_hash, ITEM_Q_OK)]
    join_key, missing = build_join_key_from_policy(
        domain_policy=domain_policy,
        identity_items=items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    return join_key.get("join_hash"), join_key, missing


# ---------------------------------------------------------------------------
# Reconstruction
# ---------------------------------------------------------------------------

@dataclass
class ReconstructedPattern:
    join_hash: str
    status: str
    reasons: List[str] = field(default_factory=list)
    segments: Optional[List[Tuple[int, int, float]]] = None
    segments_def_hash_recomputed: Optional[str] = None
    segments_norm_hash_recomputed: Optional[str] = None
    reconstructed_join_hash: Optional[str] = None

    @property
    def blocked(self) -> bool:
        return self.status == STATUS_BLOCKED


def _blocked(join_hash: str, reason: str) -> ReconstructedPattern:
    return ReconstructedPattern(join_hash=join_hash, status=STATUS_BLOCKED, reasons=[reason])


def reconstruct_pattern(join_hash: str, settings_rows: List[Dict[str, str]]) -> ReconstructedPattern:
    """Reconstruct an ordered segment list for one requested join_hash from
    pattern_settings.csv rows, validating evidence completeness per the PR's
    blocking rules. Returns status=blocked (segments=None) on any validation
    failure; never infers a missing value."""
    reasons: List[str] = []

    if not settings_rows:
        return _blocked(join_hash, "settings_absent")

    # Duplicate-key detection on the raw rows, before any dict collapse would hide it.
    key_counts: Dict[str, int] = {}
    for row in settings_rows:
        k = str(row.get("k", "")).strip()
        key_counts[k] = key_counts.get(k, 0) + 1

    if NO_ITEMS_MARKER_KEY in key_counts:
        return _blocked(join_hash, "no_items_marker")

    dup_keys = sorted(k for k, c in key_counts.items() if c > 1 and k)
    if dup_keys:
        return ReconstructedPattern(
            join_hash=join_hash,
            status=STATUS_BLOCKED,
            reasons=["duplicate_settings_key:{}".format(k) for k in dup_keys],
        )

    items: Dict[str, Tuple[str, str]] = {
        str(row.get("k", "")).strip(): (row.get("v", ""), str(row.get("q", "")).strip())
        for row in settings_rows
    }

    if SEGMENT_COUNT_KEY not in items:
        return _blocked(join_hash, "segment_count_missing")
    sc_v, sc_q = items[SEGMENT_COUNT_KEY]
    if sc_q != ITEM_Q_OK:
        return _blocked(join_hash, "segment_count_quality:{}".format(sc_q))
    try:
        segment_count = int(str(sc_v).strip())
    except (TypeError, ValueError):
        return _blocked(join_hash, "segment_count_invalid")
    if segment_count < 1:
        return _blocked(join_hash, "segment_count_not_creatable:{}".format(segment_count))

    seg_map: Dict[int, Dict[str, Tuple[str, str]]] = {}
    for k, (v, q) in items.items():
        m = _SEGMENT_ITEM_KEY_RE.match(k)
        if not m:
            continue
        idx = int(m.group(1))
        field_name = m.group(2)
        seg_map.setdefault(idx, {})[field_name] = (v, q)

    indices = sorted(seg_map.keys())
    if not indices:
        return _blocked(join_hash, "segment_rows_absent")
    if indices != list(range(len(indices))):
        return _blocked(join_hash, "segment_indices_non_contiguous")
    if len(indices) != segment_count:
        return _blocked(
            join_hash,
            "segment_count_mismatch:declared={}:found={}".format(segment_count, len(indices)),
        )

    segments: List[Tuple[int, int, float]] = []
    for idx in indices:
        fields = seg_map[idx]
        if "kind" not in fields or "length" not in fields:
            return _blocked(join_hash, "segment_incomplete:{:03d}".format(idx))

        kind_v, kind_q = fields["kind"]
        length_v, length_q = fields["length"]

        if kind_q != ITEM_Q_OK:
            return _blocked(join_hash, "segment_kind_quality:{:03d}:{}".format(idx, kind_q))
        if length_q != ITEM_Q_OK:
            return _blocked(join_hash, "segment_length_quality:{:03d}:{}".format(idx, length_q))

        try:
            kind = int(str(kind_v).strip())
        except (TypeError, ValueError):
            return _blocked(join_hash, "segment_kind_invalid:{:03d}".format(idx))
        if kind not in _LP_SEG_TYPE_NAME:
            return _blocked(join_hash, "segment_kind_unmapped:{:03d}:{}".format(idx, kind))

        try:
            length = float(str(length_v).strip())
        except (TypeError, ValueError):
            return _blocked(join_hash, "segment_length_invalid:{:03d}".format(idx))
        if not math.isfinite(length):
            return _blocked(join_hash, "segment_length_invalid:{:03d}".format(idx))

        if kind == DOT_KIND:
            # Authoritative Dot-length normalization (domains/line_patterns.py
            # forces this at extraction time already; defend here too in case the
            # CSV was hand-edited/tampered/stale).
            if length != 0.0:
                reasons.append("dot_length_not_normalized:{:03d}".format(idx))
            length = 0.0
        elif length <= 0.0:
            return _blocked(join_hash, "segment_length_non_positive:{:03d}".format(idx))

        segments.append((idx, kind, length))

    def_hash_recomputed = compute_segments_def_hash(segments)
    evidence_def_hash_v, evidence_def_hash_q = items.get(SEGMENTS_DEF_HASH_KEY, (None, None))
    if evidence_def_hash_q == ITEM_Q_OK and str(evidence_def_hash_v or "").strip():
        if str(evidence_def_hash_v).strip() != def_hash_recomputed:
            return _blocked(join_hash, "segments_def_hash_mismatch")
    else:
        reasons.append("segments_def_hash_evidence_unavailable")

    norm_hash_recomputed = compute_segments_norm_hash(segments)
    evidence_norm_hash_v, evidence_norm_hash_q = items.get(SEGMENTS_NORM_HASH_KEY, (None, None))
    if evidence_norm_hash_q == ITEM_Q_OK and str(evidence_norm_hash_v or "").strip():
        if str(evidence_norm_hash_v).strip() != norm_hash_recomputed:
            return _blocked(join_hash, "segments_norm_hash_mismatch")
    else:
        reasons.append("segments_norm_hash_evidence_unavailable")

    reconstructed_join_hash, _join_key, _missing = compute_join_hash_for_segments(segments)
    if reconstructed_join_hash != join_hash:
        return ReconstructedPattern(
            join_hash=join_hash,
            status=STATUS_BLOCKED,
            reasons=["reconstructed_join_hash_mismatch"],
            reconstructed_join_hash=reconstructed_join_hash,
        )

    status = STATUS_DEGRADED if reasons else STATUS_OK
    return ReconstructedPattern(
        join_hash=join_hash,
        status=status,
        reasons=reasons,
        segments=segments,
        segments_def_hash_recomputed=def_hash_recomputed,
        segments_norm_hash_recomputed=norm_hash_recomputed,
        reconstructed_join_hash=reconstructed_join_hash,
    )


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

def short_join_hash(join_hash: str, length: int = SHORT_JOIN_HASH_LEN) -> str:
    return str(join_hash)[:length]


def sanitize_revit_name(raw: Optional[str]) -> str:
    """Deterministically replace characters Revit rejects in element/type names
    (\\:{}|;<>?'~ and control chars) with '_'. New rule introduced by this PR --
    nothing upstream needs to construct Revit-legal names today."""
    s = "" if raw is None else str(raw).strip()
    out_chars = []
    for ch in s:
        if ch in _INVALID_NAME_CHARS or ord(ch) < 32:
            out_chars.append("_")
        else:
            out_chars.append(ch)
    out = "".join(out_chars).strip()
    if not out:
        out = "unnamed"
    return out[:_MAX_REVIT_NAME_LEN]


def select_observed_name(name_rows: List[Dict[str, str]]) -> Tuple[Optional[str], List[str]]:
    """Deterministic observed_name selection per PR spec:
      1. consider only acceptable-quality observed names (label_q == "ok" and non-empty label_v)
      2. highest files_count wins
      3. lexical (ascending) ordering is the tie-breaker
    Returns (observed_name_or_None, reasons).
    """
    acceptable = [
        r
        for r in (name_rows or [])
        if str(r.get("label_q", "")).strip() == ITEM_Q_OK and str(r.get("label_v", "")).strip()
    ]
    if not acceptable:
        return None, ["no_acceptable_observed_name"]

    def _files_count(r: Dict[str, str]) -> int:
        try:
            return int(str(r.get("files_count", "")).strip())
        except (TypeError, ValueError):
            return 0

    best = sorted(
        acceptable,
        key=lambda r: (-_files_count(r), str(r.get("label_v", "")).strip()),
    )[0]
    return str(best.get("label_v")).strip(), []


def resolve_observed_name(name_rows: List[Dict[str, str]], join_hash: str) -> Tuple[str, bool, List[str]]:
    """Returns (observed_name, is_synthetic, reasons)."""
    observed_name, reasons = select_observed_name(name_rows)
    if observed_name is not None:
        return observed_name, False, reasons
    synthetic = "unnamed_{}".format(short_join_hash(join_hash))
    return synthetic, True, reasons + ["synthetic_name_used"]


def build_mapping_name_candidates(observed_name: str, join_hash: str) -> Tuple[str, str]:
    """Returns (primary_mapping_name, collision_safe_mapping_name)."""
    sanitized = sanitize_revit_name(observed_name)
    primary = "{}{}".format(MAPPING_NAME_PREFIX, sanitized)
    collision = "{}{}__{}".format(MAPPING_NAME_PREFIX, sanitized, short_join_hash(join_hash))
    return primary, collision


# ---------------------------------------------------------------------------
# Outcome / report (pure data + formatting -- no Revit dependency)
# ---------------------------------------------------------------------------

ACTION_EXISTING = "existing"
ACTION_CREATED = "created"
ACTION_SKIPPED = "skipped"
ACTION_BLOCKED = "blocked"


@dataclass
class MappingOutcome:
    """One requested (domain, join_hash) configuration's final disposition.
    Populated by mapping/line_pattern_revit_apply.py (or synthesized directly in
    tests); build_report_rows()/write_report_csv() below never touch Revit."""

    join_hash: str
    segment_id: str = ""
    domain: str = DOMAIN_NAME
    observed_name: str = ""
    mapping_name: str = ""
    action: str = ACTION_BLOCKED
    status: str = STATUS_BLOCKED
    reasons: List[str] = field(default_factory=list)
    revit_element_id: str = ""
    requested_join_hash: str = ""
    verified_join_hash: str = ""
    bundle_ids: List[str] = field(default_factory=list)
    pattern_ids: List[str] = field(default_factory=list)


REPORT_FIELDS: Tuple[str, ...] = (
    "segment_id",
    "domain",
    "join_hash",
    "observed_name",
    "mapping_name",
    "action",
    "status",
    "status_reason",
    "revit_element_id",
    "requested_join_hash",
    "verified_join_hash",
    "source_bundle_ids",
    "source_pattern_ids",
)


def outcome_to_report_row(outcome: MappingOutcome) -> Dict[str, str]:
    return {
        "segment_id": outcome.segment_id,
        "domain": outcome.domain,
        "join_hash": outcome.join_hash,
        "observed_name": outcome.observed_name,
        "mapping_name": outcome.mapping_name,
        "action": outcome.action,
        "status": outcome.status,
        "status_reason": ";".join(sorted(set(outcome.reasons))),
        "revit_element_id": outcome.revit_element_id,
        "requested_join_hash": outcome.requested_join_hash,
        "verified_join_hash": outcome.verified_join_hash,
        "source_bundle_ids": ";".join(sorted(set(outcome.bundle_ids))),
        "source_pattern_ids": ";".join(sorted(set(outcome.pattern_ids))),
    }


def build_report_rows(outcomes: List[MappingOutcome]) -> List[Dict[str, str]]:
    """Deterministic row ordering: by join_hash, then segment_id/pattern_ids for the
    (rare) skipped rows whose join_hash is blank."""
    ordered = sorted(
        outcomes,
        key=lambda o: (o.join_hash or "", o.segment_id or "", tuple(sorted(o.pattern_ids))),
    )
    return [outcome_to_report_row(o) for o in ordered]


def write_report_csv(path: str, rows: List[Dict[str, str]]) -> None:
    """Atomic CSV write (write-to-temp then rename) so a failed/partial run never
    leaves a truncated report at the requested path."""
    out_dir = os.path.dirname(os.path.abspath(path))
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(REPORT_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in REPORT_FIELDS})
    os.replace(tmp_path, path)


def compute_run_status(outcomes: List[MappingOutcome]) -> str:
    """Overall run status using the same ok/degraded/blocked vocabulary as each row
    (blocked > degraded > ok dominance, per dominant_status())."""
    if not outcomes:
        return STATUS_OK
    return dominant_status([o.status for o in outcomes])

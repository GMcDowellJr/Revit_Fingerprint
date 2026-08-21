# -*- coding: utf-8 -*-
"""
Pure-Python reconstruction/validation/naming logic for the fill_patterns
Revit mapping utility (see mapping/fill_pattern_revit_apply.py for the
Revit-API-touching half, and mapping/create_fill_pattern_mappings.py for the
Dynamo entry point).

No Revit dependency -- safe to unit test with plain pytest.

This module consumes the three CSVs produced by
tools/export_bundle_pattern_detail.py:
    bundle_pattern_inventory.csv
    pattern_settings.csv
    pattern_names.csv

and reconstructs an ordered fill-pattern grid definition per requested
(domain, join_hash) -- domain is one of "fill_patterns_drafting" /
"fill_patterns_model" (the D-015 domain-family partitions
domains/fill_patterns.py emits) -- validating evidence completeness BEFORE
any Revit mutation is attempted. Nothing here writes to a Revit document.

Both partitions share this one reconstruction module (not one per
partition): the only differences between them are the expected
`fill_pattern.target` value ("Drafting" vs "Model") and which join-key
policy entry applies -- everything else (grid item shape, hashing,
naming) is identical. See the module's DOMAIN_NAMES/TARGET_NAME_BY_DOMAIN.

Reuse, not duplication
-----------------------
This module deliberately reuses the repository's authoritative
canonicalization/hashing/join-key primitives rather than re-deriving them:

  - core.hashing.make_hash / safe_str                      (hash primitive)
  - core.record_v2.canonicalize_float / canonicalize_int / canonicalize_str /
    ITEM_Q_OK / STATUS_* / make_identity_item
  - core.join_key_policy / core.join_key_builder            (join_hash computation,
    against policies/domain_join_key_policies.json's "fill_patterns_drafting"/
    "fill_patterns_model" entries)

Do NOT substitute sig_hash for join_hash: domains/fill_patterns.py's sig_hash
is computed from serialize_identity_items() over the FULL, sorted
identity_basis.items list (every individual fill_pattern.grid[NNN].* field),
while join_hash (via build_join_key_from_policy against required_items =
["fill_pattern.target", "fill_pattern.grid_count", "fill_pattern.grids_def_hash"])
is computed from just those three summary items. These are different
preimages over different item sets -- sig_hash and join_hash are NOT equal
in general for this domain (unlike a single-required-item domain where
core.join_key_builder.build_join_key_from_policy's def_hash-passthrough
shortcut would make them coincide). This module verifies against join_hash
because that is what bundle_pattern_inventory.csv/domain_patterns.csv
clustering keys on (tools/extractor.py's cluster_id, sourced from
phase0_records.csv's join_hash column) -- see
docs/fill_pattern_mapping.md for the full trace.

grids_def_hash is NOT a flatten-stage synthetic augmentation
--------------------------------------------------------------
Unlike line_patterns' segments_norm_hash (appended synthetically by
tools/run_extract_all.py during the flatten stage -- see
mapping/line_pattern_reconstruction.py's docstring), fill_pattern.grids_def_hash
is computed INLINE by domains/fill_patterns.py itself at extraction time, from
the ordered per-grid identity items (grid_count + each
fill_pattern.grid[NNN].* item) via a raw "k=..|q=..|v=.." token join + make_hash
-- there is no run_extract_all.py-side synthetic augmentation for this domain
(confirmed: no `fill_pattern` reference anywhere in tools/run_extract_all.py).
compute_grids_def_hash() below reconstructs that SAME per-record algorithm from
already-exported evidence.

Grid item insertion order is NOT preserved in the exported CSV
-----------------------------------------------------------------
domains/fill_patterns.py builds each grid's items in a fixed insertion order
(angle, origin.kind, then origin.u+origin.v OR origin.x+origin.y depending on
origin.kind, then offset, shift) and hashes them in THAT order -- explicitly
NOT sorted ("grid order is identity-significant; do NOT sort the preimage").
But the record's identity_basis.items (and therefore
identity_items_by_domain/<domain>.csv, and therefore pattern_settings.csv) is
always the LEXICALLY-SORTED-BY-KEY list (identity_items_v2_sorted), because
that sorted list is what feeds sig_hash/identity_basis -- the original
insertion order is not preserved anywhere in the export. This module does NOT
try to recover insertion order from the CSV (it can't -- the information is
gone); instead it reconstructs the KNOWN, fixed insertion order directly from
domains/fill_patterns.py's own field-building sequence (a code invariant, not
data), the same way tools/pattern_id_utils.py independently reimplements
tools/extractor.py's private _stable_pattern_id() rather than importing it.
test_fill_pattern_mapping_reconstruction.py cross-checks this against
domains/fill_patterns.py's own grids_def_hash computation directly (importing
the domain module and comparing hash values over synthetic grid lists), the
same kind of gap-closing cross-check
test_line_pattern_mapping_reconstruction.py already does for
segments_norm_hash.
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
    canonicalize_int,
    canonicalize_str,
    make_identity_item,
)
from core.join_key_policy import load_join_key_policies, get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DOMAIN_DRAFTING = "fill_patterns_drafting"
DOMAIN_MODEL = "fill_patterns_model"
DOMAIN_NAMES: Tuple[str, ...] = (DOMAIN_DRAFTING, DOMAIN_MODEL)

TARGET_NAME_BY_DOMAIN: Dict[str, str] = {
    DOMAIN_DRAFTING: "Drafting",
    DOMAIN_MODEL: "Model",
}

TARGET_KEY = "fill_pattern.target"
GRID_COUNT_KEY = "fill_pattern.grid_count"
GRIDS_DEF_HASH_KEY = "fill_pattern.grids_def_hash"
NO_ITEMS_MARKER_KEY = "__no_items__"

_GRID_ITEM_KEY_RE = re.compile(
    r"^fill_pattern\.grid\[(\d+)\]\.(angle|origin\.kind|origin\.u|origin\.v|origin\.x|origin\.y|offset|shift)$"
)

_ORIGIN_KIND_UV = "uv"
_ORIGIN_KIND_XY = "xy"

MAPPING_NAME_PREFIX = "MAP__"
SHORT_JOIN_HASH_LEN = 12

# Characters Revit rejects in element/type names (same set line_patterns uses).
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


def get_fill_pattern_join_key_policy(domain_name: str) -> Optional[Dict[str, Any]]:
    """Load (and cache) the authoritative join-key policy for one fill_patterns partition."""
    global _policy_cache
    if _policy_cache is None:
        _policy_cache = load_join_key_policies(_POLICY_PATH)
    return get_domain_join_key_policy(_policy_cache, domain_name)


# ---------------------------------------------------------------------------
# CSV loading (domain-agnostic -- shared by both partitions)
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
# Grouping (bundle_pattern_inventory.csv -> requested join_hash set), per domain
# ---------------------------------------------------------------------------

@dataclass
class SkippedRequest:
    domain: str
    segment_id: str
    pattern_id: str
    bundle_ids: List[str]
    reason: str


def group_requested_join_hashes(
    inventory_rows: List[Dict[str, str]], domain_name: str
) -> Tuple[Dict[str, Dict[str, Any]], List[SkippedRequest]]:
    """Dedupe bundle_pattern_inventory.csv rows into requested (domain, join_hash)
    configurations for ONE fill_patterns partition (domain_name). Callers process
    both DOMAIN_NAMES against the same export directory, one call each -- the
    two partitions never mix within a single call.

    Rows whose join_hash is blank cannot key a requested configuration at all
    -- reported separately as action=skipped, grouped by pattern_id.
    """
    requested: Dict[str, Dict[str, Any]] = {}
    skipped_by_key: Dict[str, Dict[str, Any]] = {}

    for row in inventory_rows:
        if str(row.get("domain", "")).strip() != domain_name:
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
            domain=domain_name,
            segment_id=e["segment_id"],
            pattern_id=e["pattern_id"],
            bundle_ids=sorted(e["bundle_ids"]),
            reason="join_hash_missing",
        )
        for e in sorted(skipped_by_key.values(), key=lambda e: (e["segment_id"], e["pattern_id"]))
    ]
    return requested, skipped


def group_settings_by_join_hash(settings_rows: List[Dict[str, str]], domain_name: str) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in settings_rows:
        if str(row.get("domain", "")).strip() != domain_name:
            continue
        jh = str(row.get("join_hash", "")).strip()
        if not jh:
            continue
        out.setdefault(jh, []).append(row)
    return out


def group_names_by_join_hash(names_rows: List[Dict[str, str]], domain_name: str) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in names_rows:
        if str(row.get("domain", "")).strip() != domain_name:
            continue
        jh = str(row.get("join_hash", "")).strip()
        if not jh:
            continue
        out.setdefault(jh, []).append(row)
    return out


# ---------------------------------------------------------------------------
# Hash reconstruction (mirrors domains/fill_patterns.py's grids_def_hash + join_key)
# ---------------------------------------------------------------------------

@dataclass
class ReconstructedGrid:
    idx: int
    angle: float
    origin_kind: str  # "uv" | "xy"
    origin_a: float  # u or x
    origin_b: float  # v or y
    offset: float
    shift: float


def _grid_identity_items(grid_count: int, grids: List[ReconstructedGrid]) -> List[Dict[str, Any]]:
    """Rebuild the grid_count + per-grid identity items in domains/fill_patterns.py's
    OWN fixed insertion order (angle, origin.kind, then origin.u/v OR origin.x/y,
    then offset, shift), re-canonicalizing each value via the same canonicalize_*
    functions the domain itself uses. This is the preimage compute_grids_def_hash()
    filters down to grid_count + grid[NNN].* tokens.
    """
    gc_v, gc_q = canonicalize_int(grid_count)
    items: List[Dict[str, Any]] = [make_identity_item(GRID_COUNT_KEY, gc_v, gc_q)]

    for g in grids:
        idx = "{:03d}".format(g.idx)
        ang_v, ang_q = canonicalize_float(g.angle)
        items.append(make_identity_item("fill_pattern.grid[{}].angle".format(idx), ang_v, ang_q))

        kind_v, kind_q = canonicalize_str(g.origin_kind)
        items.append(make_identity_item("fill_pattern.grid[{}].origin.kind".format(idx), kind_v, kind_q))

        if g.origin_kind == _ORIGIN_KIND_UV:
            a_v, a_q = canonicalize_float(g.origin_a)
            b_v, b_q = canonicalize_float(g.origin_b)
            items.append(make_identity_item("fill_pattern.grid[{}].origin.u".format(idx), a_v, a_q))
            items.append(make_identity_item("fill_pattern.grid[{}].origin.v".format(idx), b_v, b_q))
        elif g.origin_kind == _ORIGIN_KIND_XY:
            a_v, a_q = canonicalize_float(g.origin_a)
            b_v, b_q = canonicalize_float(g.origin_b)
            items.append(make_identity_item("fill_pattern.grid[{}].origin.x".format(idx), a_v, a_q))
            items.append(make_identity_item("fill_pattern.grid[{}].origin.y".format(idx), b_v, b_q))
        # else: kind unmapped -- no origin leaf items at all (mirrors the domain).

        off_v, off_q = canonicalize_float(g.offset)
        items.append(make_identity_item("fill_pattern.grid[{}].offset".format(idx), off_v, off_q))
        sh_v, sh_q = canonicalize_float(g.shift)
        items.append(make_identity_item("fill_pattern.grid[{}].shift".format(idx), sh_v, sh_q))

    return items


def compute_grids_def_hash(grid_count: int, grids: List[ReconstructedGrid]) -> str:
    """Mirror domains/fill_patterns.py's inline grids_def_hash computation
    exactly: k=..|q=..|v=.. tokens (v serialized via safe_str, so None becomes
    the literal string "None" -- NOT an empty string, unlike
    core.record_v2.serialize_identity_items' "" convention) over grid_count +
    every fill_pattern.grid[NNN].* item, in INSERTION order (not sorted).
    """
    items = _grid_identity_items(grid_count, grids)
    grid_like = [
        "k={}|q={}|v={}".format(safe_str(it.get("k", "")), safe_str(it.get("q", "")), safe_str(it.get("v")))
        for it in items
    ]
    return make_hash(grid_like)


def compute_join_hash_for_grids(
    domain_name: str,
    target_name: str,
    grid_count: int,
    grids_def_hash: str,
    *,
    domain_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Dict[str, Any], List[str]]:
    """Authoritative join_hash for a reconstructed/read-back grid list, computed via
    core.join_key_builder.build_join_key_from_policy against the real
    fill_patterns_drafting/fill_patterns_model join-key policy (required_items =
    target, grid_count, grids_def_hash). Used both for the pre-mutation evidence
    self-consistency check and for post-creation verification -- see
    mapping/fill_pattern_revit_apply.py.
    """
    if domain_policy is None:
        domain_policy = get_fill_pattern_join_key_policy(domain_name)
    gc_v, gc_q = canonicalize_int(grid_count)
    items = [
        make_identity_item(TARGET_KEY, target_name, ITEM_Q_OK),
        make_identity_item(GRID_COUNT_KEY, gc_v, gc_q),
        make_identity_item(GRIDS_DEF_HASH_KEY, grids_def_hash, ITEM_Q_OK),
    ]
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

class _Block(Exception):
    """Internal control-flow exception for per-grid-field validation inside
    reconstruct_pattern(); never escapes that function."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class ReconstructedPattern:
    domain: str
    join_hash: str
    status: str
    reasons: List[str] = field(default_factory=list)
    target_name: Optional[str] = None
    grid_count: Optional[int] = None
    grids: Optional[List[ReconstructedGrid]] = None
    grids_def_hash_recomputed: Optional[str] = None
    reconstructed_join_hash: Optional[str] = None

    @property
    def blocked(self) -> bool:
        return self.status == STATUS_BLOCKED


def _blocked(domain: str, join_hash: str, reason: str) -> ReconstructedPattern:
    return ReconstructedPattern(domain=domain, join_hash=join_hash, status=STATUS_BLOCKED, reasons=[reason])


def reconstruct_pattern(domain_name: str, join_hash: str, settings_rows: List[Dict[str, str]]) -> ReconstructedPattern:
    """Reconstruct an ordered grid definition for one requested (domain, join_hash)
    from pattern_settings.csv rows, validating evidence completeness. Returns
    status=blocked (grids=None) on any validation failure; never infers a
    missing value.
    """
    reasons: List[str] = []
    expected_target = TARGET_NAME_BY_DOMAIN[domain_name]

    if not settings_rows:
        return _blocked(domain_name, join_hash, "settings_absent")

    key_counts: Dict[str, int] = {}
    for row in settings_rows:
        k = str(row.get("k", "")).strip()
        key_counts[k] = key_counts.get(k, 0) + 1

    if NO_ITEMS_MARKER_KEY in key_counts:
        return _blocked(domain_name, join_hash, "no_items_marker")

    dup_keys = sorted(k for k, c in key_counts.items() if c > 1 and k)
    if dup_keys:
        return ReconstructedPattern(
            domain=domain_name,
            join_hash=join_hash,
            status=STATUS_BLOCKED,
            reasons=["duplicate_settings_key:{}".format(k) for k in dup_keys],
        )

    items: Dict[str, Tuple[str, str]] = {
        str(row.get("k", "")).strip(): (row.get("v", ""), str(row.get("q", "")).strip())
        for row in settings_rows
    }

    # Cross-check target evidence, if present (degraded, not blocked, if absent).
    if TARGET_KEY in items:
        t_v, t_q = items[TARGET_KEY]
        if t_q == ITEM_Q_OK:
            if str(t_v).strip() != expected_target:
                return _blocked(domain_name, join_hash, "target_mismatch:{}".format(t_v))
        else:
            reasons.append("target_quality:{}".format(t_q))
    else:
        reasons.append("target_evidence_unavailable")

    if GRID_COUNT_KEY not in items:
        return _blocked(domain_name, join_hash, "grid_count_missing")
    gc_v, gc_q = items[GRID_COUNT_KEY]
    if gc_q != ITEM_Q_OK:
        return _blocked(domain_name, join_hash, "grid_count_quality:{}".format(gc_q))
    try:
        grid_count = int(str(gc_v).strip())
    except (TypeError, ValueError):
        return _blocked(domain_name, join_hash, "grid_count_invalid")
    if grid_count < 1:
        return _blocked(domain_name, join_hash, "grid_count_not_creatable:{}".format(grid_count))

    grids: List[ReconstructedGrid] = []
    for idx in range(grid_count):
        pfx = "fill_pattern.grid[{:03d}].".format(idx)

        def _require_float(field_name: str) -> float:
            k = pfx + field_name
            if k not in items:
                raise _Block("grid_incomplete:{:03d}:{}".format(idx, field_name))
            v, q = items[k]
            if q != ITEM_Q_OK:
                raise _Block("grid_field_quality:{:03d}:{}:{}".format(idx, field_name, q))
            try:
                fv = float(str(v).strip())
            except (TypeError, ValueError):
                raise _Block("grid_field_invalid:{:03d}:{}".format(idx, field_name))
            if not math.isfinite(fv):
                raise _Block("grid_field_invalid:{:03d}:{}".format(idx, field_name))
            return fv

        try:
            angle = _require_float("angle")

            kind_key = pfx + "origin.kind"
            if kind_key not in items:
                return _blocked(domain_name, join_hash, "grid_incomplete:{:03d}:origin.kind".format(idx))
            kind_v, kind_q = items[kind_key]
            if kind_q != ITEM_Q_OK:
                return _blocked(domain_name, join_hash, "grid_field_quality:{:03d}:origin.kind:{}".format(idx, kind_q))
            origin_kind = str(kind_v).strip()
            if origin_kind == _ORIGIN_KIND_UV:
                origin_a = _require_float("origin.u")
                origin_b = _require_float("origin.v")
            elif origin_kind == _ORIGIN_KIND_XY:
                origin_a = _require_float("origin.x")
                origin_b = _require_float("origin.y")
            else:
                return _blocked(domain_name, join_hash, "grid_origin_kind_unmapped:{:03d}:{}".format(idx, origin_kind))

            offset = _require_float("offset")
            shift = _require_float("shift")
        except _Block as b:
            return _blocked(domain_name, join_hash, b.reason)

        grids.append(
            ReconstructedGrid(
                idx=idx,
                angle=angle,
                origin_kind=origin_kind,
                origin_a=origin_a,
                origin_b=origin_b,
                offset=offset,
                shift=shift,
            )
        )

    grids_def_hash_recomputed = compute_grids_def_hash(grid_count, grids)
    evidence_v, evidence_q = items.get(GRIDS_DEF_HASH_KEY, (None, None))
    if evidence_q == ITEM_Q_OK and str(evidence_v or "").strip():
        if str(evidence_v).strip() != grids_def_hash_recomputed:
            return _blocked(domain_name, join_hash, "grids_def_hash_mismatch")
    else:
        reasons.append("grids_def_hash_evidence_unavailable")

    reconstructed_join_hash, _join_key, _missing = compute_join_hash_for_grids(
        domain_name, expected_target, grid_count, grids_def_hash_recomputed
    )
    if reconstructed_join_hash != join_hash:
        return ReconstructedPattern(
            domain=domain_name,
            join_hash=join_hash,
            status=STATUS_BLOCKED,
            reasons=["reconstructed_join_hash_mismatch"],
            reconstructed_join_hash=reconstructed_join_hash,
        )

    status = STATUS_DEGRADED if reasons else STATUS_OK
    return ReconstructedPattern(
        domain=domain_name,
        join_hash=join_hash,
        status=status,
        reasons=reasons,
        target_name=expected_target,
        grid_count=grid_count,
        grids=grids,
        grids_def_hash_recomputed=grids_def_hash_recomputed,
        reconstructed_join_hash=reconstructed_join_hash,
    )


# ---------------------------------------------------------------------------
# Naming (identical convention to mapping/line_pattern_reconstruction.py)
# ---------------------------------------------------------------------------

def short_join_hash(join_hash: str, length: int = SHORT_JOIN_HASH_LEN) -> str:
    return str(join_hash)[:length]


def sanitize_revit_name(raw: Optional[str]) -> str:
    """Deterministically replace characters Revit rejects in element/type names."""
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
    """Deterministic observed_name selection:
      1. consider only acceptable-quality observed names (label_q == "ok" and non-empty label_v)
      2. highest files_count wins
      3. lexical (ascending) ordering is the tie-breaker
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
    Populated by mapping/fill_pattern_revit_apply.py (or synthesized directly in
    tests); build_report_rows()/write_report_csv() below never touch Revit."""

    domain: str
    join_hash: str
    segment_id: str = ""
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
    """Deterministic row ordering: by domain, then join_hash, then
    segment_id/pattern_ids for the (rare) skipped rows whose join_hash is blank."""
    ordered = sorted(
        outcomes,
        key=lambda o: (o.domain or "", o.join_hash or "", o.segment_id or "", tuple(sorted(o.pattern_ids))),
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

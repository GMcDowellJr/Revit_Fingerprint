# -*- coding: utf-8 -*-
"""
Revit-API half of the line_patterns mapping utility. Everything here requires
a live Revit document; mapping/line_pattern_reconstruction.py holds the
pure-Python evidence validation, hashing, and naming logic this module calls
into rather than re-deriving.

Revit API construction path used:
    lp = Autodesk.Revit.DB.LinePattern(name)
    lp.SetSegments(List[LinePatternSegment]([LinePatternSegment(LinePatternSegmentType.<Kind>, length), ...]))
    element = Autodesk.Revit.DB.LinePatternElement.Create(doc, lp)
inside a bounded Autodesk.Revit.DB.Transaction per requested join_hash.

Follows the repository's existing guarded-import convention for Revit API
symbols (see domains/line_patterns.py, core/collect.py) so this module can
still be imported (with everything below resolving to None) outside Revit,
e.g. by tooling that inspects the mapping/ package without a live session.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.hashing import safe_str
from core.collect import collect_instances
from domains.line_patterns import _LP_SEG_TYPE_NAME, _lp_seg_type_id_and_name

from mapping.line_pattern_reconstruction import (
    ACTION_BLOCKED,
    ACTION_CREATED,
    ACTION_EXISTING,
    MappingOutcome,
    ReconstructedPattern,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    build_mapping_name_candidates,
    compute_join_hash_for_segments,
    dominant_status,
    get_line_patterns_join_key_policy,
    resolve_observed_name,
)

try:
    from Autodesk.Revit.DB import (
        LinePattern,
        LinePatternElement,
        LinePatternSegment,
        LinePatternSegmentType,
        Transaction,
        TransactionStatus,
    )
except Exception:
    LinePattern = None
    LinePatternElement = None
    LinePatternSegment = None
    LinePatternSegmentType = None
    Transaction = None
    TransactionStatus = None

try:
    import clr  # noqa: F401  (present in the Dynamo CPython3 host; enables the CLR import below)
    from System.Collections.Generic import List as _NetList
except Exception:
    _NetList = None


TRANSACTION_NAME_PREFIX = "Fingerprint Mapping: Create Line Pattern "


# ---------------------------------------------------------------------------
# Reading segments back from a live Revit LinePatternElement
# ---------------------------------------------------------------------------

def read_segments_from_element(doc: Any, element: Any) -> Optional[List[Tuple[int, int, float]]]:
    """Mirror domains/line_patterns.py::extract()'s segment-acquisition path
    (GetLinePattern -> GetSegments/.Segments) for a single already-resolved
    element, reusing its private _lp_seg_type_id_and_name() for the actual
    kind-reading logic so the "Type vs SegmentType" API fallback and the
    canonical 0/1/2 mapping stay in exactly one place. Returns None (not a
    sentinel value) if the pattern/segments could not be read at all -- callers
    treat that as a bounded, explicit failure, never as an empty pattern.
    """
    lp = None
    try:
        lp = element.GetLinePattern()
    except Exception:
        lp = None
    if lp is None:
        try:
            lp = LinePatternElement.GetLinePattern(doc, element.Id)
        except Exception:
            lp = None
    if lp is None:
        return None

    try:
        if hasattr(lp, "GetSegments"):
            raw_segments = list(lp.GetSegments() or [])
        else:
            raw_segments = list(getattr(lp, "Segments", None) or [])
    except Exception:
        return None

    segments: List[Tuple[int, int, float]] = []
    for idx, seg in enumerate(raw_segments):
        kind, _kind_name = _lp_seg_type_id_and_name(seg)
        if kind is None:
            return None
        try:
            length = getattr(seg, "Length", None)
        except Exception:
            length = None
        if kind == 2:  # Dot -- same normalization domains/line_patterns.py applies
            length = 0.0
        try:
            length = float(length)
        except (TypeError, ValueError):
            return None
        segments.append((idx, kind, length))

    return segments


# ---------------------------------------------------------------------------
# Name index (existing LinePatternElement objects in the current document)
# ---------------------------------------------------------------------------

def build_name_index(doc: Any, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """name -> LinePatternElement, for collision detection. Reuses
    core.collect.collect_instances rather than a direct FilteredElementCollector
    call, per the repository's "never collect directly in a domain" convention
    (this module isn't a domain, but there's no reason to re-derive the
    collector wrapper's caching/require_unique_id behavior here)."""
    elements = collect_instances(
        doc,
        of_class=LinePatternElement,
        require_unique_id=True,
        cctx=(ctx or {}).get("_collect") if ctx is not None else None,
        cache_key="mapping:line_patterns:LinePatternElement:instances",
    )
    index: Dict[str, Any] = {}
    for e in elements:
        try:
            name = str(getattr(e, "Name", "") or "")
        except Exception:
            name = ""
        if name:
            index[name] = e
    return index


# ---------------------------------------------------------------------------
# Verification (shared by "existing" and "created" paths)
# ---------------------------------------------------------------------------

@dataclass
class VerificationResult:
    ok: bool
    verified_join_hash: Optional[str]
    reason: Optional[str]


def verify_element_join_hash(
    doc: Any, element: Any, requested_join_hash: str, *, domain_policy: Dict[str, Any]
) -> VerificationResult:
    segments = read_segments_from_element(doc, element)
    if segments is None:
        return VerificationResult(ok=False, verified_join_hash=None, reason="element_unreadable")
    verified_join_hash, _join_key, _missing = compute_join_hash_for_segments(
        segments, domain_policy=domain_policy
    )
    if verified_join_hash != requested_join_hash:
        return VerificationResult(
            ok=False, verified_join_hash=verified_join_hash, reason="post_creation_identity_mismatch"
        )
    return VerificationResult(ok=True, verified_join_hash=verified_join_hash, reason=None)


# ---------------------------------------------------------------------------
# Creation (bounded transaction: one requested join_hash per Transaction)
# ---------------------------------------------------------------------------

@dataclass
class CreationResult:
    ok: bool
    element_id: Optional[str]
    verified_join_hash: Optional[str]
    reason: Optional[str]
    element: Any = None


def _build_api_segments(segments: List[Tuple[int, int, float]]) -> Any:
    api_segments = _NetList[LinePatternSegment]()
    for _idx, kind, length in segments:
        kind_name = _LP_SEG_TYPE_NAME.get(kind)
        if kind_name is None:
            raise ValueError("unmapped_segment_kind:{}".format(kind))
        seg_type = getattr(LinePatternSegmentType, kind_name)
        api_segments.Add(LinePatternSegment(seg_type, length))
    return api_segments


def create_and_verify_line_pattern(
    doc: Any,
    name: str,
    segments: List[Tuple[int, int, float]],
    requested_join_hash: str,
    *,
    domain_policy: Dict[str, Any],
) -> CreationResult:
    """Create one LinePatternElement inside its own bounded Transaction, verify it
    reproduces requested_join_hash while the transaction is still open, and only
    then commit. Any exception -- including a failed post-creation verification --
    rolls the transaction back so a bad mapping never partially lands. No bare
    except: every failure path returns an explicit reason.
    """
    if Transaction is None or LinePattern is None or _NetList is None:
        return CreationResult(ok=False, element_id=None, verified_join_hash=None, reason="revit_api_unavailable")

    t = Transaction(doc, TRANSACTION_NAME_PREFIX + name)
    started = False
    try:
        t.Start()
        started = True

        lp = LinePattern(name)
        lp.SetSegments(_build_api_segments(segments))
        created = LinePatternElement.Create(doc, lp)

        verification = verify_element_join_hash(
            doc, created, requested_join_hash, domain_policy=domain_policy
        )
        if not verification.ok:
            t.RollBack()
            return CreationResult(
                ok=False,
                element_id=None,
                verified_join_hash=verification.verified_join_hash,
                reason=verification.reason,
            )

        commit_status = t.Commit()
        if commit_status != TransactionStatus.Committed:
            # Revit can resolve a commit-time failure/warning by rolling the
            # transaction back internally without Commit() raising -- Commit()
            # returns the actual outcome as a TransactionStatus (see
            # reference/revit_lookup/Descriptors/ElementDescriptor.cs:358-360
            # for the same check applied there). Never report success on a
            # transaction that didn't actually land.
            return CreationResult(
                ok=False,
                element_id=None,
                verified_join_hash=verification.verified_join_hash,
                reason="transaction_not_committed:{}".format(safe_str(commit_status)),
            )

        return CreationResult(
            ok=True,
            element_id=safe_str(created.Id.IntegerValue),
            verified_join_hash=verification.verified_join_hash,
            reason=None,
            element=created,
        )
    except Exception as ex:
        try:
            if started and t.GetStatus() == TransactionStatus.Started:
                t.RollBack()
        except Exception:
            pass
        return CreationResult(
            ok=False,
            element_id=None,
            verified_join_hash=None,
            reason="creation_exception:{}:{}".format(ex.__class__.__name__, safe_str(str(ex))),
        )


# ---------------------------------------------------------------------------
# Per-join_hash orchestration
# ---------------------------------------------------------------------------

def resolve_mapping(
    doc: Any,
    join_hash: str,
    request: Dict[str, Any],
    reconstructed: ReconstructedPattern,
    name_rows: List[Dict[str, str]],
    name_index: Dict[str, Any],
    *,
    domain_policy: Optional[Dict[str, Any]] = None,
) -> MappingOutcome:
    """Resolve one requested (domain=line_patterns, join_hash) configuration to a
    final MappingOutcome: reuse a matching existing element, create a new one, or
    block -- never silently modify/replace a nonmatching existing element (a name
    collision with a *different* configuration is resolved via the
    MAP__<name>__<short_join_hash> collision-safe name, never by touching the
    original element).
    """
    if domain_policy is None:
        domain_policy = get_line_patterns_join_key_policy()

    segment_id = str(request.get("segment_id", ""))
    bundle_ids = list(request.get("bundle_ids", []))
    pattern_ids = list(request.get("pattern_ids", []))

    outcome = MappingOutcome(
        join_hash=join_hash,
        segment_id=segment_id,
        requested_join_hash=join_hash,
        bundle_ids=bundle_ids,
        pattern_ids=pattern_ids,
    )

    if reconstructed.blocked:
        outcome.action = ACTION_BLOCKED
        outcome.status = STATUS_BLOCKED
        outcome.reasons = list(reconstructed.reasons)
        return outcome

    observed_name, is_synthetic, naming_reasons = resolve_observed_name(name_rows, join_hash)
    naming_status = STATUS_DEGRADED if is_synthetic else STATUS_OK
    outcome.observed_name = observed_name
    outcome.reasons.extend(reconstructed.reasons)
    outcome.reasons.extend(naming_reasons)

    primary_name, collision_name = build_mapping_name_candidates(observed_name, join_hash)
    base_status = dominant_status([reconstructed.status, naming_status])

    existing_primary = name_index.get(primary_name)
    if existing_primary is not None:
        verification = verify_element_join_hash(doc, existing_primary, join_hash, domain_policy=domain_policy)
        if verification.ok:
            outcome.action = ACTION_EXISTING
            outcome.status = base_status
            outcome.mapping_name = primary_name
            outcome.revit_element_id = safe_str(existing_primary.Id.IntegerValue)
            outcome.verified_join_hash = verification.verified_join_hash or ""
            return outcome

        # Name collision with a DIFFERENT configuration -- never touch
        # existing_primary. Fall through to the collision-safe name.
        outcome.reasons.append("name_collision")
        existing_collision = name_index.get(collision_name)
        if existing_collision is not None:
            verification2 = verify_element_join_hash(
                doc, existing_collision, join_hash, domain_policy=domain_policy
            )
            if verification2.ok:
                outcome.action = ACTION_EXISTING
                outcome.status = STATUS_DEGRADED
                outcome.mapping_name = collision_name
                outcome.revit_element_id = safe_str(existing_collision.Id.IntegerValue)
                outcome.verified_join_hash = verification2.verified_join_hash or ""
                return outcome
            outcome.action = ACTION_BLOCKED
            outcome.status = STATUS_BLOCKED
            outcome.mapping_name = collision_name
            outcome.reasons.append("name_collision_unresolved")
            return outcome

        creation = create_and_verify_line_pattern(
            doc, collision_name, reconstructed.segments, join_hash, domain_policy=domain_policy
        )
        outcome.mapping_name = collision_name
        if creation.ok:
            outcome.action = ACTION_CREATED
            outcome.status = STATUS_DEGRADED
            outcome.revit_element_id = creation.element_id or ""
            outcome.verified_join_hash = creation.verified_join_hash or ""
            if creation.element is not None:
                name_index[collision_name] = creation.element
            return outcome
        outcome.action = ACTION_BLOCKED
        outcome.status = STATUS_BLOCKED
        outcome.reasons.append(creation.reason or "creation_failed")
        outcome.verified_join_hash = creation.verified_join_hash or ""
        return outcome

    creation = create_and_verify_line_pattern(
        doc, primary_name, reconstructed.segments, join_hash, domain_policy=domain_policy
    )
    outcome.mapping_name = primary_name
    if creation.ok:
        outcome.action = ACTION_CREATED
        outcome.status = base_status
        outcome.revit_element_id = creation.element_id or ""
        outcome.verified_join_hash = creation.verified_join_hash or ""
        if creation.element is not None:
            name_index[primary_name] = creation.element
        return outcome

    outcome.action = ACTION_BLOCKED
    outcome.status = STATUS_BLOCKED
    outcome.reasons.append(creation.reason or "creation_failed")
    outcome.verified_join_hash = creation.verified_join_hash or ""
    return outcome

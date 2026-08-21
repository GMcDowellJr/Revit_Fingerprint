# -*- coding: utf-8 -*-
"""
Revit-API half of the fill_patterns mapping utility. Everything here requires
a live Revit document; mapping/fill_pattern_reconstruction.py holds the
pure-Python evidence validation, hashing, and naming logic this module calls
into rather than re-deriving.

Revit API construction path used (confirmed against Autodesk's own
CreateFillPattern SDK sample, Revit 2012 SDK Samples/CreateFillPattern/CS/
FillPatternForm.cs -- the same construction shape is documented current
through at least Revit 2026's FillPattern/FillGrid API surface):

    fp = Autodesk.Revit.DB.FillPattern(name, target, FillPatternHostOrientation.ToHost)
    grid = Autodesk.Revit.DB.FillGrid()
    grid.Origin = Autodesk.Revit.DB.UV(u, v)
    grid.Angle = angle
    grid.Offset = offset
    grid.Shift = shift
    grid.SetSegments(List[float]())  # no per-grid dash pattern captured by this domain
    fp.SetFillGrids(List[FillGrid]([grid, ...]))
    element = Autodesk.Revit.DB.FillPatternElement.Create(doc, fp)

inside a bounded Autodesk.Revit.DB.Transaction per requested (domain, join_hash).

Known evidence gap: FillPatternHostOrientation and each FillGrid's dash
Segments are required Revit API construction parameters that
domains/fill_patterns.py does not capture as identity (neither is a member of
identity_basis.items, join_key, or sig_hash for this domain -- see
docs/fill_pattern_mapping.md). Since there is no evidence to reconstruct them
from, they are set to a fixed default (ToHost orientation, empty/continuous
segments) rather than inferred -- this cannot affect join_hash reproduction
(neither field participates in it), but it DOES mean a created mapping
element's line style along each grid is always continuous, never
reproducing an original pattern's per-line dash/dot styling if it had one.
This is the fill_patterns analogue of line_patterns' own scope boundary
(that utility does not reconstruct LinePatternElement additional properties
beyond segments either).

Follows the repository's existing guarded-import convention for Revit API
symbols (see domains/fill_patterns.py, core/collect.py) so this module can
still be imported (with everything below resolving to None) outside Revit.
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

from mapping.fill_pattern_reconstruction import (
    ACTION_BLOCKED,
    ACTION_CREATED,
    ACTION_EXISTING,
    MappingOutcome,
    ReconstructedGrid,
    ReconstructedPattern,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    TARGET_NAME_BY_DOMAIN,
    build_mapping_name_candidates,
    compute_grids_def_hash,
    compute_join_hash_for_grids,
    dominant_status,
    get_fill_pattern_join_key_policy,
    resolve_observed_name,
)

try:
    from Autodesk.Revit.DB import (
        FillGrid,
        FillPattern,
        FillPatternElement,
        FillPatternHostOrientation,
        FillPatternTarget,
        Transaction,
        TransactionStatus,
        UV,
    )
except Exception:
    FillGrid = None
    FillPattern = None
    FillPatternElement = None
    FillPatternHostOrientation = None
    FillPatternTarget = None
    Transaction = None
    TransactionStatus = None
    UV = None

try:
    import clr  # noqa: F401  (present in the Dynamo CPython3 host; enables the CLR import below)
    from System.Collections.Generic import List as _NetList
except Exception:
    _NetList = None


TRANSACTION_NAME_PREFIX = "Fingerprint Mapping: Create Fill Pattern "

# Fixed default for a required construction parameter this domain does not
# capture as identity -- see module docstring.
_DEFAULT_HOST_ORIENTATION_NAME = "ToHost"

_ORIGIN_KIND_UV = "uv"
_ORIGIN_KIND_XY = "xy"


# ---------------------------------------------------------------------------
# Reading grids back from a live Revit FillPatternElement
# ---------------------------------------------------------------------------

def read_grids_from_element(doc: Any, element: Any) -> Optional[List[ReconstructedGrid]]:
    """Mirror domains/fill_patterns.py::extract_drafting()/extract_model()'s grid
    reading path (GetFillPattern -> GetFillGrids()) for a single already-resolved
    element. Returns None (not a sentinel value) if the pattern/grids could not be
    read at all -- callers treat that as a bounded, explicit failure.
    """
    fp = None
    try:
        fp = element.GetFillPattern()
    except Exception:
        fp = None
    if fp is None:
        return None

    try:
        gc = int(fp.GridCount)
    except Exception:
        return None

    try:
        if hasattr(fp, "GetFillGrids"):
            raw_grids = list(fp.GetFillGrids() or [])
        else:
            raw_grids = []
    except Exception:
        return None

    if len(raw_grids) != gc:
        return None

    grids: List[ReconstructedGrid] = []
    for idx, g in enumerate(raw_grids):
        try:
            angle = float(g.Angle)
            offset = float(g.Offset)
            shift = float(g.Shift)
        except Exception:
            return None

        origin_kind = None
        origin_a = origin_b = None
        try:
            o = g.Origin
            u = getattr(o, "U", None)
            v = getattr(o, "V", None)
            if u is not None and v is not None:
                origin_kind = _ORIGIN_KIND_UV
                origin_a = float(u)
                origin_b = float(v)
        except Exception:
            origin_kind = None

        if origin_kind is None:
            return None

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

    return grids


# ---------------------------------------------------------------------------
# Name index (existing FillPatternElement objects in the current document)
# ---------------------------------------------------------------------------

def build_name_index(doc: Any, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """name -> FillPatternElement, for collision detection."""
    elements = collect_instances(
        doc,
        of_class=FillPatternElement,
        require_unique_id=True,
        cctx=(ctx or {}).get("_collect") if ctx is not None else None,
        cache_key="mapping:fill_patterns:FillPatternElement:instances",
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
    doc: Any,
    element: Any,
    domain_name: str,
    target_name: str,
    requested_join_hash: str,
    *,
    domain_policy: Dict[str, Any],
) -> VerificationResult:
    grids = read_grids_from_element(doc, element)
    if grids is None:
        return VerificationResult(ok=False, verified_join_hash=None, reason="element_unreadable")

    grids_def_hash = compute_grids_def_hash(len(grids), grids)
    verified_join_hash, _join_key, _missing = compute_join_hash_for_grids(
        domain_name, target_name, len(grids), grids_def_hash, domain_policy=domain_policy
    )
    if verified_join_hash != requested_join_hash:
        return VerificationResult(
            ok=False, verified_join_hash=verified_join_hash, reason="post_creation_identity_mismatch"
        )
    return VerificationResult(ok=True, verified_join_hash=verified_join_hash, reason=None)


# ---------------------------------------------------------------------------
# Creation (bounded transaction: one requested (domain, join_hash) per Transaction)
# ---------------------------------------------------------------------------

@dataclass
class CreationResult:
    ok: bool
    element_id: Optional[str]
    verified_join_hash: Optional[str]
    reason: Optional[str]
    element: Any = None


def _build_fill_grid(g: ReconstructedGrid) -> Any:
    grid = FillGrid()
    grid.Origin = UV(g.origin_a, g.origin_b)
    grid.Angle = g.angle
    grid.Offset = g.offset
    grid.Shift = g.shift
    # No per-grid dash pattern captured by this domain -- see module docstring.
    grid.SetSegments(_NetList[float]())
    return grid


def create_and_verify_fill_pattern(
    doc: Any,
    name: str,
    domain_name: str,
    target_name: str,
    grids: List[ReconstructedGrid],
    requested_join_hash: str,
    *,
    domain_policy: Dict[str, Any],
) -> CreationResult:
    """Create one FillPatternElement inside its own bounded Transaction, verify it
    reproduces requested_join_hash while the transaction is still open, and only
    then commit. Any exception -- including a failed post-creation verification --
    rolls the transaction back so a bad mapping never partially lands. No bare
    except: every failure path returns an explicit reason.
    """
    if Transaction is None or FillPattern is None or FillGrid is None or UV is None or _NetList is None:
        return CreationResult(ok=False, element_id=None, verified_join_hash=None, reason="revit_api_unavailable")

    t = Transaction(doc, TRANSACTION_NAME_PREFIX + name)
    started = False
    try:
        t.Start()
        started = True

        target_enum = getattr(FillPatternTarget, target_name)
        host_orientation = getattr(FillPatternHostOrientation, _DEFAULT_HOST_ORIENTATION_NAME)
        fp = FillPattern(name, target_enum, host_orientation)

        api_grids = _NetList[FillGrid]()
        for g in grids:
            api_grids.Add(_build_fill_grid(g))
        fp.SetFillGrids(api_grids)

        created = FillPatternElement.Create(doc, fp)

        verification = verify_element_join_hash(
            doc, created, domain_name, target_name, requested_join_hash, domain_policy=domain_policy
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
            # returns the actual outcome as a TransactionStatus. Never report
            # success on a transaction that didn't actually land (same check
            # as mapping/line_pattern_revit_apply.py::create_and_verify_line_pattern).
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
# Per-(domain, join_hash) orchestration
# ---------------------------------------------------------------------------

def resolve_mapping(
    doc: Any,
    domain_name: str,
    join_hash: str,
    request: Dict[str, Any],
    reconstructed: ReconstructedPattern,
    name_rows: List[Dict[str, str]],
    name_index: Dict[str, Any],
    *,
    domain_policy: Optional[Dict[str, Any]] = None,
) -> MappingOutcome:
    """Resolve one requested (domain, join_hash) configuration to a final
    MappingOutcome: reuse a matching existing element, create a new one, or
    block -- never silently modify/replace a nonmatching existing element.
    """
    if domain_policy is None:
        domain_policy = get_fill_pattern_join_key_policy(domain_name)
    target_name = TARGET_NAME_BY_DOMAIN[domain_name]

    segment_id = str(request.get("segment_id", ""))
    bundle_ids = list(request.get("bundle_ids", []))
    pattern_ids = list(request.get("pattern_ids", []))

    outcome = MappingOutcome(
        domain=domain_name,
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

    def _verify(element):
        return verify_element_join_hash(doc, element, domain_name, target_name, join_hash, domain_policy=domain_policy)

    def _create(name):
        return create_and_verify_fill_pattern(
            doc, name, domain_name, target_name, reconstructed.grids, join_hash, domain_policy=domain_policy
        )

    existing_primary = name_index.get(primary_name)
    if existing_primary is not None:
        verification = _verify(existing_primary)
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
            verification2 = _verify(existing_collision)
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

        creation = _create(collision_name)
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

    creation = _create(primary_name)
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

# -*- coding: utf-8 -*-
"""
View Templates domain extractor - Schedules.

Filters to only process view templates that are ViewSchedule instances or
whose ViewType matches "Schedule".

All extraction logic uses the schedule-specific path (minimal stable surface),
identical to the schedule branch in view_templates.py.

Legacy hash:
- Continues to use existing behavior including sentinel strings where present.
- Uses ctx maps: phase_filter_uid_to_hash.

semantic_v2 hash (additive):
- Uses only semantic-safe fields and upstream semantic_v2 hashes.
- BLOCKS (hash_v2=None) if any required dependency resolution fails.

NOTE: Schedule filter stack + VG signatures are not consistently supported
across Revit API versions. Schedule signature is kept minimal and stable.
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.deps import require_domain, Blocked
from core.collect import collect_instances
from core.canon import (
    canon_str,
    fnum,
    canon_num,
    canon_bool,
    canon_id,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)

from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_MISSING,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    canonicalize_int,
    canonicalize_str,
    build_record_v2,
    make_identity_item,
    make_record_id_from_element,
    serialize_identity_items,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
    phase2_join_hash,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

from core.vg_sig import (
    _canonical_identity_items_from_signature,
    _semantic_keys_from_identity_items,
    _traceability_unknown_items,
)

try:
    from Autodesk.Revit.DB import (
        View,
        ViewSchedule,
        BuiltInParameter,
    )
except Exception as e:
    View = None
    ViewSchedule = None
    BuiltInParameter = None


DOMAIN_NAME = "view_templates_schedules"

# ViewType string fallback for schedule detection (in addition to isinstance check)
_SCHEDULE_VIEW_TYPE = "Schedule"


def _is_schedule_view(v):
    """Return True if this view element is a schedule (ViewSchedule or ViewType='Schedule')."""
    if ViewSchedule is not None:
        try:
            if isinstance(v, ViewSchedule):
                return True
        except Exception:
            pass
    # Fallback: check ViewType string
    try:
        vt_str = safe_str(getattr(v, "ViewType", None)).strip()
        if vt_str == _SCHEDULE_VIEW_TYPE:
            return True
    except Exception:
        pass
    return False


def extract(doc, ctx=None):
    """
    Extract view templates fingerprint - Schedules only.

    Uses the minimal stable schedule surface (no VG/filter stack).

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "records": [],

        # debug counters
        "debug_not_template": 0,
        "debug_missing_name": 0,
        "debug_missing_uid": 0,
        "debug_fail_read": 0,
        "debug_kept": 0,
        "debug_view_type_filtered": 0,

        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},
        # PR6: deterministic degraded signaling
        "debug_view_context_problem": 0,
        "debug_view_context_reasons": {},
        "debug_collect_types_failed": 0,
    }

    ctx_map = ctx or {}

    # CRITICAL DEPENDENCIES - schedules need phase_filters at minimum
    try:
        require_domain(ctx_map.get("_domains", {}), "phase_filters")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    # Get context mappings
    phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
    phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="view_templates_schedules:View:instances",
            )
        )
    except Exception as e:
        info["debug_collect_types_failed"] += 1
        info["_domain_status"] = "degraded"
        info["_domain_diag"] = {
            "degraded_reasons": ["collect_types_failed"],
            "degraded_reason_counts": {"collect_types_failed": 1},
            "error": str(e),
        }
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    per_hashes_v2 = []
    v2_any_blocked = False

    def _v2_block(reason):
        nonlocal v2_any_blocked
        v2_any_blocked = True
        info["debug_v2_blocked"] += 1
        try:
            info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
        except Exception:
            pass

    for v in col:
        # Only process view templates
        try:
            is_template = v.IsTemplate
        except Exception:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # Check that this is a schedule template
        if not _is_schedule_view(v):
            info["debug_view_type_filtered"] += 1
            continue

        # name/uid metadata
        name = canon_str(getattr(v, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = S_MISSING
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(v, "UniqueId", None))
        except Exception:
            uid = None

        if not uid:
            info["debug_missing_uid"] += 1

        # PR6: view-scoped context snapshot (explicit missing vs unreadable)
        try:
            dv = (ctx or {}).get("_doc_view") if ctx is not None else None
            if dv is not None:
                vi = dv.view_info(v, source="HOST")
                if vi.reasons:
                    info["debug_view_context_problem"] += 1
                    for r in vi.reasons:
                        info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
        except Exception:
            info["debug_view_context_problem"] += 1
            info["debug_view_context_reasons"]["view_context_unreadable"] = (
                info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
            )

        # v2 per-template signature (contract semantic)
        v2_ok = True
        sig_v2 = []

        # -----------------------------------------
        # SCHEDULE templates: minimal stable surface
        # -----------------------------------------
        sig = []

        # Template-controlled parameters ("Include" surface)
        try:
            tpl_ids = v.GetTemplateParameterIds() or []
            tpl_bips = set(
                pid.IntegerValue for pid in tpl_ids
                if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
            )
        except Exception:
            tpl_bips = set()

        # Include flags (stable)
        try:
            sig.append(
                "include_phase_filter={}".format(
                    int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
                )
            )
        except Exception:
            sig.append("include_phase_filter=False")

        try:
            sig.append(
                "include_filters={}".format(
                    int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips
                )
            )
        except Exception:
            sig.append("include_filters=False")

        try:
            sig.append(
                "include_vg={}".format(
                    int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips
                )
            )
        except Exception:
            sig.append("include_vg=False")

        try:
            sig.append(
                "include_appearance={}".format(
                    int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips
                )
            )
        except Exception:
            sig.append("include_appearance=False")

        # Phase Filter (reference global phase_filters domain) - legacy
        try:
            include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
        except Exception:
            include_pf = False

        if include_pf:
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p is None:
                    # Schedule templates often don't expose phase filter meaningfully.
                    sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                else:
                    pf_id = p.AsElementId()
                    # Invalid/None phase filter for schedules should be treated as NOT_APPLICABLE
                    if not pf_id or canon_id(pf_id) == S_MISSING:
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_elem = doc.GetElement(pf_id)
                        if pf_elem:
                            pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                            pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                            sig.append("phase_filter={}".format(canon_str(pf_hash)))
                            # v2: require upstream v2 hash when phase filter is present
                            if v2_ok:
                                pf_hash_v2 = None
                                try:
                                    pf_hash_v2 = phase_filter_map_v2.get(pf_uid) if pf_uid else None
                                except Exception:
                                    pf_hash_v2 = None
                                if not pf_hash_v2:
                                    _v2_block("phase_filter_unresolved")
                                    v2_ok = False
                                else:
                                    sig_v2.append("phase_filter_hash={}".format(canon_str(pf_hash_v2)))
                        else:
                            sig.append(f"phase_filter={S_NOT_APPLICABLE}")
            except Exception:
                info["debug_fail_read"] += 1
                sig.append(f"phase_filter={S_UNREADABLE}")

        else:
            sig.append(f"phase_filter={S_MISSING}")

        # NOTE: Schedule filter stack + VG signatures are not consistently supported across versions.
        # We keep schedule signature minimal and stable.

        # Finalize schedule signature
        sig_final = sorted(sig)
        def_hash = make_hash(sig_final)

        # v2 finalize (schedule)
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                sig_v2_final = sorted(set(sig_v2))
                def_hash_v2 = make_hash(sig_v2_final)
                per_hashes_v2.append(def_hash_v2)
            except Exception:
                _v2_block("schedule_finalize_failed")
                v2_ok = False

        # -------------------------
        # record.v2 + Phase-2 (contract-aligned)
        # -------------------------
        identity_items = _canonical_identity_items_from_signature(def_hash, sig_final)
        semantic_keys = _semantic_keys_from_identity_items(identity_items)
        semantic_items = [it for it in identity_items if it.get("k") in set(semantic_keys)]
        sig_hash = make_hash(serialize_identity_items(semantic_items))

        rid_info = make_record_id_from_element(v)
        if rid_info:
            record_id, record_id_alg = rid_info
        else:
            record_id = "eid:{}".format(safe_str(getattr(getattr(v, "Id", None), "IntegerValue", "")))
            record_id_alg = "revit_elementid_v1"

        status = STATUS_OK
        status_reasons = []
        for it in identity_items:
            if it.get("q") != ITEM_Q_OK:
                status = STATUS_DEGRADED
                status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))
        if not v2_ok:
            status = STATUS_BLOCKED
            status_reasons.append("semantic_v2_unresolved_dependency")
            sig_hash = None

        rec = build_record_v2(
            domain=DOMAIN_NAME,
            record_id=record_id,
            record_id_alg=record_id_alg,
            status=status,
            status_reasons=sorted(set(status_reasons)),
            sig_hash=sig_hash,
            identity_items=identity_items,
            required_qs=tuple(it.get("q") for it in identity_items),
            label={
                "display": safe_str(name),
                "quality": "human" if safe_str(name) and safe_str(name) != S_MISSING else "placeholder_missing",
                "provenance": "revit.ViewName",
                "components": {
                    "view_type": safe_str(v.ViewType),
                },
            },
        )

        rec["phase2"] = {
            "schema": "phase2.{}.v1".format(DOMAIN_NAME),
            "grouping_basis": "join_key.join_hash",
            "cosmetic_items": [],
            "coordination_items": [],
            "unknown_items": _traceability_unknown_items(v),
        }

        rec["sig_basis"] = {
            "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
            "hash_alg": "md5_utf8_join_pipe",
            "keys_used": semantic_keys,
        }

        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
        vt_join_key, _vt_missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )
        rec["join_key"] = vt_join_key

        rec["def_hash"] = def_hash
        rec["def_signature"] = sig_final

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

    # Finalize
    info["names"] = sorted(set(names))
    info["count"] = len(records)

    info["records"] = sorted(
        records,
        key=lambda r: (
            safe_str(((r.get("label", {}) or {}).get("display", ""))),
            safe_str(r.get("record_id", "")),
        ),
    )

    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if v2_any_blocked:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("record_id", "")),
            "sig_hash":   safe_str(r.get("sig_hash", "")),
            "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
            "view_type":  safe_str(((r.get("label", {}) or {}).get("components", {}) or {}).get("view_type", "")),
        } for r in recs]
    except Exception:
        info["record_rows"] = []

    # PR6: deterministic degraded signaling into contract
    degraded_reason_counts = {}

    try:
        if int(info.get("debug_missing_uid", 0)) > 0:
            degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
    except Exception:
        pass

    try:
        if int(info.get("debug_fail_read", 0)) > 0:
            degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
    except Exception:
        pass

    try:
        if int(info.get("debug_view_context_problem", 0)) > 0:
            for k, vv in dict(info.get("debug_view_context_reasons", {})).items():
                key = str(k)
                if key.endswith("_not_applicable"):
                    continue
                degraded_reason_counts[key] = int(vv)
    except Exception:
        pass

    try:
        if int(info.get("debug_v2_blocked", 0)) > 0:
            degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
    except Exception:
        pass

    if degraded_reason_counts:
        info["_domain_status"] = "degraded"
        info["_domain_diag"] = {
            "degraded_reasons": sorted(degraded_reason_counts.keys()),
            "degraded_reason_counts": degraded_reason_counts,
        }
    else:
        info["_domain_status"] = "ok"
        info["_domain_diag"] = {}

    return info


# -*- coding: utf-8 -*-
"""View Templates domain family extractor."""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.deps import require_domain, Blocked
from core.collect import collect_instances
from core.canon import canon_str, fnum, canon_num, canon_bool, canon_id, S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
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
from core.phase2 import phase2_sorted_items, phase2_qv_from_legacy_sentinel_str, phase2_join_hash
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.graphic_overrides import extract_projection_graphics, extract_cut_graphics, extract_halftone, extract_transparency
from core.vg_sig import _traceability_unknown_items

try:
    from Autodesk.Revit.DB import View, ViewSchedule, BuiltInParameter
except Exception as e:
    View = None
    ViewSchedule = None
    BuiltInParameter = None

_CTX_TEMPLATES_CACHE_KEY = "_view_templates_cache"

# Canonical cache key for all-View-instances collection.
# All view_templates_* domains use this key so FEC runs once per extraction run.
_VIEW_INSTANCES_CACHE_KEY = "view_instances:View:all"


def _collect_templates(doc, ctx):
    if ctx is not None and _CTX_TEMPLATES_CACHE_KEY in ctx:
        return ctx[_CTX_TEMPLATES_CACHE_KEY]
    col = list(
        collect_instances(
            doc,
            of_class=View,
            require_unique_id=True,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            cache_key=_VIEW_INSTANCES_CACHE_KEY,
        )
    )
    if ctx is not None:
        ctx[_CTX_TEMPLATES_CACHE_KEY] = col
    return col


def _phase2_items_from_def_signature(def_signature):
    """Convert legacy def_signature entries ('k=v') into IdentityItems safely."""
    out = []
    for s in (def_signature or []):
        try:
            ss = safe_str(s)
        except Exception:
            continue
        if "=" not in ss:
            k = "view_template.sig.{}".format(ss)
            out.append(make_identity_item(k, None, "missing"))
            continue
        left, right = ss.split("=", 1)
        k = "view_template.sig.{}".format(safe_str(left).strip())
        rr = safe_str(right).strip()
        if len(rr) >= 2 and ((rr[0] == rr[-1] == "'") or (rr[0] == rr[-1] == '"')):
            rr = rr[1:-1].strip()
        if ("|" in rr) and ("=" in rr):
            parts = [p.strip() for p in rr.split("|") if p.strip()]
            for part in parts:
                if "=" not in part:
                    out.append(make_identity_item("{}.part".format(k), None, "missing"))
                    continue
                subk_raw, subv_raw = part.split("=", 1)
                subk = safe_str(subk_raw).strip()
                subv = safe_str(subv_raw).strip()
                if len(subv) >= 2 and ((subv[0] == subv[-1] == "'") or (subv[0] == subv[-1] == '"')):
                    subv = subv[1:-1].strip()
                sv, sq = phase2_qv_from_legacy_sentinel_str(subv, allow_empty=True)
                out.append(make_identity_item("{}.{}".format(k, subk), sv, sq))
        else:
            v, q = phase2_qv_from_legacy_sentinel_str(rr, allow_empty=True)
            out.append(make_identity_item(k, v, q))
    return phase2_sorted_items(out)


def _canonical_identity_items_from_signature(def_hash, def_signature, override_stack_hash=None):
    items = [make_identity_item("view_template.def_hash", def_hash, ITEM_Q_OK)]
    if override_stack_hash:
        items.append(make_identity_item("view_template.category_overrides_def_hash", override_stack_hash, ITEM_Q_OK))
    items.extend(_phase2_items_from_def_signature(def_signature))
    return phase2_sorted_items(items)


def _semantic_keys_from_identity_items(identity_items):
    keys = sorted({safe_str(it.get("k", "")) for it in (identity_items or []) if isinstance(it.get("k"), str) and safe_str(it.get("k", "")) and safe_str(it.get("k", "")) != "view_template.def_hash"})
    return [k for k in keys if k]


def _build_floor_structural_area_viewtype_set():
    """
    Build the ViewType integer set for floor/structural/area plans.

    Probe-confirmed integers only:
      1 = FloorPlan

    AreaPlan and StructuralPlan are intentionally excluded here because
    117 collides with Section in this Revit version.
    """
    return frozenset({1})


def _build_ceiling_plan_viewtype_set():
    """
    Build the ViewType integer set for ceiling plans.

    Probe-confirmed integers only:
      2 = CeilingPlan
    """
    return frozenset({2})


_FLOOR_STRUCTURAL_AREA_VIEWTYPE_SET = _build_floor_structural_area_viewtype_set()
_CEILING_PLAN_VIEWTYPE_SET = _build_ceiling_plan_viewtype_set()


def extract_floor_structural_area_plans(doc, ctx=None):
    DOMAIN_NAME = "view_templates_floor_structural_area_plans"
    DOMAIN_VIEWTYPE_SET = _FLOOR_STRUCTURAL_AREA_VIEWTYPE_SET
    """
    Extract view templates fingerprint - Floor Plans and Area Plans only.

    Per-template signature: include flags + phase filter hash + filter stack.
    No category-override iteration (VCO domain handles that separately).

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with count, hash_v2, records, record_rows, and debug counters
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

        # v2 surfaces
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
        # PR6: deterministic degraded signaling
        "debug_view_context_problem": 0,
        "debug_view_context_reasons": {},
        "debug_collect_types_failed": 0,
    }

    ctx_map = ctx or {}

    try:
        require_domain(ctx_map.get("_domains", {}), "phase_filters")
        require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
    phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
    view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key=_VIEW_INSTANCES_CACHE_KEY,
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
        try:
            is_template = v.IsTemplate
        except Exception:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # Integer ViewType filter (CPython3 returns int string from enum)
        try:
            vt_int = int(v.ViewType)
        except Exception:
            vt_int = None
        if vt_int not in DOMAIN_VIEWTYPE_SET:
            info["debug_view_type_filtered"] += 1
            continue

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

        # PR6: view-scoped context snapshot
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

        v2_ok = True
        sig_v2 = []
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

        # Common include flags
        try:
            sig.append("include_phase_filter={}".format(int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips))
        except Exception:
            sig.append("include_phase_filter=False")

        try:
            sig.append("include_filters={}".format(int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips))
        except Exception:
            sig.append("include_filters=False")

        try:
            sig.append("include_vg={}".format(int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips))
        except Exception:
            sig.append("include_vg=False")

        try:
            sig.append("include_appearance={}".format(int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips))
        except Exception:
            sig.append("include_appearance=False")

        # Domain-specific: view range (floor/area plans support view depth)
        try:
            sig.append("include_view_range={}".format(int(BuiltInParameter.VIEWER_VOLUME_OF_INTEREST_CROP) in tpl_bips))
        except Exception:
            sig.append("include_view_range=False")

        # Phase Filter (resolved via phase_filters domain)
        try:
            include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
        except Exception:
            include_pf = False

        if include_pf:
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p is None:
                    sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                else:
                    pf_id = p.AsElementId()
                    if not pf_id or canon_id(pf_id) == S_MISSING:
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_elem = doc.GetElement(pf_id)
                        if pf_elem:
                            pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                            pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                            sig.append("phase_filter={}".format(canon_str(pf_hash)))
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

        # Filter stack (order-sensitive)
        try:
            filter_ids = list(v.GetFilters() or []) if hasattr(v, "GetFilters") else []
            sig.append("filter_stack_count={}".format(len(filter_ids)))
            if v2_ok:
                sig_v2.append("vts.filter_stack_count={}".format(canon_str(len(filter_ids))))
        except Exception:
            filter_ids = None
            sig.append("filter_stack_count=<UNREADABLE>")
            if v2_ok:
                _v2_block("filter_stack_unreadable")
                v2_ok = False

        if filter_ids is not None:
            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid = None
                try:
                    fe = doc.GetElement(fid)
                    f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
                except Exception:
                    f_uid = None

                def_sig = view_filter_map.get(f_uid) if f_uid else None

                if def_sig:
                    sig.append("filter[{}].def_sig={}".format(idx3, canon_str(def_sig)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].def_sig_hash={}".format(idx3, canon_str(def_sig)))
                else:
                    sig.append("filter[{}].def_sig=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("view_filter_unresolved")
                        v2_ok = False

                try:
                    vis = bool(v.GetFilterVisibility(fid)) if hasattr(v, "GetFilterVisibility") else None
                except Exception:
                    vis = None

                if vis is None:
                    sig.append("filter[{}].vis=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_visibility_unreadable")
                        v2_ok = False
                else:
                    sig.append("filter[{}].vis={}".format(idx3, int(vis)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].visibility={}".format(idx3, int(vis)))

                try:
                    ogs = v.GetFilterOverrides(fid) if hasattr(v, "GetFilterOverrides") else None
                except Exception:
                    ogs = None

                try:
                    has_ovr = False
                    if ogs is not None:
                        if getattr(ogs, "Halftone", False):
                            has_ovr = True
                        for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
                            vattr = getattr(ogs, attr, None)
                            if vattr is not None and int(vattr) > 0:
                                has_ovr = True
                        for attr in ("ProjectionLinePatternId", "CutLinePatternId"):
                            eid = getattr(ogs, attr, None)
                            if eid is not None and int(getattr(eid, "IntegerValue", 0)) not in (0, -1):
                                has_ovr = True
                    sig.append("filter[{}].ovr={}".format(idx3, int(has_ovr)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].overrides={}".format(idx3, int(has_ovr)))
                except Exception:
                    sig.append("filter[{}].ovr=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_overrides_unreadable")
                        v2_ok = False

        # Finalize signature (deterministic)
        sig_final = sorted(sig)
        def_hash = make_hash(sig_final)

        # v2 finalize
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                sig_v2_final = sorted(set(sig_v2))
                def_hash_v2 = make_hash(sig_v2_final)
                per_hashes_v2.append(def_hash_v2)
            except Exception:
                _v2_block("template_finalize_failed")
                v2_ok = False

        # record.v2 + Phase-2
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

        vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING

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
                    "view_type": vt_raw_str,
                },
            },
        )

        rec["phase2"] = {
            "schema": "phase2.{}.v2".format(DOMAIN_NAME),
            "grouping_basis": "join_key.join_hash",
            "cosmetic_items": [],
            "coordination_items": [
                make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
                make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
            ],
            "unknown_items": _traceability_unknown_items(v),
        }

        rec["sig_basis"] = {
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

    # PR6: deterministic degraded signaling
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

def extract_ceiling_plans(doc, ctx=None):
    DOMAIN_NAME = "view_templates_ceiling_plans"
    DOMAIN_VIEWTYPE_SET = _CEILING_PLAN_VIEWTYPE_SET
    """
    Extract view templates fingerprint - Ceiling Plans only.

    Per-template signature: include flags + phase filter hash + filter stack.
    No category-override iteration (VCO domain handles that separately).

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with count, hash_v2, records, record_rows, and debug counters
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

        # v2 surfaces
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
        # PR6: deterministic degraded signaling
        "debug_view_context_problem": 0,
        "debug_view_context_reasons": {},
        "debug_collect_types_failed": 0,
    }

    ctx_map = ctx or {}

    try:
        require_domain(ctx_map.get("_domains", {}), "phase_filters")
        require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
    phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
    view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key=_VIEW_INSTANCES_CACHE_KEY,
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
        try:
            is_template = v.IsTemplate
        except Exception:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # Integer ViewType filter (CPython3 returns int string from enum)
        try:
            vt_int = int(v.ViewType)
        except Exception:
            vt_int = None
        if vt_int not in DOMAIN_VIEWTYPE_SET:
            info["debug_view_type_filtered"] += 1
            continue

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

        # PR6: view-scoped context snapshot
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

        v2_ok = True
        sig_v2 = []
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

        # Common include flags
        try:
            sig.append("include_phase_filter={}".format(int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips))
        except Exception:
            sig.append("include_phase_filter=False")

        try:
            sig.append("include_filters={}".format(int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips))
        except Exception:
            sig.append("include_filters=False")

        try:
            sig.append("include_vg={}".format(int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips))
        except Exception:
            sig.append("include_vg=False")

        try:
            sig.append("include_appearance={}".format(int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips))
        except Exception:
            sig.append("include_appearance=False")

        # Domain-specific: view range (ceiling plans support view depth)
        try:
            sig.append("include_view_range={}".format(int(BuiltInParameter.VIEWER_VOLUME_OF_INTEREST_CROP) in tpl_bips))
        except Exception:
            sig.append("include_view_range=False")

        # Phase Filter (resolved via phase_filters domain)
        try:
            include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
        except Exception:
            include_pf = False

        if include_pf:
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p is None:
                    sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                else:
                    pf_id = p.AsElementId()
                    if not pf_id or canon_id(pf_id) == S_MISSING:
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_elem = doc.GetElement(pf_id)
                        if pf_elem:
                            pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                            pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                            sig.append("phase_filter={}".format(canon_str(pf_hash)))
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

        # Filter stack (order-sensitive)
        try:
            filter_ids = list(v.GetFilters() or []) if hasattr(v, "GetFilters") else []
            sig.append("filter_stack_count={}".format(len(filter_ids)))
            if v2_ok:
                sig_v2.append("vts.filter_stack_count={}".format(canon_str(len(filter_ids))))
        except Exception:
            filter_ids = None
            sig.append("filter_stack_count=<UNREADABLE>")
            if v2_ok:
                _v2_block("filter_stack_unreadable")
                v2_ok = False

        if filter_ids is not None:
            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid = None
                try:
                    fe = doc.GetElement(fid)
                    f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
                except Exception:
                    f_uid = None

                def_sig = view_filter_map.get(f_uid) if f_uid else None

                if def_sig:
                    sig.append("filter[{}].def_sig={}".format(idx3, canon_str(def_sig)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].def_sig_hash={}".format(idx3, canon_str(def_sig)))
                else:
                    sig.append("filter[{}].def_sig=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("view_filter_unresolved")
                        v2_ok = False

                try:
                    vis = bool(v.GetFilterVisibility(fid)) if hasattr(v, "GetFilterVisibility") else None
                except Exception:
                    vis = None

                if vis is None:
                    sig.append("filter[{}].vis=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_visibility_unreadable")
                        v2_ok = False
                else:
                    sig.append("filter[{}].vis={}".format(idx3, int(vis)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].visibility={}".format(idx3, int(vis)))

                try:
                    ogs = v.GetFilterOverrides(fid) if hasattr(v, "GetFilterOverrides") else None
                except Exception:
                    ogs = None

                try:
                    has_ovr = False
                    if ogs is not None:
                        if getattr(ogs, "Halftone", False):
                            has_ovr = True
                        for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
                            vattr = getattr(ogs, attr, None)
                            if vattr is not None and int(vattr) > 0:
                                has_ovr = True
                        for attr in ("ProjectionLinePatternId", "CutLinePatternId"):
                            eid = getattr(ogs, attr, None)
                            if eid is not None and int(getattr(eid, "IntegerValue", 0)) not in (0, -1):
                                has_ovr = True
                    sig.append("filter[{}].ovr={}".format(idx3, int(has_ovr)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].overrides={}".format(idx3, int(has_ovr)))
                except Exception:
                    sig.append("filter[{}].ovr=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_overrides_unreadable")
                        v2_ok = False

        # Finalize signature (deterministic)
        sig_final = sorted(sig)
        def_hash = make_hash(sig_final)

        # v2 finalize
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                sig_v2_final = sorted(set(sig_v2))
                def_hash_v2 = make_hash(sig_v2_final)
                per_hashes_v2.append(def_hash_v2)
            except Exception:
                _v2_block("template_finalize_failed")
                v2_ok = False

        # record.v2 + Phase-2
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

        vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING

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
                    "view_type": vt_raw_str,
                },
            },
        )

        rec["phase2"] = {
            "schema": "phase2.{}.v2".format(DOMAIN_NAME),
            "grouping_basis": "join_key.join_hash",
            "cosmetic_items": [],
            "coordination_items": [
                make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
                make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
            ],
            "unknown_items": _traceability_unknown_items(v),
        }

        rec["sig_basis"] = {
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

    # PR6: deterministic degraded signaling
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

def _build_elevation_section_detail_viewtype_set():
    """
    Build the ViewType integer set for elevations/sections/detail.

    Probe-confirmed integers:
      3 = Elevation (stable across Revit versions)
      117 = Section in this Revit version (confirmed from corpus templates:
            Building Sections, Wall Sections, Exterior Details, Interior Details)

    Note: int(ViewType.Section) resolves to 117 at runtime in this environment.
    117 was intentionally removed from floor_structural_area_plans (where it was
    incorrectly routing Section templates). It belongs here in elevations.

    The runtime resolution path is kept for forward compatibility with Revit
    versions where Section may have a different integer.
    """
    vt_set = {3, 117}  # Elevation=3, Section=117 (probe-confirmed)
    try:
        from Autodesk.Revit.DB import ViewType
        sec = getattr(ViewType, "Section", None)
        if sec is not None:
            vt_set.add(int(sec))
        det = getattr(ViewType, "Detail", None)
        if det is not None:
            vt_set.add(int(det))
    except Exception:
        pass
    return frozenset(vt_set)


_ELEVATION_SECTION_DETAIL_VIEWTYPE_SET = _build_elevation_section_detail_viewtype_set()


def extract_elevations_sections_detail(doc, ctx=None):
    DOMAIN_NAME = "view_templates_elevations_sections_detail"
    DOMAIN_VIEWTYPE_SET = _ELEVATION_SECTION_DETAIL_VIEWTYPE_SET
    """
    Extract view templates fingerprint - Elevations only.

    Per-template signature: include flags + phase filter hash + filter stack.
    No category-override iteration (VCO domain handles that separately).

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with count, hash_v2, records, record_rows, and debug counters
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

        # v2 surfaces
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
        # PR6: deterministic degraded signaling
        "debug_view_context_problem": 0,
        "debug_view_context_reasons": {},
        "debug_collect_types_failed": 0,
    }

    ctx_map = ctx or {}

    try:
        require_domain(ctx_map.get("_domains", {}), "phase_filters")
        require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
    phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
    view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key=_VIEW_INSTANCES_CACHE_KEY,
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
        try:
            is_template = v.IsTemplate
        except Exception:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # Integer ViewType filter (CPython3 returns int string from enum)
        try:
            vt_int = int(v.ViewType)
        except Exception:
            vt_int = None
        if vt_int not in DOMAIN_VIEWTYPE_SET:
            info["debug_view_type_filtered"] += 1
            continue

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

        # PR6: view-scoped context snapshot
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

        v2_ok = True
        sig_v2 = []
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

        # Common include flags
        try:
            sig.append("include_phase_filter={}".format(int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips))
        except Exception:
            sig.append("include_phase_filter=False")

        try:
            sig.append("include_filters={}".format(int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips))
        except Exception:
            sig.append("include_filters=False")

        try:
            sig.append("include_vg={}".format(int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips))
        except Exception:
            sig.append("include_vg=False")

        try:
            sig.append("include_appearance={}".format(int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips))
        except Exception:
            sig.append("include_appearance=False")

        # Domain-specific: far clip (elevations/sections control far clipping)
        try:
            sig.append("include_far_clip={}".format(int(BuiltInParameter.VIEWER_BOUND_FAR_CLIPPING) in tpl_bips))
        except Exception:
            sig.append("include_far_clip=False")

        # Phase Filter (resolved via phase_filters domain)
        try:
            include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
        except Exception:
            include_pf = False

        if include_pf:
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p is None:
                    sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                else:
                    pf_id = p.AsElementId()
                    if not pf_id or canon_id(pf_id) == S_MISSING:
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_elem = doc.GetElement(pf_id)
                        if pf_elem:
                            pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                            pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                            sig.append("phase_filter={}".format(canon_str(pf_hash)))
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

        # Filter stack (order-sensitive)
        try:
            filter_ids = list(v.GetFilters() or []) if hasattr(v, "GetFilters") else []
            sig.append("filter_stack_count={}".format(len(filter_ids)))
            if v2_ok:
                sig_v2.append("vts.filter_stack_count={}".format(canon_str(len(filter_ids))))
        except Exception:
            filter_ids = None
            sig.append("filter_stack_count=<UNREADABLE>")
            if v2_ok:
                _v2_block("filter_stack_unreadable")
                v2_ok = False

        if filter_ids is not None:
            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid = None
                try:
                    fe = doc.GetElement(fid)
                    f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
                except Exception:
                    f_uid = None

                def_sig = view_filter_map.get(f_uid) if f_uid else None

                if def_sig:
                    sig.append("filter[{}].def_sig={}".format(idx3, canon_str(def_sig)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].def_sig_hash={}".format(idx3, canon_str(def_sig)))
                else:
                    sig.append("filter[{}].def_sig=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("view_filter_unresolved")
                        v2_ok = False

                try:
                    vis = bool(v.GetFilterVisibility(fid)) if hasattr(v, "GetFilterVisibility") else None
                except Exception:
                    vis = None

                if vis is None:
                    sig.append("filter[{}].vis=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_visibility_unreadable")
                        v2_ok = False
                else:
                    sig.append("filter[{}].vis={}".format(idx3, int(vis)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].visibility={}".format(idx3, int(vis)))

                try:
                    ogs = v.GetFilterOverrides(fid) if hasattr(v, "GetFilterOverrides") else None
                except Exception:
                    ogs = None

                try:
                    has_ovr = False
                    if ogs is not None:
                        if getattr(ogs, "Halftone", False):
                            has_ovr = True
                        for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
                            vattr = getattr(ogs, attr, None)
                            if vattr is not None and int(vattr) > 0:
                                has_ovr = True
                        for attr in ("ProjectionLinePatternId", "CutLinePatternId"):
                            eid = getattr(ogs, attr, None)
                            if eid is not None and int(getattr(eid, "IntegerValue", 0)) not in (0, -1):
                                has_ovr = True
                    sig.append("filter[{}].ovr={}".format(idx3, int(has_ovr)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].overrides={}".format(idx3, int(has_ovr)))
                except Exception:
                    sig.append("filter[{}].ovr=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_overrides_unreadable")
                        v2_ok = False

        # Finalize signature (deterministic)
        sig_final = sorted(sig)
        def_hash = make_hash(sig_final)

        # v2 finalize
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                sig_v2_final = sorted(set(sig_v2))
                def_hash_v2 = make_hash(sig_v2_final)
                per_hashes_v2.append(def_hash_v2)
            except Exception:
                _v2_block("template_finalize_failed")
                v2_ok = False

        # record.v2 + Phase-2
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

        vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING

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
                    "view_type": vt_raw_str,
                },
            },
        )

        rec["phase2"] = {
            "schema": "phase2.{}.v2".format(DOMAIN_NAME),
            "grouping_basis": "join_key.join_hash",
            "cosmetic_items": [],
            "coordination_items": [
                make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
                make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
            ],
            "unknown_items": _traceability_unknown_items(v),
        }

        rec["sig_basis"] = {
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

    # PR6: deterministic degraded signaling
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

def _build_renderings_drafting_viewtype_set():
    """
    Build the ViewType integer set for renderings/drafting.

    Probe-confirmed integers only:
      10 = DraftingView

    ThreeD is intentionally excluded because it collides with Section in
    this Revit version, and Rendering is excluded until probe evidence exists.
    """
    return frozenset({10})


_RENDERINGS_DRAFTING_VIEWTYPE_SET = _build_renderings_drafting_viewtype_set()


def extract_renderings_drafting(doc, ctx=None):
    DOMAIN_NAME = "view_templates_renderings_drafting"
    DOMAIN_VIEWTYPE_SET = _RENDERINGS_DRAFTING_VIEWTYPE_SET
    """
    Extract view templates fingerprint - 3D Views and Drafting Views only.

    Per-template signature: include flags + phase filter hash + filter stack.
    No category-override iteration (VCO domain handles that separately).

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with count, hash_v2, records, record_rows, and debug counters
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

        # v2 surfaces
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
        # PR6: deterministic degraded signaling
        "debug_view_context_problem": 0,
        "debug_view_context_reasons": {},
        "debug_collect_types_failed": 0,
    }

    ctx_map = ctx or {}

    try:
        require_domain(ctx_map.get("_domains", {}), "phase_filters")
        require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
    phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
    view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key=_VIEW_INSTANCES_CACHE_KEY,
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
        try:
            is_template = v.IsTemplate
        except Exception:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # Integer ViewType filter (CPython3 returns int string from enum)
        try:
            vt_int = int(v.ViewType)
        except Exception:
            vt_int = None
        if vt_int not in DOMAIN_VIEWTYPE_SET:
            info["debug_view_type_filtered"] += 1
            continue

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

        # PR6: view-scoped context snapshot
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

        v2_ok = True
        sig_v2 = []
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

        # Common include flags
        try:
            sig.append("include_phase_filter={}".format(int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips))
        except Exception:
            sig.append("include_phase_filter=False")

        try:
            sig.append("include_filters={}".format(int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips))
        except Exception:
            sig.append("include_filters=False")

        try:
            sig.append("include_vg={}".format(int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips))
        except Exception:
            sig.append("include_vg=False")

        try:
            sig.append("include_appearance={}".format(int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips))
        except Exception:
            sig.append("include_appearance=False")

        # Phase Filter (resolved via phase_filters domain)
        try:
            include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
        except Exception:
            include_pf = False

        if include_pf:
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p is None:
                    sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                else:
                    pf_id = p.AsElementId()
                    if not pf_id or canon_id(pf_id) == S_MISSING:
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_elem = doc.GetElement(pf_id)
                        if pf_elem:
                            pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                            pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                            sig.append("phase_filter={}".format(canon_str(pf_hash)))
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

        # Filter stack (order-sensitive)
        try:
            filter_ids = list(v.GetFilters() or []) if hasattr(v, "GetFilters") else []
            sig.append("filter_stack_count={}".format(len(filter_ids)))
            if v2_ok:
                sig_v2.append("vts.filter_stack_count={}".format(canon_str(len(filter_ids))))
        except Exception:
            filter_ids = None
            sig.append("filter_stack_count=<UNREADABLE>")
            if v2_ok:
                _v2_block("filter_stack_unreadable")
                v2_ok = False

        if filter_ids is not None:
            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid = None
                try:
                    fe = doc.GetElement(fid)
                    f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
                except Exception:
                    f_uid = None

                def_sig = view_filter_map.get(f_uid) if f_uid else None

                if def_sig:
                    sig.append("filter[{}].def_sig={}".format(idx3, canon_str(def_sig)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].def_sig_hash={}".format(idx3, canon_str(def_sig)))
                else:
                    sig.append("filter[{}].def_sig=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("view_filter_unresolved")
                        v2_ok = False

                try:
                    vis = bool(v.GetFilterVisibility(fid)) if hasattr(v, "GetFilterVisibility") else None
                except Exception:
                    vis = None

                if vis is None:
                    sig.append("filter[{}].vis=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_visibility_unreadable")
                        v2_ok = False
                else:
                    sig.append("filter[{}].vis={}".format(idx3, int(vis)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].visibility={}".format(idx3, int(vis)))

                try:
                    ogs = v.GetFilterOverrides(fid) if hasattr(v, "GetFilterOverrides") else None
                except Exception:
                    ogs = None

                try:
                    has_ovr = False
                    if ogs is not None:
                        if getattr(ogs, "Halftone", False):
                            has_ovr = True
                        for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
                            vattr = getattr(ogs, attr, None)
                            if vattr is not None and int(vattr) > 0:
                                has_ovr = True
                        for attr in ("ProjectionLinePatternId", "CutLinePatternId"):
                            eid = getattr(ogs, attr, None)
                            if eid is not None and int(getattr(eid, "IntegerValue", 0)) not in (0, -1):
                                has_ovr = True
                    sig.append("filter[{}].ovr={}".format(idx3, int(has_ovr)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].overrides={}".format(idx3, int(has_ovr)))
                except Exception:
                    sig.append("filter[{}].ovr=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_overrides_unreadable")
                        v2_ok = False

        # Finalize signature (deterministic)
        sig_final = sorted(sig)
        def_hash = make_hash(sig_final)

        # v2 finalize
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                sig_v2_final = sorted(set(sig_v2))
                def_hash_v2 = make_hash(sig_v2_final)
                per_hashes_v2.append(def_hash_v2)
            except Exception:
                _v2_block("template_finalize_failed")
                v2_ok = False

        # record.v2 + Phase-2
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

        vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING

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
                    "view_type": vt_raw_str,
                },
            },
        )

        rec["phase2"] = {
            "schema": "phase2.{}.v2".format(DOMAIN_NAME),
            "grouping_basis": "join_key.join_hash",
            "cosmetic_items": [],
            "coordination_items": [
                make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
                make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
            ],
            "unknown_items": _traceability_unknown_items(v),
        }

        rec["sig_basis"] = {
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

    # PR6: deterministic degraded signaling
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


def extract_schedules(doc, ctx=None):
    DOMAIN_NAME = "view_templates_schedules"
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
        "debug_v2_blocked": False,
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
                cache_key=_VIEW_INSTANCES_CACHE_KEY,
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
            "schema": "phase2.{}.v2".format(DOMAIN_NAME),
            "grouping_basis": "join_key.join_hash",
            "cosmetic_items": [],
            "coordination_items": [],
            "unknown_items": _traceability_unknown_items(v),
        }

        rec["sig_basis"] = {
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

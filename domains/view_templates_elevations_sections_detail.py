# -*- coding: utf-8 -*-
"""
View Templates domain extractor - Elevations, Sections, Detail, and ThreeD views.

Filters to only process view templates whose ViewType is one of:
  Elevation, Section, Detail, ThreeD

All extraction logic is identical to view_templates.py except for the ViewType filter
applied at the start of the per-template loop.

Legacy hash:
- Continues to use existing behavior including sentinel strings where present.
- Uses ctx maps: filter_uid_to_hash, phase_filter_uid_to_hash.

semantic_v2 hash (additive):
- Uses only semantic-safe fields and upstream semantic_v2 hashes.
- BLOCKS (hash_v2=None) if any required dependency resolution fails.
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

from core.graphic_overrides import (
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
    extract_transparency,
)

from core.vg_sig import (
    _phase2_items_from_def_signature,
    _canonical_identity_items_from_signature,
    _semantic_keys_from_identity_items,
    _traceability_unknown_items,
    _compute_delta_items,
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


DOMAIN_NAME = "view_templates_elevations_sections_detail"

# ViewType strings handled by this domain extractor
_HANDLED_VIEW_TYPES = frozenset({"Elevation", "Section", "Detail", "ThreeD"})


def extract(doc, ctx=None):
    """
    Extract view templates fingerprint - Elevations, Sections, Detail, ThreeD only.

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

        # NEW counters for referential architecture
        "debug_category_overrides_found": 0,
        "debug_category_no_baseline": 0,
        "debug_category_no_override": 0,
        "debug_baseline_map_size": 0,
    }

    ctx_map = ctx or {}

    # CRITICAL DEPENDENCIES - view templates cannot work without these
    try:
        require_domain(ctx_map.get("_domains", {}), "object_styles_model")
        require_domain(ctx_map.get("_domains", {}), "phase_filters")
        require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
        require_domain(ctx_map.get("_domains", {}), "line_patterns")
        require_domain(ctx_map.get("_domains", {}), "fill_patterns_drafting")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
        info["count"] = 0
        info["records"] = []
        info["hash_v2"] = None
        return info

    # Get baseline maps
    baseline_map = ctx_map.get("object_styles_category_to_sig_hash", {})
    baseline_records = ctx_map.get("object_styles_records", {})

    # Get override map
    override_map = ctx_map.get("view_category_overrides_sig_hash", {})

    # Get existing maps
    phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
    view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})
    line_pattern_map_v2 = ctx_map.get("line_pattern_uid_to_hash", {})
    fill_pattern_map_v2 = ctx_map.get("fill_pattern_uid_to_hash", {})

    info["debug_baseline_map_size"] = len(baseline_map)
    if not baseline_map:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"]["missing_baseline_map"] = "object_styles not run"

    # Get context mappings (may be None if global domains not run)
    phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})

    debug_vg_details = bool(ctx_map.get("debug_vg_details", False))

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="view_templates_elevations_sections_detail:View:instances",
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

        # Check ViewType filter - only process Elevation, Section, Detail, ThreeD
        try:
            vt_str = safe_str(getattr(v, "ViewType", None)).strip()
        except Exception:
            vt_str = ""
        if vt_str not in _HANDLED_VIEW_TYPES:
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
        # NON-SCHEDULE templates (Elevation/Section/Detail/ThreeD are never schedules)
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

        # Phase Filter (reference global phase_filters domain)
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
            sig.append("phase_filter={S_MISSING}")

        # Visibility/Graphics (VG) signature
        try:
            cats = doc.Settings.Categories
        except Exception:
            cats = None

        category_override_items = []
        vg_records = []
        override_idx = 0
        override_stack_hash = None

        if cats:
            try:
                categories = list(cats)
            except Exception:
                categories = []

            for category in categories:
                category_name = canon_str(getattr(category, "Name", None))
                category_path = "{}|self".format(category_name)

                baseline_sig_hash = baseline_map.get(category_path)
                if not baseline_sig_hash:
                    info["debug_category_no_baseline"] += 1
                    continue

                try:
                    ogs = v.GetCategoryOverrides(category.Id)
                except Exception:
                    continue

                if not ogs:
                    continue

                override_items = []
                override_items.extend(extract_projection_graphics(doc, ogs, ctx_map, "vco.projection"))
                override_items.extend(extract_cut_graphics(doc, ogs, ctx_map, "vco.cut"))
                override_items.extend(extract_halftone(ogs, "vco.halftone"))
                override_items.extend(extract_transparency(ogs, "vco.transparency"))

                baseline_record = baseline_records.get(baseline_sig_hash)
                if not baseline_record:
                    info["debug_category_no_baseline"] += 1
                    continue

                delta_items = _compute_delta_items(override_items, baseline_record)

                if not delta_items:
                    info["debug_category_no_override"] += 1
                    continue

                override_identity = [
                    make_identity_item("vco.baseline_sig_hash", baseline_sig_hash, ITEM_Q_OK),
                    *delta_items,
                ]
                override_sig_hash = make_hash(serialize_identity_items(override_identity))

                info["debug_category_overrides_found"] += 1

                category_override_items.append(
                    make_identity_item(
                        "view_template.category_overrides[{0:03d}].category_path".format(override_idx),
                        category_path,
                        ITEM_Q_OK,
                    )
                )
                category_override_items.append(
                    make_identity_item(
                        "view_template.category_overrides[{0:03d}].baseline_sig_hash".format(override_idx),
                        baseline_sig_hash,
                        ITEM_Q_OK,
                    )
                )
                category_override_items.append(
                    make_identity_item(
                        "view_template.category_overrides[{0:03d}].override_sig_hash".format(override_idx),
                        override_sig_hash,
                        ITEM_Q_OK,
                    )
                )

                if debug_vg_details:
                    vg_records.append(
                        "category_path={}|baseline_sig_hash={}|override_sig_hash={}".format(
                            category_path,
                            baseline_sig_hash,
                            override_sig_hash,
                        )
                    )

                override_idx += 1

        if category_override_items:
            override_stack_hash = make_hash(serialize_identity_items(category_override_items))
            sig.append("category_overrides_def_hash={}".format(canon_str(override_stack_hash)))
            sig.append("category_overrides_count={}".format(canon_str(override_idx)))
        else:
            sig.append("category_overrides_count=0")

        # Appearance (placeholder surface; legacy keeps minimal)
        try:
            include_app = int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips
        except Exception:
            include_app = False
        sig.append("appearance_included={}".format(int(bool(include_app))))

        # Template filter stack identity (order matters)
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
            vf_map = view_filter_map

            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid = None
                try:
                    fe = doc.GetElement(fid)
                    f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
                except Exception:
                    f_uid = None

                def_sig = vf_map.get(f_uid) if f_uid else None

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

        # -------------------------
        # record.v2 + Phase-2 (contract-aligned)
        # -------------------------
        identity_items = _canonical_identity_items_from_signature(def_hash, sig_final, override_stack_hash)
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

        # Optional VG debug
        if debug_vg_details:
            try:
                rec["vg_debug"] = list(vg_records)
            except Exception:
                pass

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

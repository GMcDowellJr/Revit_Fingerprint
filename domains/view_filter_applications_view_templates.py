# -*- coding: utf-8 -*-
"""domains/view_filter_applications_view_templates.py

record.v2 domain: view_filter_applications_view_templates

Extract the ordered view filter application stacks from View Templates.

Hard constraints:
  - No sentinel literals in identity values.
  - Applications reference view_filter_definitions records by sig_hash.
  - Missing/unresolvable dependencies surface via q + dependency.* reasons.
  - Minima (per registry): block if required keys are not ok.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from Autodesk.Revit.DB import ElementId, View

from core.collect import collect_instances
from core.hashing import make_hash, safe_str
from core.record_v2 import (
    ITEM_Q_MISSING,
    ITEM_Q_OK,
    ITEM_Q_UNSUPPORTED,
    ITEM_Q_UNREADABLE,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    build_record_v2,
    canonicalize_bool,
    canonicalize_int,
    canonicalize_str,
    make_identity_item,
    serialize_identity_items,
)


def _is_schedule_view(view) -> bool:
    try:
        vt = getattr(view, "ViewType", None)
        return safe_str(vt) == "Schedule"
    except Exception:
        return False


def _has_any_override(ogs) -> Tuple[Optional[str], str]:
    """Return (v, q) for vfa.stack[i].overrides as a bool-like string."""
    if ogs is None:
        return "false", ITEM_Q_OK

    try:
        # Conservative check across common override knobs.
        # Any readable non-default value marks overrides=True.
        # Note: This intentionally does NOT attempt to hash the override content.

        for attr in ("Halftone",):
            try:
                v = getattr(ogs, attr, None)
                if v is True:
                    return "true", ITEM_Q_OK
            except Exception:
                return None, ITEM_Q_UNREADABLE

        for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
            try:
                v = getattr(ogs, attr, None)
                if v is not None and int(v) > 0:
                    return "true", ITEM_Q_OK
            except Exception:
                return None, ITEM_Q_UNREADABLE

        for attr in (
            "ProjectionLinePatternId",
            "CutLinePatternId",
            "SurfaceForegroundPatternId",
            "SurfaceBackgroundPatternId",
            "CutForegroundPatternId",
            "CutBackgroundPatternId",
        ):
            try:
                eid = getattr(ogs, attr, None)
                if isinstance(eid, ElementId):
                    iv = int(eid.IntegerValue)
                    if iv != -1 and iv != 0:
                        return "true", ITEM_Q_OK
            except Exception:
                return None, ITEM_Q_UNREADABLE

        for attr in (
            "ProjectionLineColor",
            "CutLineColor",
            "SurfaceForegroundPatternColor",
            "SurfaceBackgroundPatternColor",
            "CutForegroundPatternColor",
            "CutBackgroundPatternColor",
        ):
            try:
                c = getattr(ogs, attr, None)
                if c is not None:
                    r = getattr(c, "Red", 0)
                    g = getattr(c, "Green", 0)
                    b = getattr(c, "Blue", 0)
                    if int(r) != 0 or int(g) != 0 or int(b) != 0:
                        return "true", ITEM_Q_OK
            except Exception:
                return None, ITEM_Q_UNREADABLE

        return "false", ITEM_Q_OK
    except Exception:
        return None, ITEM_Q_UNREADABLE


def extract(doc, ctx=None):
    """Extract record.v2 view filter application stacks for view templates."""

    result: Dict[str, Any] = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "hash": None,  # legacy hash intentionally not defined
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    def_map = {}
    if isinstance(ctx, dict):
        def_map = ctx.get("view_filter_uid_to_sig_hash_v2", {}) or {}

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=False,
                predicate=lambda v: bool(getattr(v, "IsTemplate", False)),
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="view_filter_applications_view_templates:View:templates",
            )
        )
    except Exception as e:
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = {"collect_failed": safe_str(str(e))}
        return result

    result["raw_count"] = len(col)

    v2_records: List[Dict[str, Any]] = []
    v2_sig_hashes: List[str] = []
    v2_block_reasons: Dict[str, Any] = {}

    for v in col:
        try:
            elem_id = safe_str(getattr(getattr(v, "Id", None), "IntegerValue", ""))
        except Exception:
            elem_id = ""
        record_id = elem_id or safe_str(id(v))

        try:
            name_raw = getattr(v, "Name", None)
        except Exception:
            name_raw = None

        name_v, _name_q = canonicalize_str(name_raw)
        label_display = "View Template Filter Stack ({})".format(name_v or "(unnamed)")
        label = {
            "display": label_display,
            "quality": ("human" if name_v is not None else "placeholder_missing"),
            "provenance": "revit.ViewName",
            "components": {"name": safe_str(name_v or "")},
        }

        identity_items: List[Dict[str, Any]] = []
        status_reasons: List[str] = []

        # Required: vfa.template_uid_or_namekey
        try:
            uid_raw = getattr(v, "UniqueId", None)
        except Exception:
            uid_raw = None
        uid_v, uid_q = canonicalize_str(uid_raw)
        if uid_v is None:
            uid_v, uid_q = canonicalize_str(name_raw)
        identity_items.append(make_identity_item("vfa.template_uid_or_namekey", uid_v, uid_q))

        # Filter stack
        filter_ids: Optional[List[ElementId]] = None
        stack_ok = True

        if _is_schedule_view(v):
            filter_ids = []
        else:
            try:
                if hasattr(v, "GetFilters"):
                    filter_ids = list(v.GetFilters() or [])
                else:
                    filter_ids = []
            except Exception:
                stack_ok = False
                filter_ids = None

        # Required: vfa.filter_stack_count
        if stack_ok and filter_ids is not None:
            sc_v, sc_q = canonicalize_int(len(filter_ids))
        else:
            sc_v, sc_q = (None, ITEM_Q_UNREADABLE)
            status_reasons.append("filter_stack.unreadable")
        identity_items.append(make_identity_item("vfa.filter_stack_count", sc_v, sc_q))

        if stack_ok and filter_ids is not None:
            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                f_uid_v, f_uid_q = (None, ITEM_Q_UNREADABLE)
                try:
                    fe = doc.GetElement(fid)
                    f_uid_v, f_uid_q = canonicalize_str(getattr(fe, "UniqueId", None) if fe is not None else None)
                except Exception:
                    f_uid_v, f_uid_q = (None, ITEM_Q_UNREADABLE)

                sig_v, sig_q = (None, ITEM_Q_UNREADABLE)
                if f_uid_v is None or f_uid_q != ITEM_Q_OK:
                    status_reasons.append("dependency.unreadable:view_filter_definitions")
                else:
                    mapped = def_map.get(f_uid_v)
                    if mapped:
                        sig_v, sig_q = (safe_str(mapped), ITEM_Q_OK)
                    else:
                        status_reasons.append("dependency.unresolved:view_filter_definitions")

                identity_items.append(make_identity_item(f"vfa.stack[{idx3}].filter_sig_hash", sig_v, sig_q))

                vis_v, vis_q = (None, ITEM_Q_UNREADABLE)
                try:
                    if hasattr(v, "GetFilterVisibility"):
                        vb = bool(v.GetFilterVisibility(fid))
                        vis_v, vis_q = canonicalize_bool(vb)
                    else:
                        vis_v, vis_q = (None, ITEM_Q_UNSUPPORTED)
                except Exception:
                    vis_v, vis_q = (None, ITEM_Q_UNREADABLE)
                identity_items.append(make_identity_item(f"vfa.stack[{idx3}].visibility", vis_v, vis_q))

                ogs = None
                try:
                    if hasattr(v, "GetFilterOverrides"):
                        ogs = v.GetFilterOverrides(fid)
                except Exception:
                    ogs = None
                ov_v, ov_q = _has_any_override(ogs)
                identity_items.append(make_identity_item(f"vfa.stack[{idx3}].overrides", ov_v, ov_q))

        items_sorted = sorted(identity_items, key=lambda it: str(it.get("k", "")))

        required_keys = ["vfa.template_uid_or_namekey", "vfa.filter_stack_count"]
        item_by_k = {it.get("k"): it for it in items_sorted}
        required_qs = [safe_str(item_by_k.get(rk, {}).get("q", ITEM_Q_MISSING)) for rk in required_keys]
        blocked = any(q != ITEM_Q_OK for q in required_qs)

        any_incomplete = False
        for it in items_sorted:
            q = it.get("q")
            if q != ITEM_Q_OK:
                any_incomplete = True
                k = it.get("k")
                status_reasons.append("identity.incomplete:{}:{}".format(q, k))

        if blocked:
            rec = build_record_v2(
                domain="view_filter_applications_view_templates",
                record_id=record_id,
                status=STATUS_BLOCKED,
                status_reasons=sorted(set(status_reasons)) or ["minima.required_not_ok"],
                sig_hash=None,
                identity_items=items_sorted,
                required_qs=(),
                label=label,
            )
            v2_block_reasons[f"record_blocked:{record_id}"] = True
        else:
            status = STATUS_DEGRADED if any_incomplete else STATUS_OK
            sig_hash = make_hash(serialize_identity_items(items_sorted))
            rec = build_record_v2(
                domain="view_filter_applications_view_templates",
                record_id=record_id,
                status=status,
                status_reasons=sorted(set(status_reasons)),
                sig_hash=sig_hash,
                identity_items=items_sorted,
                required_qs=required_qs,
                label=label,
            )
            v2_sig_hashes.append(sig_hash)

        v2_records.append(rec)

    result["records"] = sorted(v2_records, key=lambda r: safe_str(r.get("record_id", "")))
    result["count"] = len(result["records"])

    if v2_sig_hashes and not v2_block_reasons:
        result["hash_v2"] = make_hash(sorted(v2_sig_hashes))
        result["debug_v2_blocked"] = False
        result["debug_v2_block_reasons"] = {}
    else:
        result["hash_v2"] = None
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = v2_block_reasons or {"no_nonblocked_records": True}

    return result

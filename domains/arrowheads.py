# -*- coding: utf-8 -*-
"""
Arrowheads domain extractor.

Purpose:
- Export arrowhead (tick mark) definitions as record.v2.
- Provide ctx mapping for other domains (dimension_types, text_types, future domains)
  to reference arrowhead definition signatures by ElementId.

Identity policy:
- Definition-based only (NO UID in identity).
- Name is label-only (may be null for built-in/system arrowheads).
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import canon_str, fnum, S_MISSING, S_UNREADABLE
from core.rows import (
    first_param,
    _as_string,
    _as_double,
    _as_int,
    format_len_inches,
    get_type_display_name,
)
from core.phase2 import (
    phase2_sorted_items,
)
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    canonicalize_str_allow_empty,
    canonicalize_int,
    canonicalize_float,
    canonicalize_bool,
    canonicalize_enum,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import ElementType, FilteredElementCollector
except ImportError:
    ElementType = None
    FilteredElementCollector = None


def _fmt_deg_from_rad(rad):
    if rad is None:
        return None
    try:
        return float(rad) * (180.0 / 3.141592653589793)
    except Exception:
        return None


def _canon_yesno_bool(v):
    # Normalize typical Revit/Dynamo Yes/No integer-ish params.
    if v is None:
        return None
    try:
        iv = int(v)
        return bool(iv)
    except Exception:
        try:
            sv = str(v).strip().lower()
            if sv in ("1", "yes", "true"):
                return True
            if sv in ("0", "no", "false"):
                return False
        except Exception:
            pass
    return None


def _is_arrowhead_type(doc, t):
    """
    Best-effort classifier:
    Arrowhead types reliably expose parameters:
      - "Arrow Style"
      - "Tick Size"
    We only treat types with BOTH params as arrowheads.
    """
    try:
        if t is None:
            return False
        p_style = first_param(t, ui_names=["Arrow Style"])
        p_tick = first_param(t, ui_names=["Tick Size"])
        return (p_style is not None) and (p_tick is not None)
    except Exception:
        return False


def extract(doc, ctx=None):
    info = {
        "count": 0,
        "raw_count": 0,
        "records": [],
        "signature_hashes_v2": [],
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    if ctx is None:
        ctx = {}

    # Mapping for downstream domains:
    # key: stringified ElementId.IntegerValue
    # val: {"sig_hash": <str|None>, "label": <str>, "name": <str|None>}
    ctx.setdefault("arrowheads_by_type_id", {})

    if ElementType is None or FilteredElementCollector is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"api_unreachable": True}
        return info

    # Collect *all* ElementTypes, then filter by the presence of arrowhead params.
    try:
        all_types = list(FilteredElementCollector(doc).OfClass(ElementType).ToElements())
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    arrow_types = []
    for t in all_types:
        if _is_arrowhead_type(doc, t):
            arrow_types.append(t)

    v2_records = []
    v2_sig_hashes = []
    v2_blocked = False
    v2_reasons = {}

    def _v2_block(reason_key):
        nonlocal v2_blocked
        v2_blocked = True
        v2_reasons[str(reason_key)] = True

    for t in arrow_types:
        # Stable ID
        try:
            type_id_int = getattr(getattr(t, "Id", None), "IntegerValue", None)
        except Exception:
            type_id_int = None
        type_id_s = safe_str(type_id_int)

        # Label/name (may be null for system/built-in)
        try:
            nm = get_type_display_name(t)
            nm = canon_str(nm) if nm else None
            if nm in (S_MISSING, S_UNREADABLE, "", None):
                nm = None
        except Exception:
            nm = None

        # Parameters (best-effort, explicit q states)
        # Arrow Style (enum/int)
        p_style = first_param(t, ui_names=["Arrow Style"])
        style_raw = _as_int(p_style)
        style_v, style_q = canonicalize_enum(style_raw if style_raw is not None else None)

        # Tick Size (length) -> inches string
        p_tick = first_param(t, ui_names=["Tick Size"])
        tick_ft = _as_double(p_tick)
        tick_in = fnum(format_len_inches(tick_ft), 6)
        tick_in_v, tick_in_q = canonicalize_str_allow_empty(tick_in)

        # Width Angle (rad) -> degrees float
        p_ang = first_param(t, ui_names=["Width Angle"])
        ang_rad = _as_double(p_ang)
        ang_deg = _fmt_deg_from_rad(ang_rad)
        ang_deg_v, ang_deg_q = canonicalize_float(ang_deg)

        # Fill Tick (Yes/No)
        p_fill = first_param(t, ui_names=["Fill Tick"])
        fill_raw = _as_int(p_fill)
        fill_bool = _canon_yesno_bool(fill_raw)
        fill_v, fill_q = canonicalize_bool(fill_bool)

        # Arrow Closed (Yes/No)
        p_closed = first_param(t, ui_names=["Arrow Closed"])
        closed_raw = _as_int(p_closed)
        closed_bool = _canon_yesno_bool(closed_raw)
        closed_v, closed_q = canonicalize_bool(closed_bool)

        # Tick Mark Centered (Yes/No)
        p_center = first_param(t, ui_names=["Tick Mark Centered"])
        center_raw = _as_int(p_center)
        center_bool = _canon_yesno_bool(center_raw)
        center_v, center_q = canonicalize_bool(center_bool)

        # Heavy End Pen Weight (int)
        p_pen = first_param(t, ui_names=["Heavy End Pen Weight"])
        pen_raw = _as_int(p_pen)
        pen_v, pen_q = canonicalize_int(pen_raw)

        identity_items = [
            make_identity_item("arrowhead.style", style_v, style_q),
            make_identity_item("arrowhead.tick_size_in", tick_in_v, tick_in_q),
            make_identity_item("arrowhead.width_angle_deg", ang_deg_v, ang_deg_q),
            make_identity_item("arrowhead.fill_tick", fill_v, fill_q),
            make_identity_item("arrowhead.arrow_closed", closed_v, closed_q),
            make_identity_item("arrowhead.tick_mark_centered", center_v, center_q),
            make_identity_item("arrowhead.heavy_end_pen_weight", pen_v, pen_q),
        ]
        identity_items = sorted(identity_items, key=lambda it: it.get("k", ""))

        # Required qs: style + tick_size must be OK (classifier depends on their presence)
        required_qs = [style_q, tick_in_q]
        if any(q != ITEM_Q_OK for q in required_qs):
            _v2_block("required_identity_not_ok")

        # Determine record status
        any_incomplete = False
        status_reasons = []
        for it in identity_items:
            q = it.get("q")
            if q == ITEM_Q_OK:
                continue
            # Allow missing/unreadable to degrade (not block) unless it's in required_qs
            if q in (ITEM_Q_MISSING, ITEM_Q_UNREADABLE):
                any_incomplete = True
                status_reasons.append("identity.incomplete:{}:{}".format(q, it.get("k")))
            else:
                any_incomplete = True
                status_reasons.append("identity.incomplete:{}:{}".format(q, it.get("k")))

        status = STATUS_OK if not any_incomplete else STATUS_DEGRADED
        sig_hash = None

        if v2_blocked:
            status = STATUS_BLOCKED
            status_reasons = sorted(set(status_reasons)) or ["minima.required_not_ok"]
        else:
            preimage = serialize_identity_items(identity_items)
            sig_hash = make_hash(preimage)
            v2_sig_hashes.append(sig_hash)

        # Label is not identity; keep human if possible.
        label_display = nm if nm else "Arrowhead"
        label_quality = "human" if nm else "placeholder_missing"

        label = {
            "display": safe_str(label_display),
            "quality": label_quality,
            "provenance": "revit.ElementType.Name_or_params",
            "components": {
                "type_id": type_id_s,
                "type_name": safe_str(nm) if nm else "",
            },
        }

        record_id = "arrowhead_type_id:{}".format(type_id_s) if type_id_s else "arrowhead"

        rec_v2 = build_record_v2(
            domain="arrowheads",
            record_id=record_id,
            status=status,
            status_reasons=sorted(set(status_reasons)),
            sig_hash=sig_hash,
            identity_items=identity_items,
            required_qs=tuple(required_qs),
            label=label,
            debug={
                "name_may_be_null_for_system_types": True,
                "uid_excluded_from_sig": True,
            },
        )

        # Phase-2 (join-key candidates live ONLY here; identity remains authoritative)
        semantic_items = list(identity_items)  # flat parametric domain: candidates == curated scalar params
        cosmetic_items = []
        unknown_items = []

        rec_v2["phase2"] = {
            "schema": "phase2.arrowheads.v1",
            "grouping_basis": "phase2.hypothesis",
            "semantic_items": phase2_sorted_items(semantic_items),
            "cosmetic_items": phase2_sorted_items(cosmetic_items),
            "coordination_items": phase2_sorted_items([]),
            "unknown_items": phase2_sorted_items(unknown_items),
        }

        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "arrowheads")
        rec_v2["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=phase2_sorted_items(semantic_items),
        )

        v2_records.append(rec_v2)

        # ctx mapping (downstream lookup by ElementId.IntegerValue)
        if type_id_s:
            ctx["arrowheads_by_type_id"][type_id_s] = {
                "sig_hash": sig_hash,
                "label": label_display,
                "name": nm,
            }

    info["records"] = v2_records
    info["count"] = len(v2_records)

    info["signature_hashes_v2"] = sorted([h for h in v2_sig_hashes if h])
    if info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
        info["debug_v2_blocked"] = False
        info["debug_v2_block_reasons"] = {}
    else:
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = v2_reasons if v2_reasons else {"no_arrowheads_or_all_blocked": True}

    return info

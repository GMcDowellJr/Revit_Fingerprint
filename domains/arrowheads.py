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
    canonicalize_str,
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
from core.collect import collect_types
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import ElementType
except ImportError:
    ElementType = None


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

def _as_value_string(param):
    """Best-effort Revit Parameter.AsValueString() accessor.

    Needed for integer/enum parameters where AsString() is typically None.
    """
    if param is None:
        return None
    try:
        s = param.AsValueString()
        if s is None:
            return None
        s = str(s).strip()
        return s if s else None
    except Exception:
        return None

def _get_arrowhead_style(style_raw, style_q):
    """Return a canonical arrowhead style label.

    Prefer the Revit display string from AsValueString() when available.
    If we only have an int enum code, map known observed codes; otherwise "Other".
    """
    if style_q != ITEM_Q_OK:
        return None, style_q

    if style_raw is None:
        return None, ITEM_Q_MISSING

    # If display string exists (common case), it's already the canonical style label.
    try:
        if isinstance(style_raw, str):
            s = style_raw.strip()
            if s:
                return s, ITEM_Q_OK
    except Exception:
        pass

    # Otherwise treat as enum int (best-effort mapping based on probe evidence)
    try:
        iv = int(style_raw)
    except Exception:
        return None, ITEM_Q_UNREADABLE

    # Observed mapping from probe inventory (Arrow Style raw -> display)
    # 0 Diagonal, 3 Dot, 7 Heavy end tick mark, 8 Arrow, 9 Datum triangle,
    # 10 Box, 11 Elevation Target, 12 Loop
    known = {
        0: "Diagonal",
        3: "Dot",
        7: "Heavy end tick mark",
        8: "Arrow",
        9: "Datum triangle",
        10: "Box",
        11: "Elevation Target",
        12: "Loop",
    }

    if iv in known:
        return known[iv], ITEM_Q_OK

    return "Other", ITEM_Q_OK


def _build_common_identity_items(
    *,
    style_v,
    style_q,
    tick_in_v,
    tick_in_q,
):
    return [
        make_identity_item("arrowhead.style", style_v, style_q),
        make_identity_item("arrowhead.tick_size_in", tick_in_v, tick_in_q),
    ]


def _build_arrow_identity_items(
    *,
    width_angle_v,
    width_angle_q,
    fill_v,
    fill_q,
    closed_v,
    closed_q,
):
    return [
        make_identity_item("arrowhead.width_angle_deg", width_angle_v, width_angle_q),
        make_identity_item("arrowhead.fill_tick", fill_v, fill_q),
        make_identity_item("arrowhead.arrow_closed", closed_v, closed_q),
    ]


def _build_tick_identity_items(
    *,
    centered_v,
    centered_q,
    pen_v,
    pen_q,
):
    return [
        make_identity_item("arrowhead.tick_mark_centered", centered_v, centered_q),
        make_identity_item("arrowhead.heavy_end_pen_weight", pen_v, pen_q),
    ]


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
    # val: {"sig_hash": str_or_none, "label": str, "name": str_or_none}
    ctx.setdefault("arrowheads_by_type_id", {})

    if ElementType is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"api_unreachable": True}
        return info

    # Collect *all* ElementTypes, then filter by the presence of arrowhead params.
    try:
        all_types = list(
            collect_types(
                doc,
                of_class=ElementType,
                where_key="arrowheads.element_types",
            )
        )
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

        # Capture BOTH display and raw int when possible
        style_disp = _as_value_string(p_style)
        style_raw_int = _as_int(p_style)

        # Canonical enum source: display if available, else int-as-string.
        style_src = style_disp if style_disp is not None else (str(style_raw_int) if style_raw_int is not None else None)
        _style_v, _style_q = canonicalize_enum(style_src)

        # Canonical style label for identity/join (prefer display; map known ints)
        style_label_v, style_label_q = _get_arrowhead_style(style_disp if style_disp is not None else style_raw_int, _style_q)

        # Also emit raw/display as explicit identity evidence
        style_raw_v, style_raw_q = canonicalize_int(style_raw_int)
        style_disp_v, style_disp_q = canonicalize_str_allow_empty(style_disp)

        # Tick Size (length) -> inches string
        p_tick = first_param(t, ui_names=["Tick Size"])
        tick_ft = _as_double(p_tick)
        tick_in = fnum(format_len_inches(tick_ft), 6)
        tick_in_v, tick_in_q = canonicalize_str_allow_empty(tick_in)

        # Width Angle (rad) -> degrees float
        p_ang = first_param(t, ui_names=["Arrow Width Angle", "Width Angle"])

        if p_ang is None:
            try:
                names = []
                params = getattr(t, "Parameters", None)
                if params is not None:
                    for p in params:
                        try:
                            d = getattr(p, "Definition", None)
                            nm = getattr(d, "Name", None) if d is not None else None
                            if nm:
                                names.append(str(nm))
                        except Exception:
                            continue
                # keep bounded; stable ordering for debug
                names = sorted(set(names))
                debug_param_names = ";".join(names[:80])
            except Exception:
                debug_param_names = ""
        else:
            debug_param_names = ""

        ang_rad = _as_double(p_ang)
        if ang_rad is None and p_ang is not None:
            # Fallback: read directly from Revit Parameter
            try:
                ang_rad = p_ang.AsDouble()
            except Exception:
                ang_rad = None

        # Debug evidence for what Revit is returning
        width_angle_disp = None
        try:
            width_angle_disp = p_ang.AsValueString() if p_ang is not None else None
        except Exception:
            width_angle_disp = None

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
            make_identity_item("arrowhead.style", style_label_v, style_label_q),
            make_identity_item("arrowhead.arrow_style_raw_int", style_raw_v, style_raw_q),
            make_identity_item("arrowhead.arrow_style_display", style_disp_v, style_disp_q),
            make_identity_item("arrowhead.tick_size_in", tick_in_v, tick_in_q),
            make_identity_item("arrowhead.width_angle_deg", ang_deg_v, ang_deg_q),
            make_identity_item("arrowhead.fill_tick", fill_v, fill_q),
            make_identity_item("arrowhead.arrow_closed", closed_v, closed_q),
            make_identity_item("arrowhead.tick_mark_centered", center_v, center_q),
            make_identity_item("arrowhead.heavy_end_pen_weight", pen_v, pen_q),
        ]

        semantic_keys = sorted({it.get("k") for it in identity_items if isinstance(it.get("k"), str)})

        # Required qs: style + tick_size must be OK (classifier depends on their presence)
        required_qs = [style_label_q, tick_in_q]
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
                "arrow_style_display": safe_str(style_disp) if style_disp else "",
                "arrow_style_raw_int": safe_str(style_raw_int) if style_raw_int is not None else "",
                "arrow_style_label": safe_str(style_label_v) if style_label_v else "",
                "arrow_style_label_q": safe_str(style_label_q) if style_label_q else "",
                "width_angle_raw_rad": safe_str(ang_rad) if ang_rad is not None else "",
                "width_angle_display": safe_str(width_angle_disp) if width_angle_disp else "",
                "width_angle_param_names": safe_str(debug_param_names) if debug_param_names else "",
            },
        )

        # Phase-2 (join-key candidates live ONLY here; identity remains authoritative)
        cosmetic_items = []
        unknown_items = []

        # Traceability fields (metadata only — never in hash/sig/join)
        try:
            _eid_raw = getattr(getattr(t, "Id", None), "IntegerValue", None)
            _eid_v, _eid_q = canonicalize_int(_eid_raw)
        except Exception:
            _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
        try:
            _uid_raw = getattr(t, "UniqueId", None)
            _uid_v, _uid_q = canonicalize_str(_uid_raw)
        except Exception:
            _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
        unknown_items.append(make_identity_item("arrowhead.source_element_id", _eid_v, _eid_q))
        unknown_items.append(make_identity_item("arrowhead.source_unique_id", _uid_v, _uid_q))

        rec_v2["phase2"] = {
            "schema": "phase2.arrowheads.v1",
            "grouping_basis": "phase2.hypothesis",
            "semantic_keys": semantic_keys,
            "cosmetic_items": phase2_sorted_items(cosmetic_items),
            "coordination_items": phase2_sorted_items([]),
            "unknown_items": phase2_sorted_items(unknown_items),
        }

        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "arrowheads")
        rec_v2["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )
        rec_v2["sig_basis"] = {
            "schema": "arrowheads.sig_basis.v1",
            "keys_used": semantic_keys,
        }

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

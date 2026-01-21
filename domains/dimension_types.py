# -*- coding: utf-8 -*-
"""
Dimension Types domain extractor.

Fingerprints dimension types including:
- Text font, size
- Line weight, color
- Tick mark (arrowhead)
- Witness line control

Per-record identity: UniqueId
Ordering: order-insensitive (sorted before hashing)
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    sig_val,
    fnum,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)
from core.rows import (
    first_param,
    _as_string,
    _as_double,
    _as_int,
    format_len_inches,
    try_get_color_rgb_from_elem,
    get_element_display_name,
    get_type_display_name,
)

try:
    from Autodesk.Revit.DB import DimensionType
except ImportError:
    DimensionType = None


# --- v2 helpers: Units / Alternate Units FormatOptions ---

def _as_string(v):
    """
    Defensive conversion to a stable string.

    Handles:
      - None
      - Revit Parameter-like objects (AsString / AsValueString)
      - Any other object via str()
    """
    if v is None:
        return ""

    # Revit DB.Parameter has AsString/AsValueString.
    try:
        if hasattr(v, "AsString"):
            s = v.AsString()
            if s is not None:
                return str(s)
    except Exception:
        pass

    try:
        if hasattr(v, "AsValueString"):
            s = v.AsValueString()
            if s is not None:
                return str(s)
    except Exception:
        pass

    try:
        return str(v)
    except Exception:
        return ""

def get_type_display_name(elem_type):
    """
    Deterministic, defensive type name extraction.

    Preference order:
      1) FamilyName + ":" + Name (common for many types)
      2) Name
      3) ElementId string as last resort

    This intentionally avoids localized UI display names.
    """
    if elem_type is None:
        return S_MISSING

    # Avoid raising if the element is a proxy or partially invalid.
    family = None
    name = None

    try:
        family = getattr(elem_type, "FamilyName", None)
    except Exception:
        family = None

    try:
        name = getattr(elem_type, "Name", None)
    except Exception:
        name = None

    if family and name:
        return "{0}:{1}".format(str(family), str(name))
    if name:
        return str(name)

    try:
        eid = getattr(elem_type, "Id", None)
        if eid is not None:
            return "id:{0}".format(str(eid))
    except Exception:
        pass

    return S_MISSING

def _fmt_in_from_ft(ft, places=6):
    if ft is None:
        return None
    try:
        inches = float(ft) * 12.0
        return format(inches, ".{}f".format(int(places)))
    except Exception as e:
        return None

def _fmt_float(x, places=12):
    if x is None:
        return None
    try:
        return format(float(x), ".{}g".format(int(places)))
    except Exception as e:
        return None

def _fmt_in_from_ft(ft, places=6):
    if ft is None:
        return None
    try:
        inches = float(ft) * 12.0
        return format(inches, ".{}f".format(int(places)))
    except Exception as e:
        return None

def _format_options_to_kv(fo):
    """
    Serialize Autodesk.Revit.DB.FormatOptions to a stable, hashable dict.
    Only include semantically relevant fields; stringify enums.
    """
    if fo is None:
        return None

    out = {}
    try:
        out["use_default"] = bool(getattr(fo, "UseDefault", False))
    except Exception as e:
        out["use_default"] = False

    # If using project default, do NOT serialize overrides
    if out["use_default"]:
        return out

    keys = [
        "Accuracy",
        "RoundingMethod",
        "UseDigitGrouping",
        "SuppressLeadingZeros",
        "SuppressTrailingZeros",
        "SuppressSpaces",
        "SuppressZeroFeet",
        "SuppressZeroInches",
        "UsePlusPrefix",
    ]

    for k in keys:
        try:
            if not hasattr(fo, k):
                continue

            v = getattr(fo, k)

            if k == "Accuracy":
                out["accuracy_in"] = _fmt_in_from_ft(v)
            else:
                out[k.lower()] = safe_str(v)

        except Exception as e:
            continue

    return out

def extract(doc, ctx=None):
    """
    Extract Dimension Types fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "names": [],
        "hash": None,

        # new
        "records": [],
        "signature_hashes": [],
        "raw_count": 0,
        "debug_missing_name": 0,

        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    types = list(
        collect_types(
            doc,
            of_class=DimensionType,
            require_unique_id=True,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            cache_key="dimension_types:DimensionType:types",
        )
    )

    info["raw_count"] = len(types)

    names = []
    missing = 0
    records = []
    sig_hashes = []

    # record.v2 build state
    v2_records = []
    v2_sig_hashes = []  # non-null only
    v2_block_reasons = {}

    for d in types:
        type_name = get_type_display_name(d)
        if type_name:
            type_name = canon_str(type_name)
            if type_name:
                names.append(type_name)
            else:
                missing += 1
                continue
        else:
            missing += 1
            continue

        # --- minimal dim-style signature (text + graphics + ticks) ---
        # Prefer BuiltInParameter when possible to avoid localized UI name dependence.
        text_font = _as_string(
            first_param(
                d,
                bip_names=[
                    "TEXT_FONT",
                    "DIM_TEXT_FONT",
                    "SPOT_ELEV_TEXT_FONT",
                    "SPOT_COORDINATE_TEXT_FONT",
                ],
                ui_names=["Text Font"],
            )
        )
        text_font = canon_str(text_font)

        text_size_ft = _as_double(
            first_param(
                d,
                bip_names=[
                    "TEXT_SIZE",
                    "DIM_TEXT_SIZE",
                    "SPOT_ELEV_TEXT_SIZE",
                    "SPOT_COORDINATE_TEXT_SIZE",
                ],
                ui_names=["Text Size"],
            )
        )
        text_size_in = fnum(format_len_inches(text_size_ft), 6)

        lw = _as_int(
            first_param(
                d,
                bip_names=["LINE_WEIGHT", "DIM_LINE_WEIGHT"],
                ui_names=["Line Weight"],
            )
        )
        color_int, color_rgb = try_get_color_rgb_from_elem(d)

        # Tick Mark (arrowhead) – store UniqueId metadata + include NAME in signature (more stable than ids)
        tick_name = _as_string(
            first_param(
                d,
                bip_names=["DIM_LEADER_ARROWHEAD", "TICK_MARK", "DIM_TICK_MARK"],
                ui_names=["Tick Mark"],
            )
        )
        tick_uid = None
        try:
            p_tick = first_param(
                d,
                bip_names=["DIM_LEADER_ARROWHEAD", "TICK_MARK", "DIM_TICK_MARK"],
                ui_names=["Tick Mark"],
            )
            if p_tick and p_tick.HasValue:
                tid = p_tick.AsElementId()
                if tid and tid.IntegerValue > 0:
                    te = doc.GetElement(tid)
                    if te:
                        tick_uid = te.UniqueId
                        # prefer element.Name where available
                        try:
                            tick_name = tick_name or get_element_display_name(te)
                            if tick_name is not None:
                                tick_name = canon_str(tick_name)
                        except Exception:
                            pass
        except Exception:
            pass

        # Witness line control is common; keep as metadata + optional signature
        witness = _as_string(first_param(d, ui_names=["Witness Line Control"]))
        witness = canon_str(witness)

        # --- additional likely-visible parameters (optional; will be S_MISSING if absent) ---
        def _p(ui_name):
            return first_param(d, ui_names=[ui_name])

        # Text formatting / placement
        text_bg = canon_str(_as_string(_p("Text Background")))
        width_factor = _as_double(_p("Width Factor"))
        text_offset = _as_double(_p("Text Offset"))

        bold = _as_int(_p("Bold"))
        italic = _as_int(_p("Italic"))
        underline = _as_int(_p("Underline"))
        suppress_spaces = _as_int(_p("Suppress Spaces"))
        read_conv = canon_str(_as_string(_p("Read Convention")))

        # Leaders (dims + spots vary; optional)
        leader_type = canon_str(_as_string(_p("Leader Type")))
        show_leader_when_text_moves = _as_int(_p("Show Leader When Text Moves"))
        leader_tick_mark = canon_str(_as_string(_p("Leader Tick Mark")))

        # Tick / line weights
        tick_lw = _as_int(_p("Tick Mark Line Weight"))

        # Common dim line + witness line settings (mostly linear/angular; optional)
        dim_line_ext = _as_double(_p("Dimension Line Extension"))
        flipped_dim_line_ext = _as_double(_p("Flipped Dimension Line Extension"))
        snap_dist = _as_double(_p("Dimension Line Snap Distance"))

        witness_ext = _as_double(_p("Witness Line Extension"))
        witness_gap = _as_double(_p("Witness Line Gap to Element"))
        witness_len = _as_double(_p("Witness Line Length"))

        # Center marks (radial/diameter; optional)
        center_marks = _as_int(_p("Center Marks"))
        center_mark_size = _as_double(_p("Center Mark Size"))

        # Units formatting via FormatOptions (NOT parameters)
        units_fmt = None
        alt_units_fmt = None

        try:
            fo = d.GetUnitsFormatOptions()
            units_fmt = _format_options_to_kv(fo)
        except Exception:
            units_fmt = None

        try:
            afo = d.GetAlternateUnitsFormatOptions()
            alt_units_fmt = _format_options_to_kv(afo)
        except Exception:
            alt_units_fmt = None

        tick_name = canon_str(tick_name)

        signature_tuple = [
            "text_font={}".format(sig_val(text_font)),
            "text_size_in={}".format(sig_val(text_size_in)),
            "line_weight={}".format(sig_val(lw)),
            "color_int={}".format(sig_val(color_int)),
            "tick_mark={}".format(sig_val(tick_name)),
            "witness_ctrl={}".format(sig_val(witness)),

            # expanded signature (optional fields)
            "text_bg={}".format(sig_val(text_bg)),
            "width_factor={}".format(sig_val(width_factor)),
            "text_offset_in={}".format(sig_val(_fmt_in_from_ft(text_offset))),
            "bold={}".format(sig_val(bold)),
            "italic={}".format(sig_val(italic)),
            "underline={}".format(sig_val(underline)),
            "suppress_spaces={}".format(sig_val(suppress_spaces)),
            "read_convention={}".format(sig_val(read_conv)),

            "leader_type={}".format(sig_val(leader_type)),
            "show_leader_when_text_moves={}".format(sig_val(show_leader_when_text_moves)),
            "leader_tick_mark={}".format(sig_val(leader_tick_mark)),

            "tick_mark_line_weight={}".format(sig_val(tick_lw)),

            "dim_line_ext_in={}".format(sig_val(_fmt_in_from_ft(dim_line_ext))),
            "flipped_dim_line_ext_in={}".format(sig_val(_fmt_in_from_ft(flipped_dim_line_ext))),
            "snap_dist_in={}".format(sig_val(_fmt_in_from_ft(snap_dist))),

            "witness_ext_in={}".format(sig_val(_fmt_in_from_ft(witness_ext))),
            "witness_gap_in={}".format(sig_val(_fmt_in_from_ft(witness_gap))),
            "witness_len_in={}".format(sig_val(_fmt_in_from_ft(witness_len))),

            "center_marks={}".format(sig_val(center_marks)),
            "center_mark_size_in={}".format(sig_val(_fmt_in_from_ft(center_mark_size))),

            # FormatOptions stringify (captures UseDefault + overrides without pretending it's a Parameter)
            "units_fmt={}".format(sig_val(safe_str(units_fmt) if units_fmt is not None else None)),
            "alt_units_fmt={}".format(sig_val(safe_str(alt_units_fmt) if alt_units_fmt is not None else None)),
        ]

        sig_hash = make_hash(signature_tuple)

        # ---------------------------
        # record.v2 per-record emission (identity lives in sig_hash)
        # ---------------------------

        # Required: dim_type.uid
        try:
            uid_raw = getattr(d, "UniqueId", None)
        except Exception:
            uid_raw = None
        uid_v, uid_q = canonicalize_str(uid_raw)

        # Required: dim_type.shape
        # Revit API varies by version; prefer any stable style/shape enum we can read.
        shape_raw = None
        for _attr in ("Shape", "StyleType", "DimensionShape", "DimensionStyleType"):
            try:
                if hasattr(d, _attr):
                    shape_raw = getattr(d, _attr, None)
                    if shape_raw is not None:
                        break
            except Exception:
                continue
        shape_v, shape_q = canonicalize_enum(shape_raw)

        # Optional: dim_type.text_type_uid (ElementId -> element.UniqueId)
        text_type_uid_v, text_type_uid_q = (None, ITEM_Q_MISSING)
        try:
            p_tt = first_param(d, bip_names=["TEXT_TYPE"], ui_names=["Text Type"])  # best-effort
            if p_tt is None:
                text_type_uid_v, text_type_uid_q = (None, ITEM_Q_MISSING)
            else:
                try:
                    eid = p_tt.AsElementId()
                except Exception:
                    eid = None
                if eid is None:
                    text_type_uid_v, text_type_uid_q = (None, ITEM_Q_UNREADABLE)
                else:
                    try:
                        el = doc.GetElement(eid)
                    except Exception:
                        el = None
                    if el is None:
                        text_type_uid_v, text_type_uid_q = (None, ITEM_Q_MISSING)
                    else:
                        try:
                            text_type_uid_v, text_type_uid_q = canonicalize_str(getattr(el, "UniqueId", None))
                        except Exception:
                            text_type_uid_v, text_type_uid_q = (None, ITEM_Q_UNREADABLE)
        except Exception:
            text_type_uid_v, text_type_uid_q = (None, ITEM_Q_UNREADABLE)

        # Optional: dim_type.tick_mark_uid
        tick_uid_v, tick_uid_q = canonicalize_str(tick_uid if tick_uid else None)

        # Optional: dim_type.witness_line_control
        witness_v, witness_q = canonicalize_str(witness if witness else None)

        # Optional: unit format identity fields from UnitsFormatOptions
        unit_format_id_v, unit_format_id_q = (None, ITEM_Q_MISSING)
        rounding_v, rounding_q = (None, ITEM_Q_MISSING)
        accuracy_v, accuracy_q = (None, ITEM_Q_MISSING)
        prefix_v, prefix_q = (None, ITEM_Q_UNSUPPORTED)
        suffix_v, suffix_q = (None, ITEM_Q_UNSUPPORTED)

        try:
            fo = d.GetUnitsFormatOptions()
        except Exception:
            fo = None

        if fo is None:
            unit_format_id_v, unit_format_id_q = (None, ITEM_Q_UNREADABLE)
            rounding_v, rounding_q = (None, ITEM_Q_UNREADABLE)
            accuracy_v, accuracy_q = (None, ITEM_Q_UNREADABLE)
        else:
            try:
                unit_format_id_v, unit_format_id_q = canonicalize_str(safe_str(fo.GetUnitTypeId()))
            except Exception:
                unit_format_id_v, unit_format_id_q = (None, ITEM_Q_UNREADABLE)
            try:
                rounding_v, rounding_q = canonicalize_enum(getattr(fo, "RoundingMethod", None))
            except Exception:
                rounding_v, rounding_q = (None, ITEM_Q_UNREADABLE)
            try:
                # UnitsFormatOptions.Accuracy is stored as feet; identity key expects a string, so use inches string.
                accuracy_v, accuracy_q = canonicalize_float(_fmt_in_from_ft(getattr(fo, "Accuracy", None)))
            except Exception:
                accuracy_v, accuracy_q = (None, ITEM_Q_UNREADABLE)
            # Prefix/Suffix are version-dependent; mark unsupported when absent.
            if hasattr(fo, "Prefix"):
                try:
                    prefix_v, prefix_q = canonicalize_str(getattr(fo, "Prefix", None))
                except Exception:
                    prefix_v, prefix_q = (None, ITEM_Q_UNREADABLE)
            if hasattr(fo, "Suffix"):
                try:
                    suffix_v, suffix_q = canonicalize_str(getattr(fo, "Suffix", None))
                except Exception:
                    suffix_v, suffix_q = (None, ITEM_Q_UNREADABLE)

        identity_items = [
            make_identity_item("dim_type.uid", uid_v, uid_q),
            make_identity_item("dim_type.shape", shape_v, shape_q),
            make_identity_item("dim_type.text_type_uid", text_type_uid_v, text_type_uid_q),
            make_identity_item("dim_type.tick_mark_uid", tick_uid_v, tick_uid_q),
            make_identity_item("dim_type.witness_line_control", witness_v, witness_q),
            make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
            make_identity_item("dim_type.rounding", rounding_v, rounding_q),
            make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
            make_identity_item("dim_type.prefix", prefix_v, prefix_q),
            make_identity_item("dim_type.suffix", suffix_v, suffix_q),
        ]
        identity_items = sorted(identity_items, key=lambda it: it.get("k", ""))

        required_qs = [uid_q, shape_q]
        blocked = any(q != ITEM_Q_OK for q in required_qs)

        status_reasons = []
        any_incomplete = False
        for it in identity_items:
            q = it.get("q")
            if q != ITEM_Q_OK:
                any_incomplete = True
                status_reasons.append("identity.incomplete:{}:{}".format(q, it.get("k")))

        # Label
        label_quality = "human"
        label_display = safe_str(type_name) if (type_name not in (S_MISSING, S_UNREADABLE) and safe_str(type_name).strip()) else "Dimension Type"
        if blocked:
            # Required identity missing/unreadable: mark placeholder to avoid implying a human label is authoritative.
            label_quality = "placeholder_unreadable" if (uid_q == ITEM_Q_UNREADABLE or shape_q == ITEM_Q_UNREADABLE) else "placeholder_missing"

        label = {
            "display": label_display,
            "quality": label_quality,
            "provenance": "revit.Name",
            "components": {
                "type_id": safe_str(getattr(getattr(d, "Id", None), "IntegerValue", "")),
                "type_name": safe_str(type_name),
            },
        }

        record_id = uid_v if (uid_q == ITEM_Q_OK and uid_v) else "dim_type_id:{}".format(safe_str(getattr(getattr(d, "Id", None), "IntegerValue", "")))

        if blocked:
            v2_block_reasons["blocked_required"] = int(v2_block_reasons.get("blocked_required", 0)) + 1
            rec_v2 = build_record_v2(
                domain="dimension_types",
                record_id=record_id,
                status=STATUS_BLOCKED,
                status_reasons=sorted(set(status_reasons)) or ["minima.required_not_ok"],
                sig_hash=None,
                identity_items=identity_items,
                required_qs=(),
                label=label,
            )
        else:
            status = STATUS_DEGRADED if any_incomplete else STATUS_OK
            preimage = serialize_identity_items(identity_items)
            sig_hash_v2 = make_hash(preimage)
            v2_sig_hashes.append(sig_hash_v2)
            rec_v2 = build_record_v2(
                domain="dimension_types",
                record_id=record_id,
                status=status,
                status_reasons=sorted(set(status_reasons)),
                sig_hash=sig_hash_v2,
                identity_items=identity_items,
                required_qs=required_qs,
                label=label,
            )

        v2_records.append_

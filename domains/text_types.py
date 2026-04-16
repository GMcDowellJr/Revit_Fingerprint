# -*- coding: utf-8 -*-
"""
Text Types domain extractor.

Fingerprints text note types including:
- Font, size, width factor
- Background, border, tab settings
- Line weight, color
- Bold, italic, underline

Per-record identity: sig_hash (UID-free by contract; UIDs are metadata only)
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
   
    fnum,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)

from core.rows import (
    first_param, _as_string, _as_double, _as_int, _as_bool_from_param,
    format_len_inches, try_get_color_rgb_from_elem,
    get_type_display_name, get_element_display_name
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)

from core.record_v2 import (
    STATUS_OK,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    canonicalize_str,
    canonicalize_int,
    canonicalize_float,
    canonicalize_bool,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)

from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import BuiltInCategory, TextNoteType
except ImportError:
    TextNoteType = None

def _is_type_purgeable(doc, type_id, bic):
    """
    Returns True if no instances reference this type (safe to purge).
    Returns False if at least one instance references it.
    Returns None if the API check fails.

    Only valid for types exposed in Revit's Purge Unused UI.
    Other domains intentionally emit None for this field by design.
    """
    try:
        from Autodesk.Revit.DB import FilteredElementCollector

        count = 0
        collector = (
            FilteredElementCollector(doc)
            .OfCategory(bic)
            .WhereElementIsNotElementType()
        )
        for elem in collector:
            if elem.GetTypeId() == type_id:
                count += 1
                break  # early exit — we only need to know if > 0
        return count == 0
    except Exception:
        return None


def _phase2_item(k, raw_v, *, allow_empty=False):
    v, q = phase2_qv_from_legacy_sentinel_str(raw_v, allow_empty=allow_empty)
    return {"k": k, "q": q, "v": v}


def _phase2_build_payload(rec, elem=None):
    # Hypotheses only: semantic vs cosmetic vs unknown buckets.
    cosmetic_items = phase2_sorted_items([
        _phase2_item("text_type.name", rec.get("type_name"), allow_empty=True),
    ])

    unknown_list = [
        _phase2_item("text_type.type_uid", rec.get("type_uid"), allow_empty=True),
        _phase2_item("text_type.type_id", rec.get("type_id"), allow_empty=True),
        _phase2_item("text_type.color_int", rec.get("color_int")),

        # Tri-state:
        # - q="ok", v=None => explicit "no arrowhead" (valid, joinable state)
        # - q="ok", v=<hash/name> => arrowhead present
        _phase2_item("text_type.leader_arrowhead_uid", rec.get("leader_arrowhead_uid"), allow_empty=True)
        if rec.get("leader_arrowhead_sig_hash") not in (None, "")
        else {"k": "text_type.leader_arrowhead_uid", "q": "ok", "v": None},

        _phase2_item("text_type.leader_arrowhead_name", rec.get("leader_arrowhead_name"), allow_empty=True)
        if rec.get("leader_arrowhead_sig_hash") not in (None, "")
        else {"k": "text_type.leader_arrowhead_name", "q": "ok", "v": None},

        _phase2_item("text_type.leader_arrowhead_sig_hash", rec.get("leader_arrowhead_sig_hash"), allow_empty=True)
        if rec.get("leader_arrowhead_sig_hash") not in (None, "")
        else {"k": "text_type.leader_arrowhead_sig_hash", "q": "ok", "v": None},
    ]

    # Traceability fields (metadata only — never in hash/sig/join)
    if elem is not None:
        try:
            _eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
            _eid_v, _eid_q = canonicalize_int(_eid_raw)
        except Exception:
            _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
        try:
            _uid_raw = getattr(elem, "UniqueId", None)
            _uid_v, _uid_q = canonicalize_str(_uid_raw)
        except Exception:
            _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
        unknown_list.append({"k": "text_type.source_element_id", "q": _eid_q, "v": _eid_v})
        unknown_list.append({"k": "text_type.source_unique_id", "q": _uid_q, "v": _uid_v})

    unknown_items = phase2_sorted_items(unknown_list)

    return {
        "schema": "phase2.text_types.v1",
        "grouping_basis": "phase2.hypothesis",
        # Selector-based semantic basis; canonical evidence lives in identity_basis.items.
        # Deprecated direction: semantic_items should not duplicate canonical k/q/v evidence.
                "cosmetic_items": cosmetic_items,
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": unknown_items,
    }


TEXT_TYPE_SEMANTIC_KEYS = sorted([
    "text_type.name",
    "text_type.font",
    "text_type.size_in",
    "text_type.width_factor",
    "text_type.background",
    "text_type.line_weight",
    "text_type.color_rgb",
    "text_type.show_border",
    "text_type.leader_border_offset_in",
    "text_type.tab_size_in",
    "text_type.bold",
    "text_type.italic",
    "text_type.underline",
])

def extract(doc, ctx=None):
    """
    Extract Text Types fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused here; present for extractor parity)

    Returns:
        Dictionary with count, hash, records, record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "records": [],
        
        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    types = []
    try:
        types = list(
            collect_types(
                doc,
                of_class=TextNoteType,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="text_types:TextNoteType:types",
            )
        )
    except Exception as e:
        types = []

    info["raw_count"] = len(types)

    names = []
    missing = 0
    records = []
    
    # v2 build state (domain-level block; no partial coverage semantics)
    v2_records = []
    v2_sig_hashes = []
    v2_blocked = False
    v2_reasons = {}
    # Debug-only: keep the legacy pipe-delimited signature row out of records[]
    # (records[] must contain only record.v2 objects). Bound for diff friendliness.
    v2_sig_rows = []

    def _v2_block(reason_key):
        nonlocal v2_blocked
        if not v2_blocked:
            v2_blocked = True
        v2_reasons[reason_key] = True

    for t in types:
        type_name = get_type_display_name(t)
        if type_name:
            type_name = canon_str(type_name)
            names.append(type_name)
        else:
            missing += 1
            type_name = S_MISSING

        # --- core fields ---
        font = _as_string(first_param(t, bip_names=["TEXT_FONT"], ui_names=["Text Font"]))
        size_ft = _as_double(first_param(t, bip_names=["TEXT_SIZE"], ui_names=["Text Size"]))
        size_in = fnum(format_len_inches(size_ft), 6)

        font = canon_str(font)

        width_factor = _as_double(first_param(t, bip_names=["TEXT_WIDTH_SCALE"], ui_names=["Width Factor"]))
        width_factor_n = fnum(width_factor, 6)

        background_i = _as_int(first_param(t, bip_names=["TEXT_BACKGROUND"], ui_names=["Background"]))

        # Graphics
        p_lw = first_param(t, bip_names=["TEXT_LINE_WEIGHT", "LINE_PEN"], ui_names=["Line Weight"])
        line_weight = _as_int(p_lw)

        color_int, color_rgb = try_get_color_rgb_from_elem(t)

        # Canonicalize RGB to a stable string (avoid Python dict repr drift)
        # Accepts dict/tuple/list/"r-g-b" strings; emits "r-g-b" or None.
        def _canon_rgb(v):
            if v is None:
                return None
            if isinstance(v, dict):
                try:
                    r = int(v.get("r", 0))
                    g = int(v.get("g", 0))
                    b = int(v.get("b", 0))
                    return "{}-{}-{}".format(r, g, b)
                except Exception:
                    return safe_str(v)
            if isinstance(v, (list, tuple)) and len(v) == 3:
                try:
                    r, g, b = [int(x) for x in v]
                    return "{}-{}-{}".format(r, g, b)
                except Exception:
                    return safe_str(v)
            s = safe_str(v)
            # If already "r-g-b", keep as-is
            if isinstance(s, str) and s.count("-") == 2:
                return s
            return s

        color_rgb = _canon_rgb(color_rgb)

        # Border / tabs / styles
        show_border = _as_bool_from_param(first_param(t, ui_names=["Show Border", "Show border"]))
        leader_border_offset_ft = _as_double(first_param(t, ui_names=["Leader/Border Offset", "Leader / Border Offset"]))
        leader_border_offset_in = fnum(format_len_inches(leader_border_offset_ft), 6)

        tab_size_ft = _as_double(first_param(t, ui_names=["Tab Size", "Tab size"]))
        tab_size_in = fnum(format_len_inches(tab_size_ft), 6)

        bold = _as_bool_from_param(first_param(t, ui_names=["Bold"]))
        italic = _as_bool_from_param(first_param(t, ui_names=["Italic"]))
        underline = _as_bool_from_param(first_param(t, ui_names=["Underline"]))

        # Leader Arrowhead (metadata only; do NOT put in core signature)
        leader_arrow_uid = None
        leader_arrow_name = None
        leader_arrow_sig_hash = None

        try:
            p_arrow = first_param(t, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])
            if p_arrow:
                arrow_id = p_arrow.AsElementId()
                if arrow_id and arrow_id.IntegerValue > 0:
                    arrow = doc.GetElement(arrow_id)
                    if arrow:
                        leader_arrow_uid = getattr(arrow, "UniqueId", None)

                        try:
                            ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
                            k = safe_str(getattr(arrow_id, "IntegerValue", None))
                            if k and isinstance(ah_map, dict) and k in ah_map:
                                leader_arrow_sig_hash = ah_map.get(k, {}).get("sig_hash", None)
                        except Exception:
                            leader_arrow_sig_hash = None

                        leader_arrow_name = get_type_display_name(arrow) or getattr(arrow, "Name", None)
                        leader_arrow_name = canon_str(leader_arrow_name)
        except Exception as e:
            leader_arrow_uid = None
            leader_arrow_name = None

        # --- signature tuple (core) ---
        signature_tuple = [
            "font={}".format(canon_str(font)),
            "size_in={}".format(canon_str(size_in)),
            "width_factor={}".format(canon_str(width_factor_n)),
            "background={}".format(canon_str(background_i)),
            "line_weight={}".format(canon_str(line_weight)),
            "color_int={}".format(canon_str(color_int)),

            "show_border={}".format(canon_str(show_border)),
            "leader_border_offset_in={}".format(canon_str(leader_border_offset_in)),
            "tab_size_in={}".format(canon_str(tab_size_in)),
            "bold={}".format(canon_str(bold)),
            "italic={}".format(canon_str(italic)),
            "underline={}".format(canon_str(underline)),
        ]
        sig_hash = make_hash(signature_tuple)

        # ---------------------------
        # v2 signature (contract semantic hash)
        # ---------------------------
        # Exception (explicit): text type name is part of identity/definition in this domain.
        # Leader Arrowhead is excluded from v2 by policy decision.
        if not v2_blocked:
            # Block on unreadables / sentinel-like values (no partial coverage semantics)
            if not font:
                _v2_block("unreadable_font")

            # numeric fields must be non-None and must not be sentinel strings
            if size_ft is None:
                _v2_block("unreadable_text_size")
            if width_factor is None:
                _v2_block("unreadable_width_factor")
            if background_i is None:
                _v2_block("unreadable_background")
            if line_weight is None:
                _v2_block("unreadable_line_weight")

            # color must be readable; use RGB in v2 (avoid element ids / GUIDs)
            if canon_str(color_rgb) in (S_MISSING, S_UNREADABLE):
                 _v2_block("unreadable_color_rgb")

            # boolean-ish fields must be non-None
            if show_border is None:
                _v2_block("unreadable_show_border")
            if bold is None:
                _v2_block("unreadable_bold")
            if italic is None:
                _v2_block("unreadable_italic")
            if underline is None:
                _v2_block("unreadable_underline")

            # offsets/tab sizes must be readable doubles
            if leader_border_offset_ft is None:
                _v2_block("unreadable_leader_border_offset")
            if tab_size_ft is None:
                _v2_block("unreadable_tab_size")

            if not v2_blocked:
                # Use the already-normalized representations where applicable
                v2_sig_rows.append("|".join([
                    "name={}".format(safe_str(type_name)),
                    "font={}".format(safe_str(font)),
                    "size_in={}".format(safe_str(size_in)),
                    "width_factor={}".format(safe_str(width_factor_n)),
                    "background={}".format(safe_str(background_i)),
                    "line_weight={}".format(safe_str(line_weight)),
                    "color_rgb={}".format(safe_str(color_rgb)),

                    "show_border={}".format(safe_str(show_border)),
                    "leader_border_offset_in={}".format(safe_str(leader_border_offset_in)),
                    "tab_size_in={}".format(safe_str(tab_size_in)),
                    "bold={}".format(safe_str(bold)),
                    "italic={}".format(safe_str(italic)),
                    "underline={}".format(safe_str(underline)),
                ]))

        rec = {
            "type_id": safe_str(t.Id.IntegerValue),
            "type_uid": getattr(t, "UniqueId", "") or "",
            "type_name": type_name,

            "font": font,
            "text_size_ft": size_ft,
            "text_size_in": size_in,
            "width_factor": width_factor_n,
            "background_raw": background_i,
            "line_weight": line_weight,

            "color_int": color_int,
            "color_rgb": color_rgb,

            "show_border": show_border,
            "leader_border_offset_in": leader_border_offset_in,
            "tab_size_in": tab_size_in,
            "bold": bold,
            "italic": italic,
            "underline": underline,

            "leader_arrowhead_uid": leader_arrow_uid,
            "leader_arrowhead_name": leader_arrow_name,
            "leader_arrowhead_sig_hash": safe_str(leader_arrow_sig_hash) if leader_arrow_sig_hash else None,
            "signature_tuple": signature_tuple,
            "signature_hash": sig_hash
        }

        # ---------------------------
        # Phase 2 (additive, explanatory, reversible)
        # Emit even if v2 is domain-blocked.
        # ---------------------------
        # Canonical evidence source for this pilot is identity_basis.items.
        # join_key and semantic selectors reference subsets without duplicating k/q/v payloads.
        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "text_types")

        rec["phase2"] = _phase2_build_payload(rec, elem=t)

        status_v2 = STATUS_OK
        status_reasons_v2 = []
        identity_items_v2 = []

        name_v2, name_q = canonicalize_str(type_name)
        font_v2, font_q = canonicalize_str(font)
        size_in_v2, size_in_q = canonicalize_str(size_in)
        # Keep width_factor as the already-normalized fixed-precision string (avoid 6 vs 9 drift)
        width_v2, width_q = canonicalize_str(width_factor_n)
        bg_v2, bg_q = canonicalize_int(background_i)
        lw_v2, lw_q = canonicalize_int(line_weight)
        rgb_v2, rgb_q = canonicalize_str(color_rgb)
        show_v2, show_q = canonicalize_bool(show_border)
        leader_off_v2, leader_off_q = canonicalize_str(leader_border_offset_in)
        tab_v2, tab_q = canonicalize_str(tab_size_in)
        bold_v2, bold_q = canonicalize_bool(bold)
        italic_v2, italic_q = canonicalize_bool(italic)
        underline_v2, underline_q = canonicalize_bool(underline)

        identity_items_v2.append(make_identity_item("text_type.name", name_v2, name_q))
        identity_items_v2.append(make_identity_item("text_type.font", font_v2, font_q))
        identity_items_v2.append(make_identity_item("text_type.size_in", size_in_v2, size_in_q))
        identity_items_v2.append(make_identity_item("text_type.width_factor", width_v2, width_q))
        identity_items_v2.append(make_identity_item("text_type.background", bg_v2, bg_q))
        identity_items_v2.append(make_identity_item("text_type.line_weight", lw_v2, lw_q))
        identity_items_v2.append(make_identity_item("text_type.color_rgb", rgb_v2, rgb_q))
        identity_items_v2.append(make_identity_item("text_type.show_border", show_v2, show_q))
        identity_items_v2.append(make_identity_item("text_type.leader_border_offset_in", leader_off_v2, leader_off_q))
        identity_items_v2.append(make_identity_item("text_type.tab_size_in", tab_v2, tab_q))
        identity_items_v2.append(make_identity_item("text_type.bold", bold_v2, bold_q))
        identity_items_v2.append(make_identity_item("text_type.italic", italic_v2, italic_q))
        identity_items_v2.append(make_identity_item("text_type.underline", underline_v2, underline_q))

        required_qs = [name_q, font_q, size_in_q, width_q, bg_q, lw_q, rgb_q, show_q, leader_off_q, tab_q, bold_q, italic_q, underline_q]

        if any(q != ITEM_Q_OK for q in required_qs):
            status_v2 = STATUS_BLOCKED
            status_reasons_v2.append("required_identity_not_ok")

        identity_items_v2_sorted = sorted(identity_items_v2, key=lambda d: str(d.get("k","")))
        semantic_items_v2 = [
            it for it in identity_items_v2_sorted
            if safe_str(it.get("k", "")) in set(TEXT_TYPE_SEMANTIC_KEYS)
        ]
        sig_preimage_v2 = serialize_identity_items(semantic_items_v2)
        sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(sig_preimage_v2)

        is_purgeable = _is_type_purgeable(doc, getattr(t, "Id", None), BuiltInCategory.OST_TextNotes)

        rec_v2 = build_record_v2(
            domain="text_types",
            record_id=safe_str(type_name) if safe_str(type_name) else safe_str(t.Id.IntegerValue),
            status=status_v2,
            status_reasons=sorted(set(status_reasons_v2)),
            sig_hash=sig_hash_v2,
            identity_items=identity_items_v2_sorted,
            required_qs=required_qs,
            label={
                "display": safe_str(type_name),
                "quality": "human",
                "provenance": "revit.TextNoteType.Name",
            },
            debug={
                "sig_preimage_sample": sig_preimage_v2[:6],
                "uid_excluded_from_sig": True,
                "leader_arrowhead_uid_excluded_from_sig": True,
            },
        )
        rec_v2["is_purgeable"] = is_purgeable
        rec_v2["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items_v2_sorted,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )

        rec_v2["phase2"] = rec["phase2"]
        rec_v2["sig_basis"] = {
            "schema": "text_types.sig_basis.v1",
            "keys_used": TEXT_TYPE_SEMANTIC_KEYS,
        }

        v2_records.append(rec_v2)
        if sig_hash_v2 is not None:
            v2_sig_hashes.append(sig_hash_v2)

        records.append(rec)

    info["names"] = sorted(names)
    info["count"] = len(info["names"])

    info["records"] = v2_records
    # Bound debug payload (avoid large repeated strings in exports)
    if v2_sig_rows:
        info["debug_v2_sig_rows_sample"] = v2_sig_rows[:10]

    # v2 hash (domain-level block; no partial coverage semantics)
    info["debug_v2_blocked"] = bool(v2_blocked)
    info["debug_v2_block_reasons"] = v2_reasons if v2_blocked else {}
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)
    if (not v2_blocked) and info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    else:
        info["hash_v2"] = None

    info["record_rows"] = [{
        "record_key": safe_str(r.get("record_id", "")),
        "sig_hash":   safe_str(r.get("sig_hash", "")),
        "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
    } for r in v2_records if isinstance(r, dict)]

    return info

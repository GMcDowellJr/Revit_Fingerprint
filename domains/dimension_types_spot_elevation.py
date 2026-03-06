# -*- coding: utf-8 -*-
"""
Dimension Types - Spot Elevation domain extractor.

Fingerprints SpotElevation and SpotElevationFixed dimension types.

Domain family: dimension_types
Contains shapes: SpotElevation, SpotElevationFixed

Per-record identity: sig_hash (UID-free) derived from identity_items.
Ordering: order-insensitive (identity_items sorted before hashing)
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.rows import first_param, _as_string, _as_double, _as_int, get_element_display_name
from core.canon import canon_str, S_MISSING, S_UNREADABLE
from core.record_v2 import (
    canonicalize_str,
    canonicalize_str_allow_empty,
    canonicalize_int,
    canonicalize_float,
    canonicalize_bool,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    build_record_v2,
    make_identity_item,
    serialize_identity_items,
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
)
from core.phase2 import phase2_sorted_items
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy
from core.dimension_type_helpers import (
    _get_dimension_shape,
    _build_text_appearance_items,
    _read_unit_format_info,
    get_type_display_name,
    SHAPE_SPOT_ELEVATION,
    SHAPE_SPOT_ELEVATION_FIXED,
    SHAPE_SPOT_COORDINATE,
    SHAPE_SPOT_SLOPE,
    FAMILY_SPOT,
)

try:
    from Autodesk.Revit.DB import DimensionType
except ImportError:
    DimensionType = None

DOMAIN_NAME = "dimension_types_spot_elevation"

# Shapes handled by this domain
_HANDLED_SHAPES = frozenset({SHAPE_SPOT_ELEVATION, SHAPE_SPOT_ELEVATION_FIXED})


def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
    """
    Heuristic override: use FamilyName prefix to more precisely classify Spot types.
    Returns updated (shape_v, shape_family, shape_q).
    """
    try:
        family_name = getattr(d, "FamilyName", None)
        basis = family_name if family_name else type_name
        bn_l = safe_str(basis).strip().lower()

        if bn_l.startswith("spot slopes"):
            return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
        elif bn_l.startswith("spot elevations"):
            return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
        elif bn_l.startswith("spot coordinates"):
            return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
    except Exception:
        pass
    return (shape_v, shape_family, shape_q)


def _read_symbol_sig_hash(d, doc):
    """
    Try to read a "Spot Symbol" or "Symbol" parameter that references a loaded family.
    If ElementId > 0, resolve element and use its name as the hash input.
    Returns (symbol_sig_hash_v, symbol_sig_hash_q).
    """
    try:
        p_sym = first_param(d, ui_names=["Spot Symbol", "Symbol"])
        if p_sym is None:
            return (None, ITEM_Q_MISSING)

        if not getattr(p_sym, "HasValue", False):
            return (None, ITEM_Q_MISSING)

        eid = None
        try:
            eid = p_sym.AsElementId()
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if eid is None or getattr(eid, "IntegerValue", 0) <= 0:
            return (None, ITEM_Q_MISSING)

        elem = None
        try:
            elem = doc.GetElement(eid)
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if elem is None:
            return (None, ITEM_Q_MISSING)

        name = None
        try:
            name = get_element_display_name(elem)
        except Exception:
            pass

        if not name:
            try:
                name = safe_str(getattr(elem, "Name", None))
            except Exception:
                pass

        if name:
            sym_hash = make_hash([safe_str(name)])
            return (sym_hash, ITEM_Q_OK)
        else:
            return (None, ITEM_Q_MISSING)

    except Exception:
        return (None, ITEM_Q_UNREADABLE)


def extract(doc, ctx=None):
    """
    Extract SpotElevation and SpotElevationFixed dimension types fingerprint.

    Args:
        doc: Revit Document
        ctx: Context dictionary

    Returns:
        Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
    """
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

    if DimensionType is None:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"api_unreachable": True}
        return info

    try:
        all_types = list(collect_types(
            doc,
            of_class=DimensionType,
            require_unique_id=True,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            cache_key="dimension_types:DimensionType:types",
        ))
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             _rounding_v, _rounding_q,
             _accuracy_v, _accuracy_q) = _read_unit_format_info(d)

            # Elevation Indicator
            elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_ei = first_param(d, ui_names=["Elevation Indicator"])
                ei_raw = _as_string(p_ei) if p_ei is not None else None
                elevation_indicator_v, elevation_indicator_q = canonicalize_str_allow_empty(ei_raw)
            except Exception:
                elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Elevation Indicator as Prefix/Suffix
            elev_ind_prefix_v, elev_ind_prefix_q = (None, ITEM_Q_MISSING)
            try:
                p_eip = first_param(d, ui_names=["Indicator as Prefix/Suffix", "Elevation Indicator as Prefix/Suffix"])
                eip_int = _as_int(p_eip) if p_eip is not None else None
                elev_ind_prefix_v, elev_ind_prefix_q = canonicalize_bool(eip_int)
            except Exception:
                elev_ind_prefix_v, elev_ind_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Top Value indicator
            top_indicator_v, top_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_top = first_param(d, ui_names=["Top Value", "Top Indicator"])
                top_raw = _as_string(p_top) if p_top is not None else None
                top_indicator_v, top_indicator_q = canonicalize_str_allow_empty(top_raw)
            except Exception:
                top_indicator_v, top_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Bottom Value indicator
            bottom_indicator_v, bottom_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_bot = first_param(d, ui_names=["Bottom Value", "Bottom Indicator"])
                bot_raw = _as_string(p_bot) if p_bot is not None else None
                bottom_indicator_v, bottom_indicator_q = canonicalize_str_allow_empty(bot_raw)
            except Exception:
                bottom_indicator_v, bottom_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Top Value as Prefix/Suffix
            top_ind_prefix_v, top_ind_prefix_q = (None, ITEM_Q_MISSING)
            try:
                p_tip = first_param(d, ui_names=["Top Value as Prefix/Suffix"])
                tip_int = _as_int(p_tip) if p_tip is not None else None
                top_ind_prefix_v, top_ind_prefix_q = canonicalize_bool(tip_int)
            except Exception:
                top_ind_prefix_v, top_ind_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Bottom Value as Prefix/Suffix
            bot_ind_prefix_v, bot_ind_prefix_q = (None, ITEM_Q_MISSING)
            try:
                p_bip = first_param(d, ui_names=["Bottom Value as Prefix/Suffix"])
                bip_int = _as_int(p_bip) if p_bip is not None else None
                bot_ind_prefix_v, bot_ind_prefix_q = canonicalize_bool(bip_int)
            except Exception:
                bot_ind_prefix_v, bot_ind_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Text Orientation
            text_orientation_v, text_orientation_q = (None, ITEM_Q_MISSING)
            try:
                p_to = first_param(d, ui_names=["Text Orientation"])
                to_raw = _as_string(p_to) if p_to is not None else None
                text_orientation_v, text_orientation_q = canonicalize_str_allow_empty(to_raw)
            except Exception:
                text_orientation_v, text_orientation_q = (None, ITEM_Q_UNREADABLE)

            # Text Location / Note Location
            text_location_v, text_location_q = (None, ITEM_Q_MISSING)
            try:
                p_tl = first_param(d, ui_names=["Note Location", "Text Location"])
                tl_raw = _as_string(p_tl) if p_tl is not None else None
                text_location_v, text_location_q = canonicalize_str_allow_empty(tl_raw)
            except Exception:
                text_location_v, text_location_q = (None, ITEM_Q_UNREADABLE)

            # Symbol sig hash
            symbol_sig_hash_v, symbol_sig_hash_q = _read_symbol_sig_hash(d, doc)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
                make_identity_item("dim_type.elevation_indicator", elevation_indicator_v, elevation_indicator_q),
                make_identity_item("dim_type.elevation_indicator_as_prefix_suffix", elev_ind_prefix_v, elev_ind_prefix_q),
                make_identity_item("dim_type.top_indicator", top_indicator_v, top_indicator_q),
                make_identity_item("dim_type.bottom_indicator", bottom_indicator_v, bottom_indicator_q),
                make_identity_item("dim_type.top_indicator_as_prefix_suffix", top_ind_prefix_v, top_ind_prefix_q),
                make_identity_item("dim_type.bottom_indicator_as_prefix_suffix", bot_ind_prefix_v, bot_ind_prefix_q),
                make_identity_item("dim_type.text_orientation", text_orientation_v, text_orientation_q),
                make_identity_item("dim_type.text_location", text_location_v, text_location_q),
                make_identity_item("dim_type.symbol_sig_hash", symbol_sig_hash_v, symbol_sig_hash_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            required_qs = [
                shape_q,
                unit_format_id_q,
                elevation_indicator_q,
                elev_ind_prefix_q,
                top_indicator_q,
                bottom_indicator_q,
                top_ind_prefix_q,
                bot_ind_prefix_q,
                text_orientation_q,
                text_location_q,
                symbol_sig_hash_q,
            ]
            for it in text_items:
                required_qs.append(it.get("q", ITEM_Q_MISSING))

            blocked = any(q != ITEM_Q_OK for q in required_qs)

            status_reasons = []
            for it in identity_items:
                q = it.get("q")
                k = it.get("k", "")
                if q == ITEM_Q_OK:
                    continue
                status_reasons.append("identity.incomplete:{}:{}".format(q, k))

            if blocked:
                status = STATUS_BLOCKED
            elif status_reasons:
                status = STATUS_DEGRADED
            else:
                status = STATUS_OK

            preimage = serialize_identity_items(identity_items)
            sig_hash = None if blocked else make_hash(preimage)

            try:
                type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
            except Exception:
                type_id_int = None

            try:
                uid_raw = getattr(d, "UniqueId", None)
            except Exception:
                uid_raw = None

            label_str = type_name
            rec_v2 = build_record_v2(
                domain=DOMAIN_NAME,
                record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
                status=status,
                status_reasons=sorted(set(status_reasons)),
                sig_hash=sig_hash,
                identity_items=identity_items,
                required_qs=tuple(required_qs),
                label={
                    "display": safe_str(label_str) if label_str else DOMAIN_NAME,
                    "quality": "human" if label_str else "placeholder_missing",
                    "provenance": "revit.DimensionType.params",
                },
            )

            pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
            rec_v2["join_key"], _missing = build_join_key_from_policy(
                domain_policy=pol,
                identity_items=identity_items,
                include_optional_items=False,
                emit_keys_used=True,
                hash_optional_items=False,
                emit_items=False,
                emit_selectors=True,
            )

            coordination_items = [
                make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
            ]

            unknown_items = []
            try:
                _eid_v, _eid_q = canonicalize_int(type_id_int)
            except Exception:
                _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
            try:
                _uid_v, _uid_q = canonicalize_str(uid_raw)
            except Exception:
                _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
            unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
            unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))

            rec_v2["phase2"] = {
                "schema": "phase2.{}.v1".format(DOMAIN_NAME),
                "grouping_basis": "phase2.hypothesis",
                "cosmetic_items": phase2_sorted_items([]),
                "coordination_items": phase2_sorted_items(coordination_items),
                "unknown_items": phase2_sorted_items(unknown_items),
            }

            if sig_hash:
                v2_sig_hashes.append(sig_hash)
            v2_records.append(rec_v2)

        except Exception:
            continue  # fail-soft per record

    info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    info["count"] = len(v2_records)
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)

    if v2_sig_hashes:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
        info["debug_v2_blocked"] = False
    else:
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}

    return info

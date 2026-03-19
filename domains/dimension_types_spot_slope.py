# -*- coding: utf-8 -*-
"""
Dimension Types - Spot Slope domain extractor.

Fingerprints SpotSlope dimension types.

Domain family: dimension_types
Contains shapes: SpotSlope

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
from core.rows import first_param, _as_string, _as_double
from core.canon import canon_str, S_MISSING, S_UNREADABLE
from core.record_v2 import (
    canonicalize_str,
    canonicalize_str_allow_empty,
    canonicalize_int,
    canonicalize_float,
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
    _fmt_in_from_ft,
    get_type_display_name,
    SHAPE_SPOT_SLOPE,
    SHAPE_SPOT_ELEVATION,
    SHAPE_SPOT_COORDINATE,
    FAMILY_SPOT,
)

try:
    from Autodesk.Revit.DB import DimensionType
except ImportError:
    DimensionType = None

DOMAIN_NAME = "dimension_types_spot_slope"

# Shapes handled by this domain
_HANDLED_SHAPES = frozenset({SHAPE_SPOT_SLOPE})


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


def extract(doc, ctx=None):
    """
    Extract SpotSlope dimension types fingerprint.

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

            # Slope Direction / Read Convention
            slope_direction_v, slope_direction_q = (None, ITEM_Q_MISSING)
            try:
                p_sd = first_param(d, ui_names=["Slope Direction", "Read Convention"])
                sd_raw = _as_string(p_sd) if p_sd is not None else None
                slope_direction_v, slope_direction_q = canonicalize_str_allow_empty(sd_raw)
            except Exception:
                slope_direction_v, slope_direction_q = (None, ITEM_Q_UNREADABLE)

            # Leader Line Length (stored in feet, convert to inches)
            leader_line_length_v, leader_line_length_q = (None, ITEM_Q_MISSING)
            try:
                p_lll = first_param(d, ui_names=["Leader Line Length"])
                lll_ft = _as_double(p_lll) if p_lll is not None else None
                if lll_ft is not None:
                    leader_line_length_v, leader_line_length_q = canonicalize_float(_fmt_in_from_ft(lll_ft))
                else:
                    leader_line_length_v, leader_line_length_q = (None, ITEM_Q_MISSING)
            except Exception:
                leader_line_length_v, leader_line_length_q = (None, ITEM_Q_UNREADABLE)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
                make_identity_item("dim_type.slope_direction", slope_direction_v, slope_direction_q),
                make_identity_item("dim_type.leader_line_length", leader_line_length_v, leader_line_length_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            # leader_line_length is optional enrichment — not blocking
            required_qs = [
                shape_q,
                unit_format_id_q,
                slope_direction_q,
            ]
            # text/appearance fields are cross-family alignment, not primary identity — not blocking

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


# -*- coding: utf-8 -*-
"""
Dimension Types domain family extractor.

One extract_* function per domain. All share module-level constants,
helper imports, and cached DimensionType collection.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.rows import first_param, _as_string, _as_value_string, _as_double, _as_int, format_len_inches
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
    _read_tick_mark_sig_hash,
    _read_unit_format_info,
    _read_prefix_suffix,
    get_type_display_name,
    SHAPE_LINEAR,
    SHAPE_LINEAR_FIXED,
    SHAPE_ARC_LENGTH,
    SHAPE_ANGULAR,
    SHAPE_RADIAL,
    SHAPE_DIAMETER,
    SHAPE_SPOT_ELEVATION,
    SHAPE_SPOT_ELEVATION_FIXED,
    SHAPE_SPOT_COORDINATE,
    SHAPE_ALIGNMENT_STATION_LABEL,
    SHAPE_SPOT_SLOPE,
    FAMILY_LINEAR,
    FAMILY_ANGULAR,
    FAMILY_RADIAL,
    FAMILY_SPOT,
)

try:
    from Autodesk.Revit.DB import DimensionType
except ImportError:
    DimensionType = None

_CTX_DIM_TYPES_CACHE_KEY = "_dim_types_cache"


def _collect_dim_types(doc, ctx):
    if ctx is not None and _CTX_DIM_TYPES_CACHE_KEY in ctx:
        return ctx[_CTX_DIM_TYPES_CACHE_KEY]
    types = list(
        collect_types(
            doc,
            of_class=DimensionType,
            require_unique_id=True,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            cache_key="dimension_types:DimensionType:types",
        )
    )
    if ctx is not None:
        ctx[_CTX_DIM_TYPES_CACHE_KEY] = types
    return types

def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
    """
    Heuristic override: if the FamilyName prefix indicates a Spot family,
    force Spot classification so we skip this record (spot shapes have their own domain).
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


def extract_linear(doc, ctx=None):
    """
    Extract Linear/LinearFixed/ArcLength dimension types fingerprint.

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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            # Get display name for heuristic override
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override to detect Spot types
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # Exclude confirmed wrong-family types (system/infrastructure types)
            # family_name=None means unreadable — do not exclude on absence
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name != EXPECTED_FAMILY:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             rounding_v, rounding_q,
             accuracy_v, accuracy_q) = _read_unit_format_info(d)

            # Prefix/Suffix
            prefix_v, prefix_q, suffix_v, suffix_q = _read_prefix_suffix(d)

            # Tick mark sig hash
            tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)

            # Witness line control (required for all shapes in this domain)
            witness_v, witness_q = (None, ITEM_Q_MISSING)
            try:
                p_wit = first_param(d, ui_names=["Witness Line Control", "Witness line control"])
                if p_wit is None:
                    witness_v, witness_q = (None, ITEM_Q_MISSING)
                else:
                    # Witness Line Control is Integer/enum — must use AsValueString(), not AsString()
                    witness_raw = _as_value_string(p_wit)
                    if witness_raw is not None and witness_raw.strip() == "":
                        witness_v, witness_q = (None, ITEM_Q_MISSING)
                    else:
                        witness_v, witness_q = canonicalize_str(witness_raw)
            except Exception:
                witness_v, witness_q = (None, ITEM_Q_UNREADABLE)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
                make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
                make_identity_item("dim_type.witness_line_control", witness_v, witness_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
                make_identity_item("dim_type.rounding", rounding_v, rounding_q),
                make_identity_item("dim_type.prefix", prefix_v, prefix_q),
                make_identity_item("dim_type.suffix", suffix_v, suffix_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            required_qs = [
                shape_q,
                accuracy_q,
                tick_sig_hash_q,
                unit_format_id_q,
                rounding_q,
                prefix_q,
                suffix_q,
            ]
            # witness_line_control: soft-required — only contributes to blocking if
            # successfully read (q=OK appended to list has no blocking effect; this
            # pattern ensures the field never blocks on lookup failure)
            if witness_q == ITEM_Q_OK:
                required_qs.append(witness_q)
            # text/appearance fields are cross-family alignment, not primary identity — not blocking

            blocked = any(q != ITEM_Q_OK for q in required_qs)

            status_reasons = []
            for it in identity_items:
                q = it.get("q")
                k = it.get("k", "")
                if q == ITEM_Q_OK:
                    continue
                # tick_mark_sig_hash missing is acceptable
                if q == ITEM_Q_MISSING and k == "dim_type.tick_mark_sig_hash":
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

            # Record ID from element ID integer
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

            # coordination_items
            coordination_items = [
                make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
            ]

            # unknown_items (traceability only)
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

def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
    """
    Heuristic override: if the FamilyName prefix indicates a Spot family,
    force Spot classification so we skip this record (spot shapes have their own domain).
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


def extract_angular(doc, ctx=None):
    """
    Extract Angular dimension types fingerprint.

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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override to detect Spot types
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # Exclude confirmed wrong-family types (system/infrastructure types)
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name != EXPECTED_FAMILY:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             rounding_v, rounding_q,
             accuracy_v, accuracy_q) = _read_unit_format_info(d)

            # Prefix/Suffix
            prefix_v, prefix_q, suffix_v, suffix_q = _read_prefix_suffix(d)

            # Tick mark sig hash
            tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)

            # Witness line control (Angular dimensions expose witness_line_control per spec)
            witness_v, witness_q = (None, ITEM_Q_MISSING)
            try:
                p_wit = first_param(d, ui_names=["Witness Line Control", "Witness line control"])
                if p_wit is None:
                    witness_v, witness_q = (None, ITEM_Q_MISSING)
                else:
                    # Witness Line Control is Integer/enum — must use AsValueString(), not AsString()
                    witness_raw = _as_value_string(p_wit)
                    if witness_raw is not None and witness_raw.strip() == "":
                        witness_v, witness_q = (None, ITEM_Q_MISSING)
                    else:
                        witness_v, witness_q = canonicalize_str(witness_raw)
            except Exception:
                witness_v, witness_q = (None, ITEM_Q_UNREADABLE)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
                make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
                make_identity_item("dim_type.witness_line_control", witness_v, witness_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
                make_identity_item("dim_type.rounding", rounding_v, rounding_q),
                make_identity_item("dim_type.prefix", prefix_v, prefix_q),
                make_identity_item("dim_type.suffix", suffix_v, suffix_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            # rounding, prefix, suffix are optional enrichment — not blocking for Angular
            required_qs = [
                shape_q,
                accuracy_q,
                tick_sig_hash_q,
                unit_format_id_q,
            ]
            # witness_line_control: soft-required — only contributes when successfully read
            if witness_q == ITEM_Q_OK:
                required_qs.append(witness_q)
            # text/appearance fields are cross-family alignment, not primary identity — not blocking
            blocked = any(q != ITEM_Q_OK for q in required_qs)

            status_reasons = []
            for it in identity_items:
                q = it.get("q")
                k = it.get("k", "")
                if q == ITEM_Q_OK:
                    continue
                if q == ITEM_Q_MISSING and k == "dim_type.tick_mark_sig_hash":
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

def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
    """
    Heuristic override: if the FamilyName prefix indicates a Spot family,
    force Spot classification so we skip this record (spot shapes have their own domain).
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


def extract_radial(doc, ctx=None):
    """
    Extract Radial dimension types fingerprint.

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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override to detect Spot types
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # Exclude confirmed wrong-family types (system/infrastructure types)
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name != EXPECTED_FAMILY:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             rounding_v, rounding_q,
             accuracy_v, accuracy_q) = _read_unit_format_info(d)

            # Tick mark sig hash
            tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)

            # Center marks (radial-specific)
            center_marks_v, center_marks_q = (None, ITEM_Q_MISSING)
            try:
                p_cm = first_param(d, ui_names=["Center Marks"])
                cm_int = _as_int(p_cm) if p_cm is not None else None
                if cm_int is not None:
                    center_marks_v, center_marks_q = canonicalize_str(safe_str(cm_int))
                    if center_marks_v is None:
                        center_marks_q = ITEM_Q_UNREADABLE
            except Exception:
                center_marks_v, center_marks_q = (None, ITEM_Q_UNREADABLE)

            # Center mark size (radial-specific), stored in feet, convert to inches
            center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
            try:
                p_cms = first_param(d, ui_names=["Center Mark Size"])
                cms_ft = _as_double(p_cms) if p_cms is not None else None
                if cms_ft is not None:
                    center_mark_size_v, center_mark_size_q = canonicalize_float(_fmt_in_from_ft(cms_ft))
                else:
                    center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
            except Exception:
                center_mark_size_v, center_mark_size_q = (None, ITEM_Q_UNREADABLE)

            # Radius symbol location
            radius_symbol_location_v, radius_symbol_location_q = (None, ITEM_Q_MISSING)
            try:
                p_rsl = first_param(d, ui_names=["Radius Symbol Location", "Symbol Location"])
                rsl_raw = _as_string(p_rsl) if p_rsl is not None else None
                radius_symbol_location_v, radius_symbol_location_q = canonicalize_str_allow_empty(rsl_raw)
            except Exception:
                radius_symbol_location_v, radius_symbol_location_q = (None, ITEM_Q_UNREADABLE)

            # Radius symbol text
            radius_symbol_text_v, radius_symbol_text_q = (None, ITEM_Q_MISSING)
            try:
                p_rst = first_param(d, ui_names=["Radius Symbol Text"])
                rst_raw = _as_string(p_rst) if p_rst is not None else None
                radius_symbol_text_v, radius_symbol_text_q = canonicalize_str_allow_empty(rst_raw)
            except Exception:
                radius_symbol_text_v, radius_symbol_text_q = (None, ITEM_Q_UNREADABLE)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
                make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
                make_identity_item("dim_type.center_marks", center_marks_v, center_marks_q),
                make_identity_item("dim_type.center_mark_size", center_mark_size_v, center_mark_size_q),
                make_identity_item("dim_type.radius_symbol_location", radius_symbol_location_v, radius_symbol_location_q),
                make_identity_item("dim_type.radius_symbol_text", radius_symbol_text_v, radius_symbol_text_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            # radius_symbol_location, radius_symbol_text are optional enrichment — not blocking
            required_qs = [
                shape_q,
                accuracy_q,
                center_marks_q,
                center_mark_size_q,
                unit_format_id_q,
            ]
            # text/appearance fields are cross-family alignment, not primary identity — not blocking

            blocked = any(q != ITEM_Q_OK for q in required_qs)

            status_reasons = []
            for it in identity_items:
                q = it.get("q")
                k = it.get("k", "")
                if q == ITEM_Q_OK:
                    continue
                if q == ITEM_Q_MISSING and k == "dim_type.tick_mark_sig_hash":
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

def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
    """
    Heuristic override: if the FamilyName prefix indicates a Spot family,
    force Spot classification so we skip this record (spot shapes have their own domain).
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


def extract_diameter(doc, ctx=None):
    """
    Extract Diameter dimension types fingerprint.

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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override to detect Spot types
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Note: no shape-based filter here. Revit's DimensionStyleType enum maps
            # some Diameter types to SpotElevationFixed (integer collision in the enum).
            # Family name is the sole authoritative gate for this domain.

            # Exclude confirmed wrong-family types (e.g. Alignment Station Labels)
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name != EXPECTED_FAMILY:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             rounding_v, rounding_q,
             accuracy_v, accuracy_q) = _read_unit_format_info(d)

            # Tick mark sig hash
            tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)

            # Center marks (radial-family specific)
            center_marks_v, center_marks_q = (None, ITEM_Q_MISSING)
            try:
                p_cm = first_param(d, ui_names=["Center Marks"])
                cm_int = _as_int(p_cm) if p_cm is not None else None
                if cm_int is not None:
                    center_marks_v, center_marks_q = canonicalize_str(safe_str(cm_int))
                    if center_marks_v is None:
                        center_marks_q = ITEM_Q_UNREADABLE
            except Exception:
                center_marks_v, center_marks_q = (None, ITEM_Q_UNREADABLE)

            # Center mark size (radial-family specific), stored in feet, convert to inches
            center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
            try:
                p_cms = first_param(d, ui_names=["Center Mark Size"])
                cms_ft = _as_double(p_cms) if p_cms is not None else None
                if cms_ft is not None:
                    center_mark_size_v, center_mark_size_q = canonicalize_float(_fmt_in_from_ft(cms_ft))
                else:
                    center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
            except Exception:
                center_mark_size_v, center_mark_size_q = (None, ITEM_Q_UNREADABLE)

            # Diameter symbol location
            diameter_symbol_location_v, diameter_symbol_location_q = (None, ITEM_Q_MISSING)
            try:
                p_dsl = first_param(d, ui_names=["Diameter Symbol Location", "Symbol Location"])
                dsl_raw = _as_string(p_dsl) if p_dsl is not None else None
                diameter_symbol_location_v, diameter_symbol_location_q = canonicalize_str_allow_empty(dsl_raw)
            except Exception:
                diameter_symbol_location_v, diameter_symbol_location_q = (None, ITEM_Q_UNREADABLE)

            # Diameter symbol text
            diameter_symbol_text_v, diameter_symbol_text_q = (None, ITEM_Q_MISSING)
            try:
                p_dst = first_param(d, ui_names=["Diameter Symbol Text"])
                dst_raw = _as_string(p_dst) if p_dst is not None else None
                diameter_symbol_text_v, diameter_symbol_text_q = canonicalize_str_allow_empty(dst_raw)
            except Exception:
                diameter_symbol_text_v, diameter_symbol_text_q = (None, ITEM_Q_UNREADABLE)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
                make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
                make_identity_item("dim_type.center_marks", center_marks_v, center_marks_q),
                make_identity_item("dim_type.center_mark_size", center_mark_size_v, center_mark_size_q),
                make_identity_item("dim_type.diameter_symbol_location", diameter_symbol_location_v, diameter_symbol_location_q),
                make_identity_item("dim_type.diameter_symbol_text", diameter_symbol_text_v, diameter_symbol_text_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            # diameter_symbol_location, diameter_symbol_text are optional enrichment — not blocking
            required_qs = [
                shape_q,
                accuracy_q,
                center_marks_q,
                center_mark_size_q,
                unit_format_id_q,
            ]
            # text/appearance fields are cross-family alignment, not primary identity — not blocking

            blocked = any(q != ITEM_Q_OK for q in required_qs)

            status_reasons = []
            for it in identity_items:
                q = it.get("q")
                k = it.get("k", "")
                if q == ITEM_Q_OK:
                    continue
                if q == ITEM_Q_MISSING and k == "dim_type.tick_mark_sig_hash":
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


def _read_symbol_name(d, doc):
    """
    Try to read the "Symbol" parameter that references a loaded family.
    If ElementId > 0, resolve element and return its name directly.
    Returns (symbol_name_v, symbol_name_q).
    """
    try:
        p_sym = first_param(d, ui_names=["Symbol"])
        if p_sym is None:
            return (None, ITEM_Q_MISSING)

        if not getattr(p_sym, "HasValue", False):
            return (None, ITEM_Q_MISSING)

        eid = None
        try:
            eid = p_sym.AsElementId()
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if eid is None or getattr(eid, "IntegerValue", -1) <= 0:
            return (None, ITEM_Q_MISSING)

        sym_elem = None
        try:
            sym_elem = doc.GetElement(eid)
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if sym_elem is None:
            return (None, ITEM_Q_MISSING)

        sym_name = None
        try:
            sym_name = getattr(sym_elem, "Name", None)
        except Exception:
            pass

        if sym_name:
            return canonicalize_str(str(sym_name))
        return (None, ITEM_Q_MISSING)

    except Exception:
        return (None, ITEM_Q_UNREADABLE)


def extract_spot_elevation(doc, ctx=None):
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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # Exclude confirmed wrong-family types (e.g. Diameter types misrouted via SpotElevationFixed)
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name != EXPECTED_FAMILY:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
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
                p_eip = first_param(d, ui_names=["Elevation Indicator as Prefix/Suffix", "Elevation Indicator as Prefix/S"])
                eip_int = _as_int(p_eip) if p_eip is not None else None
                elev_ind_prefix_v, elev_ind_prefix_q = canonicalize_bool(eip_int)
            except Exception:
                elev_ind_prefix_v, elev_ind_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Top Indicator
            top_indicator_v, top_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_top = first_param(d, ui_names=["Top Indicator"])
                top_raw = _as_string(p_top) if p_top is not None else None
                top_indicator_v, top_indicator_q = canonicalize_str_allow_empty(top_raw)
            except Exception:
                top_indicator_v, top_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Bottom Indicator
            bottom_indicator_v, bottom_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_bot = first_param(d, ui_names=["Bottom Indicator"])
                bot_raw = _as_string(p_bot) if p_bot is not None else None
                bottom_indicator_v, bottom_indicator_q = canonicalize_str_allow_empty(bot_raw)
            except Exception:
                bottom_indicator_v, bottom_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Top Indicator as Prefix/Suffix
            top_ind_prefix_v, top_ind_prefix_q = (None, ITEM_Q_MISSING)
            try:
                p_tip = first_param(d, ui_names=["Top Indicator as Prefix/Suffix"])
                tip_int = _as_int(p_tip) if p_tip is not None else None
                top_ind_prefix_v, top_ind_prefix_q = canonicalize_bool(tip_int)
            except Exception:
                top_ind_prefix_v, top_ind_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Bottom Indicator as Prefix/Suffix
            bot_ind_prefix_v, bot_ind_prefix_q = (None, ITEM_Q_MISSING)
            try:
                p_bip = first_param(d, ui_names=["Bottom Indicator as Prefix/Suffix", "Bottom Indicator as Prefix/Suf"])
                bip_int = _as_int(p_bip) if p_bip is not None else None
                bot_ind_prefix_v, bot_ind_prefix_q = canonicalize_bool(bip_int)
            except Exception:
                bot_ind_prefix_v, bot_ind_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Text Orientation (storage=Integer/enum — use AsValueString)
            text_orientation_v, text_orientation_q = (None, ITEM_Q_MISSING)
            try:
                p_to = first_param(d, ui_names=["Text Orientation"])
                to_raw = _as_value_string(p_to) if p_to is not None else None
                text_orientation_v, text_orientation_q = canonicalize_str(to_raw)
            except Exception:
                text_orientation_v, text_orientation_q = (None, ITEM_Q_UNREADABLE)

            # Text Location (storage=Integer/enum — use AsValueString; probe name is "Text Location")
            text_location_v, text_location_q = (None, ITEM_Q_MISSING)
            try:
                p_tl = first_param(d, ui_names=["Text Location", "Note Location"])
                tl_raw = _as_value_string(p_tl) if p_tl is not None else None
                text_location_v, text_location_q = canonicalize_str(tl_raw)
            except Exception:
                text_location_v, text_location_q = (None, ITEM_Q_UNREADABLE)

            # Symbol name (ElementId resolved to name; no ctx map available for sig_hash)
            symbol_name_v, symbol_name_q = _read_symbol_name(d, doc)

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
                make_identity_item("dim_type.symbol_name", symbol_name_v, symbol_name_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            # Indicator fields, text placement, and symbol_name are non-blocking:
            # SpotElevationFixed records may not expose all indicator params,
            # and missing optional fields should degrade (not block) a record.
            required_qs = [
                shape_q,
                unit_format_id_q,
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


def _read_symbol_name(d, doc):
    """
    Try to read the "Symbol" parameter that references a loaded family.
    If ElementId > 0, resolve element and return its name directly.
    Returns (symbol_name_v, symbol_name_q).
    """
    try:
        p_sym = first_param(d, ui_names=["Symbol"])
        if p_sym is None:
            return (None, ITEM_Q_MISSING)

        if not getattr(p_sym, "HasValue", False):
            return (None, ITEM_Q_MISSING)

        eid = None
        try:
            eid = p_sym.AsElementId()
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if eid is None or getattr(eid, "IntegerValue", -1) <= 0:
            return (None, ITEM_Q_MISSING)

        sym_elem = None
        try:
            sym_elem = doc.GetElement(eid)
        except Exception:
            return (None, ITEM_Q_UNREADABLE)

        if sym_elem is None:
            return (None, ITEM_Q_MISSING)

        sym_name = None
        try:
            sym_name = getattr(sym_elem, "Name", None)
        except Exception:
            pass

        if sym_name:
            return canonicalize_str(str(sym_name))
        return (None, ITEM_Q_MISSING)

    except Exception:
        return (None, ITEM_Q_UNREADABLE)


def extract_spot_coordinate(doc, ctx=None):
    """
    Extract SpotCoordinate dimension types fingerprint.

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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # Exclude confirmed wrong-family types (system/infrastructure types)
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name not in ACCEPTED_FAMILIES:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             _rounding_v, _rounding_q,
             _accuracy_v, _accuracy_q) = _read_unit_format_info(d)

            # Top Coordinate (storage=Integer/enum, display='North / South' — use AsValueString)
            top_coordinate_v, top_coordinate_q = (None, ITEM_Q_MISSING)
            try:
                p_tc = first_param(d, ui_names=["Top Coordinate", "Top Value"])
                tc_raw = _as_value_string(p_tc) if p_tc is not None else None
                top_coordinate_v, top_coordinate_q = canonicalize_str(tc_raw)
            except Exception:
                top_coordinate_v, top_coordinate_q = (None, ITEM_Q_UNREADABLE)

            # Bottom Coordinate (storage=Integer/enum, display='East / West' — use AsValueString)
            bottom_coordinate_v, bottom_coordinate_q = (None, ITEM_Q_MISSING)
            try:
                p_bc = first_param(d, ui_names=["Bottom Coordinate", "Bottom Value"])
                bc_raw = _as_value_string(p_bc) if p_bc is not None else None
                bottom_coordinate_v, bottom_coordinate_q = canonicalize_str(bc_raw)
            except Exception:
                bottom_coordinate_v, bottom_coordinate_q = (None, ITEM_Q_UNREADABLE)

            # N/S Indicator
            north_south_indicator_v, north_south_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_ns = first_param(d, ui_names=["North / South Indicator", "N/S Indicator"])
                ns_raw = _as_string(p_ns) if p_ns is not None else None
                north_south_indicator_v, north_south_indicator_q = canonicalize_str_allow_empty(ns_raw)
            except Exception:
                north_south_indicator_v, north_south_indicator_q = (None, ITEM_Q_UNREADABLE)

            # E/W Indicator
            east_west_indicator_v, east_west_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_ew = first_param(d, ui_names=["East / West Indicator", "E/W Indicator"])
                ew_raw = _as_string(p_ew) if p_ew is not None else None
                east_west_indicator_v, east_west_indicator_q = canonicalize_str_allow_empty(ew_raw)
            except Exception:
                east_west_indicator_v, east_west_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Include Elevation
            include_elevation_v, include_elevation_q = (None, ITEM_Q_MISSING)
            try:
                p_ie = first_param(d, ui_names=["Include Elevation"])
                ie_int = _as_int(p_ie) if p_ie is not None else None
                include_elevation_v, include_elevation_q = canonicalize_bool(ie_int)
            except Exception:
                include_elevation_v, include_elevation_q = (None, ITEM_Q_UNREADABLE)

            # Elevation Indicator
            elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_MISSING)
            try:
                p_ei = first_param(d, ui_names=["Elevation Indicator"])
                ei_raw = _as_string(p_ei) if p_ei is not None else None
                elevation_indicator_v, elevation_indicator_q = canonicalize_str_allow_empty(ei_raw)
            except Exception:
                elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_UNREADABLE)

            # Indicator as Prefix/Suffix
            indicator_prefix_v, indicator_prefix_q = (None, ITEM_Q_MISSING)
            try:
                p_ip = first_param(d, ui_names=["Indicator as Prefix / Suffix", "Indicator as Prefix/Suffix"])
                ip_int = _as_int(p_ip) if p_ip is not None else None
                indicator_prefix_v, indicator_prefix_q = canonicalize_bool(ip_int)
            except Exception:
                indicator_prefix_v, indicator_prefix_q = (None, ITEM_Q_UNREADABLE)

            # Text Orientation (storage=Integer/enum — use AsValueString)
            text_orientation_v, text_orientation_q = (None, ITEM_Q_MISSING)
            try:
                p_to = first_param(d, ui_names=["Text Orientation"])
                to_raw = _as_value_string(p_to) if p_to is not None else None
                text_orientation_v, text_orientation_q = canonicalize_str(to_raw)
            except Exception:
                text_orientation_v, text_orientation_q = (None, ITEM_Q_UNREADABLE)

            # Text Location (storage=Integer/enum — use AsValueString; probe name is "Text Location")
            text_location_v, text_location_q = (None, ITEM_Q_MISSING)
            try:
                p_tl = first_param(d, ui_names=["Text Location", "Note Location"])
                tl_raw = _as_value_string(p_tl) if p_tl is not None else None
                text_location_v, text_location_q = canonicalize_str(tl_raw)
            except Exception:
                text_location_v, text_location_q = (None, ITEM_Q_UNREADABLE)

            # Symbol name (ElementId resolved to name; no ctx map available for sig_hash)
            symbol_name_v, symbol_name_q = _read_symbol_name(d, doc)

            # --- Build identity items ---
            core_items = [
                make_identity_item("dim_type.shape", shape_v, shape_q),
                make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
                make_identity_item("dim_type.top_coordinate", top_coordinate_v, top_coordinate_q),
                make_identity_item("dim_type.bottom_coordinate", bottom_coordinate_v, bottom_coordinate_q),
                make_identity_item("dim_type.north_south_indicator", north_south_indicator_v, north_south_indicator_q),
                make_identity_item("dim_type.east_west_indicator", east_west_indicator_v, east_west_indicator_q),
                make_identity_item("dim_type.include_elevation", include_elevation_v, include_elevation_q),
                make_identity_item("dim_type.elevation_indicator", elevation_indicator_v, elevation_indicator_q),
                make_identity_item("dim_type.indicator_as_prefix_suffix", indicator_prefix_v, indicator_prefix_q),
                make_identity_item("dim_type.text_orientation", text_orientation_v, text_orientation_q),
                make_identity_item("dim_type.text_location", text_location_v, text_location_q),
                make_identity_item("dim_type.symbol_name", symbol_name_v, symbol_name_q),
            ]

            text_items = _build_text_appearance_items(d)
            all_items = core_items + text_items

            identity_items = sorted(all_items, key=lambda it: it.get("k", ""))

            # Required qualities for blocking
            # include_elevation, elevation_indicator, indicator_prefix, symbol_name are optional — not blocking
            required_qs = [
                shape_q,
                unit_format_id_q,
                top_coordinate_q,
                bottom_coordinate_q,
                north_south_indicator_q,
                east_west_indicator_q,
                text_orientation_q,
                text_location_q,
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


def extract_spot_slope(doc, ctx=None):
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
        all_types = _collect_dim_types(doc, ctx)
    except Exception:
        all_types = []

    info["raw_count"] = len(all_types)

    v2_records = []
    v2_sig_hashes = []

    for d in all_types:
        try:
            type_name = get_type_display_name(d)

            # Exclude system built-in types with id-based labels (not user-accessible)
            if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
                info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
                continue

            shape_v, shape_family, shape_q = _get_dimension_shape(d)

            # Apply family-name heuristic override
            shape_v, shape_family, shape_q = _apply_family_name_override(
                d, shape_v, shape_family, shape_q, type_name
            )

            # Filter: skip shapes not handled by this domain
            if shape_v not in _HANDLED_SHAPES:
                continue

            # Exclude confirmed wrong-family types (system/infrastructure types)
            family_name = None
            try:
                p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
                if p_fam:
                    family_name = _as_string(p_fam)
                    if family_name:
                        family_name = canon_str(family_name)
            except Exception:
                pass
            if family_name and family_name != EXPECTED_FAMILY:
                info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
                continue

            # --- Read core identity fields ---

            # Unit format info
            (unit_format_id_v, unit_format_id_q,
             _rounding_v, _rounding_q,
             _accuracy_v, _accuracy_q) = _read_unit_format_info(d)

            # Slope Direction / Read Convention (storage=Integer/enum — use AsValueString)
            slope_direction_v, slope_direction_q = (None, ITEM_Q_MISSING)
            try:
                p_sd = first_param(d, ui_names=["Slope Direction", "Read Convention"])
                # Probe confirms storage=Integer (display='Down') — must use AsValueString
                sd_raw = _as_value_string(p_sd) if p_sd is not None else None
                slope_direction_v, slope_direction_q = canonicalize_str(sd_raw)
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

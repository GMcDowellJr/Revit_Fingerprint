# -*- coding: utf-8 -*-
"""Floor types domain extractor (floor_types).

Reads FloorType compound-structure and behavioral data via
domains/compound_layers.py's shared helpers.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types, purge_lookup
from core.canon import S_UNREADABLE
from core.sig_hash_policy import resolve_sig_hash_keys
from core.record_v2 import (
    STATUS_OK,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    canonicalize_str,
    canonicalize_int,
    canonicalize_float,
    canonicalize_bool,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from domains.compound_layers import (
    _enum_name,
    _build_name_key,
    _build_instance_count_map,
    _attach_placeholder_metadata,
    _read_compound_structure,
    _read_type_name,
    _label_for_type,
    _require_compound_dependencies,
    _coarse_fill_reads,
)

try:
    from Autodesk.Revit.DB import BuiltInCategory
except ImportError:
    BuiltInCategory = None

try:
    from Autodesk.Revit.DB import FloorType
except ImportError:
    FloorType = None

try:
    from Autodesk.Revit.DB import FloorFunction
except (ImportError, AttributeError):
    FloorFunction = None

_DOMAIN_FLOOR = "floor_types"
_FLOOR_FUNCTION_NAMES = {0: "Interior", 1: "Exterior"}

# Fallback sig_hash preimage key set, used only when ctx["sig_hash_policies"]
# is unavailable. Must stay in sync with policies/domain_sig_hash_policies.json's
# floor_types.allowed_items -- see core/sig_hash_policy.py's resolve_sig_hash_keys()
# and DECISIONS.md D-039/D-040.
_FLOOR_TYPES_SIG_HASH_KEYS_FALLBACK = [
    "ft.layer_count",
    "ft.total_thickness_in",
    "ft.stack_hash_loose",
]


def extract_floor_types(doc, ctx=None):
    info = {
        "count": 0,
        "raw_count": 0,
        "hash_v2": None,
        "records": [],
        "record_rows": [],
        "signature_hashes_v2": [],
        "status": "ok",
        "debug_blocked_no_cs": 0,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    if ctx is None:
        ctx = {}

    if not _require_compound_dependencies(ctx, info):
        return info

    if FloorType is None:
        info["status"] = "blocked"
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"api_unreachable": 1}
        return info

    try:
        floor_types = list(
            collect_types(
                doc,
                of_class=FloorType,
                cctx=(ctx or {}).get("_collect"),
                cache_key="compound_types:floor_types:FloorType:types",
            )
        )
    except Exception:
        floor_types = []

    info["raw_count"] = len(floor_types)
    _total_type_count = len(floor_types)
    _instance_count_map, _instance_count_map_q = _build_instance_count_map(
        doc, ctx, getattr(BuiltInCategory, "OST_Floors", None), "compound_types.floor.instances"
    )

    fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_sig_hash_v2", None)
    if not isinstance(fp_uid_to_sig_hash, dict):
        fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_hash", {}) or {}

    records = []
    sigs = []

    for ft in floor_types:
        type_name = _read_type_name(ft)

        try:
            cs = ft.GetCompoundStructure()
        except Exception:
            cs = None

        if cs is None:
            blocked_items = sorted([
                make_identity_item("ft.type_name", type_name, ITEM_Q_OK),
                make_identity_item("ft.layer_count", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
                make_identity_item("ft.total_thickness_in", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
                make_identity_item("ft.stack_hash_loose", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
            ], key=lambda it: safe_str(it.get("k", "")))
            rec = build_record_v2(
                domain=_DOMAIN_FLOOR,
                record_id="floor_type|{}".format(type_name),
                status=STATUS_BLOCKED,
                status_reasons=["no_compound_structure"],
                sig_hash=None,
                identity_items=blocked_items,
                required_qs=[ITEM_Q_OK],
                label=_label_for_type(type_name),
            )
            rec["join_key_name_identity"] = _build_name_key(ctx, "floor_types", blocked_items)
            _ip, _ip_q = purge_lookup(getattr(getattr(ft, "Id", None), "IntegerValue", None), ctx)
            rec["is_purgeable"] = _ip
            rec["is_purgeable_q"] = _ip_q
            _attach_placeholder_metadata(rec, ft, _instance_count_map, _instance_count_map_q, _total_type_count)
            rec["layer_rows"] = []
            records.append(rec)
            info["debug_blocked_no_cs"] += 1
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"]["no_compound_structure"] = (
                info["debug_v2_block_reasons"].get("no_compound_structure", 0) + 1
            )
            continue

        cs_data = _read_compound_structure(cs, doc, ctx, "floor")

        try:
            raw = getattr(ft, "Function", None)
            ft_function = _enum_name(FloorFunction, int(str(raw)), _FLOOR_FUNCTION_NAMES)
            ft_function_q = ITEM_Q_OK
        except Exception:
            ft_function = S_UNREADABLE
            ft_function_q = ITEM_Q_UNREADABLE

        cfpsh_v, cfpsh_q, cfc_v, cfc_q = _coarse_fill_reads(ft, doc, fp_uid_to_sig_hash, ctx)

        sweeps_v, sweeps_q = (None, ITEM_Q_UNREADABLE)
        try:
            sweeps = cs.GetWallSweepsInfo()
            sweeps_v, sweeps_q = canonicalize_bool(len(list(sweeps or [])) > 0)
        except Exception:
            sweeps_v, sweeps_q = (None, ITEM_Q_UNREADABLE)

        if cs_data.get("has_unreadable_thickness", False):
            total_thickness_v, total_thickness_q = (None, ITEM_Q_UNREADABLE)
        else:
            total_thickness_v, total_thickness_q = canonicalize_float(cs_data["total_thickness_in"], nd=4)

        semantic = [
            make_identity_item("ft.layer_count", *canonicalize_int(cs_data["layer_count"])),
            make_identity_item("ft.total_thickness_in", total_thickness_v, total_thickness_q),
            make_identity_item("ft.stack_hash_loose", *canonicalize_str(cs_data["stack_hash_loose"])),
        ]
        coordination = [
            make_identity_item("ft.function", ft_function if ft_function != S_UNREADABLE else None, ft_function_q),
            make_identity_item("ft.total_layer_rows", *canonicalize_int(cs_data["total_layer_rows"])),
            make_identity_item("ft.stack_hash_strict", *canonicalize_str(cs_data["stack_hash_strict"])),
            make_identity_item("ft.stack_hash_function_only", *canonicalize_str(cs_data["stack_hash_function_only"])),
            make_identity_item("ft.coarse_fill_pattern_sig_hash", cfpsh_v, cfpsh_q),
            make_identity_item("ft.has_embedded_sweeps", sweeps_v, sweeps_q),
        ]
        cosmetic = [
            make_identity_item("ft.type_name", *canonicalize_str(type_name)),
            make_identity_item("ft.coarse_fill_color_rgb", cfc_v, cfc_q),
        ]

        identity_items = sorted((semantic + coordination + cosmetic), key=lambda it: safe_str(it.get("k", "")))
        required_keys = {"ft.layer_count", "ft.total_thickness_in", "ft.stack_hash_loose"}
        required_qs = [it.get("q") for it in semantic if safe_str(it.get("k", "")) in required_keys]
        required_not_ok = any(q != ITEM_Q_OK for q in required_qs)
        status = STATUS_BLOCKED if required_not_ok else STATUS_OK
        status_reasons = ["required_identity_not_ok"] if required_not_ok else []
        sig_hash_keys = set(resolve_sig_hash_keys(
            (ctx or {}).get("sig_hash_policies"),
            _DOMAIN_FLOOR,
            [it.get("k") for it in identity_items],
            _FLOOR_TYPES_SIG_HASH_KEYS_FALLBACK,
        ))
        sig_hash_items = [it for it in identity_items if safe_str(it.get("k", "")) in sig_hash_keys]
        sig_hash = None if required_not_ok else make_hash(serialize_identity_items(sig_hash_items))

        rec = build_record_v2(
            domain=_DOMAIN_FLOOR,
            record_id="floor_type|{}".format(type_name),
            status=status,
            status_reasons=status_reasons,
            sig_hash=sig_hash,
            identity_items=identity_items,
            required_qs=required_qs,
            label=_label_for_type(type_name),
        )
        rec["join_key_name_identity"] = _build_name_key(ctx, "floor_types", identity_items)
        _ip, _ip_q = purge_lookup(getattr(getattr(ft, "Id", None), "IntegerValue", None), ctx)
        rec["is_purgeable"] = _ip
        rec["is_purgeable_q"] = _ip_q
        _attach_placeholder_metadata(rec, ft, _instance_count_map, _instance_count_map_q, _total_type_count)
        rec["sig_basis"] = {
            "schema": "floor_types.sig_basis.v1",
            "keys_used": ["ft.layer_count", "ft.total_thickness_in", "ft.stack_hash_loose"],
        }
        rec["layer_rows"] = cs_data["layer_rows"]
        records.append(rec)
        if sig_hash is not None:
            sigs.append(sig_hash)
            info["count"] += 1
        else:
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"]["required_identity_not_ok"] = (
                info["debug_v2_block_reasons"].get("required_identity_not_ok", 0) + 1
            )

    info["records"] = records
    info["signature_hashes_v2"] = sorted([s for s in sigs if s])
    info["record_rows"] = [
        {
            "record_key": safe_str(r.get("record_id", "")),
            "sig_hash": r.get("sig_hash", None),
            "name": ((r.get("label", {}) or {}).get("display", None) if isinstance(r.get("label", {}), dict) else None),
        }
        for r in records
    ]
    if info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])
    else:
        info["hash_v2"] = None
    return info

# -*- coding: utf-8 -*-
"""Roof types domain extractor (roof_types).

Reads RoofType compound-structure and behavioral data via
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
from core.record_v2 import (
    STATUS_OK,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    canonicalize_str,
    canonicalize_int,
    canonicalize_float,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from domains.compound_layers import (
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
    from Autodesk.Revit.DB import RoofType
except ImportError:
    RoofType = None

_DOMAIN_ROOF = "roof_types"


def extract_roof_types(doc, ctx=None):
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

    if RoofType is None:
        info["status"] = "blocked"
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"api_unreachable": 1}
        return info

    try:
        roof_types = list(
            collect_types(
                doc,
                of_class=RoofType,
                cctx=(ctx or {}).get("_collect"),
                cache_key="compound_types:roof_types:RoofType:types",
            )
        )
    except Exception:
        roof_types = []

    info["raw_count"] = len(roof_types)
    _total_type_count = len(roof_types)
    _instance_count_map, _instance_count_map_q = _build_instance_count_map(
        doc, ctx, getattr(BuiltInCategory, "OST_Roofs", None), "compound_types.roof.instances"
    )

    fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_sig_hash_v2", None)
    if not isinstance(fp_uid_to_sig_hash, dict):
        fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_hash", {}) or {}

    records = []
    sigs = []

    for rt in roof_types:
        type_name = _read_type_name(rt)

        try:
            cs = rt.GetCompoundStructure()
        except Exception:
            cs = None

        if cs is None:
            blocked_items = sorted([
                make_identity_item("rt.type_name", type_name, ITEM_Q_OK),
                make_identity_item("rt.layer_count", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
                make_identity_item("rt.total_thickness_in", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
                make_identity_item("rt.stack_hash_loose", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
            ], key=lambda it: safe_str(it.get("k", "")))
            rec = build_record_v2(
                domain=_DOMAIN_ROOF,
                record_id="roof_type|{}".format(type_name),
                status=STATUS_BLOCKED,
                status_reasons=["no_compound_structure"],
                sig_hash=None,
                identity_items=blocked_items,
                required_qs=[ITEM_Q_OK],
                label=_label_for_type(type_name),
            )
            rec["join_key_name_identity"] = _build_name_key(ctx, "roof_types", blocked_items)
            _ip, _ip_q = purge_lookup(getattr(getattr(rt, "Id", None), "IntegerValue", None), ctx)
            rec["is_purgeable"] = _ip
            rec["is_purgeable_q"] = _ip_q
            _attach_placeholder_metadata(rec, rt, _instance_count_map, _instance_count_map_q, _total_type_count)
            rec["layer_rows"] = []
            records.append(rec)
            info["debug_blocked_no_cs"] += 1
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"]["no_compound_structure"] = (
                info["debug_v2_block_reasons"].get("no_compound_structure", 0) + 1
            )
            continue

        cs_data = _read_compound_structure(cs, doc, ctx, "roof")
        cfpsh_v, cfpsh_q, cfc_v, cfc_q = _coarse_fill_reads(rt, doc, fp_uid_to_sig_hash, ctx)

        if cs_data.get("has_unreadable_thickness", False):
            total_thickness_v, total_thickness_q = (None, ITEM_Q_UNREADABLE)
        else:
            total_thickness_v, total_thickness_q = canonicalize_float(cs_data["total_thickness_in"], nd=4)

        semantic = [
            make_identity_item("rt.layer_count", *canonicalize_int(cs_data["layer_count"])),
            make_identity_item("rt.total_thickness_in", total_thickness_v, total_thickness_q),
            make_identity_item("rt.stack_hash_loose", *canonicalize_str(cs_data["stack_hash_loose"])),
        ]
        coordination = [
            make_identity_item("rt.total_layer_rows", *canonicalize_int(cs_data["total_layer_rows"])),
            make_identity_item("rt.stack_hash_strict", *canonicalize_str(cs_data["stack_hash_strict"])),
            make_identity_item("rt.stack_hash_function_only", *canonicalize_str(cs_data["stack_hash_function_only"])),
            make_identity_item("rt.coarse_fill_pattern_sig_hash", cfpsh_v, cfpsh_q),
        ]
        cosmetic = [
            make_identity_item("rt.type_name", *canonicalize_str(type_name)),
            make_identity_item("rt.coarse_fill_color_rgb", cfc_v, cfc_q),
        ]

        identity_items = sorted((semantic + coordination + cosmetic), key=lambda it: safe_str(it.get("k", "")))
        required_keys = {"rt.layer_count", "rt.total_thickness_in", "rt.stack_hash_loose"}
        required_qs = [it.get("q") for it in semantic if safe_str(it.get("k", "")) in required_keys]
        required_not_ok = any(q != ITEM_Q_OK for q in required_qs)
        status = STATUS_BLOCKED if required_not_ok else STATUS_OK
        status_reasons = ["required_identity_not_ok"] if required_not_ok else []
        sig_hash = None if required_not_ok else make_hash(serialize_identity_items(semantic))

        rec = build_record_v2(
            domain=_DOMAIN_ROOF,
            record_id="roof_type|{}".format(type_name),
            status=status,
            status_reasons=status_reasons,
            sig_hash=sig_hash,
            identity_items=identity_items,
            required_qs=required_qs,
            label=_label_for_type(type_name),
        )
        rec["join_key_name_identity"] = _build_name_key(ctx, "roof_types", identity_items)
        _ip, _ip_q = purge_lookup(getattr(getattr(rt, "Id", None), "IntegerValue", None), ctx)
        rec["is_purgeable"] = _ip
        rec["is_purgeable_q"] = _ip_q
        _attach_placeholder_metadata(rec, rt, _instance_count_map, _instance_count_map_q, _total_type_count)
        rec["sig_basis"] = {
            "schema": "roof_types.sig_basis.v1",
            "keys_used": ["rt.layer_count", "rt.total_thickness_in", "rt.stack_hash_loose"],
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

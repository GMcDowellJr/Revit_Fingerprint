# -*- coding: utf-8 -*-
"""Wall types domain extractor (wall_types).

Reads WallType compound-structure and behavioral data via
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
from core.canon import S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
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
from domains.materials import CTX_MATERIAL_UID_TO_NAME, CTX_MATERIAL_UID_TO_CLASS
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
    from Autodesk.Revit.DB import (
        WallType,
        WallKind,
        WallFunction,
        BuiltInCategory,
    )
except ImportError:
    WallType = None
    WallKind = None
    WallFunction = None
    BuiltInCategory = None

_DOMAIN_WALL = "wall_types"
_WALL_KIND_BASIC = 0
_WALL_KIND_STACKED = 1
_WALL_KIND_CURTAIN = 2
_WALL_KIND_NAMES = {
    _WALL_KIND_BASIC: "Basic",
    _WALL_KIND_STACKED: "Stacked",
    _WALL_KIND_CURTAIN: "Curtain",
}


_WALL_FUNCTION_NAMES = {
    0: "Interior", 1: "Exterior", 2: "Foundation",
    3: "Retaining", 4: "Soffit", 5: "Coreshaft",
}


def _canon_non_sentinel_str(v):
    try:
        if v in (S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE):
            return None, ITEM_Q_UNREADABLE
    except Exception:
        pass
    return canonicalize_str(v)


def _read_wall_kind(wt):
    kind_raw = getattr(wt, "Kind", None)
    if kind_raw is None:
        return -1, S_UNREADABLE
    try:
        # str() round-trip is required in Dynamo CPython3 — .NET Int32
        # does not satisfy == against Python int without explicit conversion
        kind_int = int(str(kind_raw))
        return kind_int, _WALL_KIND_NAMES.get(kind_int, str(kind_raw))
    except Exception:
        pass
    return -1, safe_str(kind_raw)


def _blocked_required_items(wt_function_v=None, wt_function_q=ITEM_Q_UNREADABLE):
    return [
        make_identity_item("wt.function", wt_function_v, wt_function_q),
        make_identity_item("wt.layer_count", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
        make_identity_item("wt.total_thickness_in", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
        make_identity_item("wt.stack_hash_loose", None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE),
    ]


def extract_wall_types(doc, ctx=None):
    info = {
        "count": 0,
        "raw_count": 0,
        "hash_v2": None,
        "records": [],
        "record_rows": [],
        "signature_hashes_v2": [],
        "status": "ok",
        "debug_blocked_kind": 0,
        "debug_blocked_no_cs": 0,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    if ctx is None:
        ctx = {}

    if not _require_compound_dependencies(ctx, info):
        return info

    if WallType is None:
        info["status"] = "blocked"
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {"api_unreachable": 1}
        return info

    try:
        wall_types = list(
            collect_types(
                doc,
                of_class=WallType,
                cctx=(ctx or {}).get("_collect"),
                cache_key="compound_types:wall_types:WallType:types",
            )
        )
    except Exception:
        wall_types = []

    info["raw_count"] = len(wall_types)
    _total_type_count = len(wall_types)
    _instance_count_map, _instance_count_map_q = _build_instance_count_map(
        doc, ctx, getattr(BuiltInCategory, "OST_Walls", None), "compound_types.wall.instances"
    )

    fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_sig_hash_v2", None)
    if not isinstance(fp_uid_to_sig_hash, dict):
        fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_hash", {}) or {}

    records = []
    sigs = []

    for wt in wall_types:
        type_name = _read_type_name(wt)
        kind_int, kind_str = _read_wall_kind(wt)
        is_basic = (kind_int == _WALL_KIND_BASIC)

        if not is_basic:
            blocked_items = [
                make_identity_item("wt.type_name", type_name, ITEM_Q_OK),
                make_identity_item("wt.kind", *_canon_non_sentinel_str(kind_str)),
            ] + _blocked_required_items(wt_function_v=None, wt_function_q=ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
            rec = build_record_v2(
                domain=_DOMAIN_WALL,
                record_id="wall_type|{}".format(type_name),
                status=STATUS_BLOCKED,
                status_reasons=["kind_not_compound"],
                sig_hash=None,
                identity_items=sorted(blocked_items, key=lambda it: safe_str(it.get("k", ""))),
                required_qs=[ITEM_Q_OK],
                label=_label_for_type(type_name),
            )
            rec["join_key_name_identity"] = _build_name_key(ctx, "wall_types", blocked_items)
            _ip, _ip_q = purge_lookup(getattr(getattr(wt, "Id", None), "IntegerValue", None), ctx)
            rec["is_purgeable"] = _ip
            rec["is_purgeable_q"] = _ip_q
            _attach_placeholder_metadata(rec, wt, _instance_count_map, _instance_count_map_q, _total_type_count)
            rec["layer_rows"] = []
            records.append(rec)
            info["debug_blocked_kind"] += 1
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"]["kind_not_compound"] = (
                info["debug_v2_block_reasons"].get("kind_not_compound", 0) + 1
            )
            continue

        try:
            cs = wt.GetCompoundStructure()
        except Exception:
            cs = None

        if cs is None:
            try:
                wt_function_v, wt_function_q = canonicalize_str(getattr(wt, "Function", None))
            except Exception:
                wt_function_v, wt_function_q = (None, ITEM_Q_UNREADABLE)
            blocked_items = [make_identity_item("wt.type_name", type_name, ITEM_Q_OK)]
            blocked_items.extend(_blocked_required_items(wt_function_v=wt_function_v, wt_function_q=wt_function_q))
            rec = build_record_v2(
                domain=_DOMAIN_WALL,
                record_id="wall_type|{}".format(type_name),
                status=STATUS_BLOCKED,
                status_reasons=["no_compound_structure"],
                sig_hash=None,
                identity_items=sorted(blocked_items, key=lambda it: safe_str(it.get("k", ""))),
                required_qs=[ITEM_Q_OK],
                label=_label_for_type(type_name),
            )
            rec["join_key_name_identity"] = _build_name_key(ctx, "wall_types", blocked_items)
            _ip, _ip_q = purge_lookup(getattr(getattr(wt, "Id", None), "IntegerValue", None), ctx)
            rec["is_purgeable"] = _ip
            rec["is_purgeable_q"] = _ip_q
            _attach_placeholder_metadata(rec, wt, _instance_count_map, _instance_count_map_q, _total_type_count)
            rec["layer_rows"] = []
            records.append(rec)
            info["debug_blocked_no_cs"] += 1
            info["debug_v2_blocked"] = True
            info["debug_v2_block_reasons"]["no_compound_structure"] = (
                info["debug_v2_block_reasons"].get("no_compound_structure", 0) + 1
            )
            continue

        cs_data = _read_compound_structure(cs, doc, ctx, "wall")

        # type-level reads
        try:
            raw = getattr(wt, "Function", None)
            wt_function = _enum_name(WallFunction, int(raw), _WALL_FUNCTION_NAMES)
            wt_function_q = ITEM_Q_OK
        except Exception:
            wt_function = S_UNREADABLE
            wt_function_q = ITEM_Q_UNREADABLE

        cfpsh_v, cfpsh_q, cfc_v, cfc_q = _coarse_fill_reads(wt, doc, fp_uid_to_sig_hash, ctx)

        # has embedded sweeps
        sweeps_v = None
        sweeps_q = ITEM_Q_UNREADABLE
        try:
            sweeps = cs.GetWallSweepsInfo()
            sweeps_v, sweeps_q = canonicalize_bool(len(list(sweeps or [])) > 0)
        except Exception:
            sweeps_v, sweeps_q = (None, ITEM_Q_UNREADABLE)

        # semantic
        if cs_data.get("has_unreadable_thickness", False):
            total_thickness_v, total_thickness_q = (None, ITEM_Q_UNREADABLE)
        else:
            total_thickness_v, total_thickness_q = canonicalize_float(cs_data["total_thickness_in"], nd=4)
        semantic = [
            make_identity_item("wt.function", wt_function if wt_function != S_UNREADABLE else None, wt_function_q),
            make_identity_item("wt.wraps_at_inserts", *_canon_non_sentinel_str(cs_data["wraps_at_inserts"])),
            make_identity_item("wt.wraps_at_ends", *_canon_non_sentinel_str(cs_data["wraps_at_ends"])),
            make_identity_item("wt.layer_count", *canonicalize_int(cs_data["layer_count"])),
            make_identity_item("wt.total_thickness_in", total_thickness_v, total_thickness_q),
            make_identity_item("wt.stack_hash_loose", *canonicalize_str(cs_data["stack_hash_loose"])),
        ]
        coordination = [
            make_identity_item("wt.kind", *_canon_non_sentinel_str(kind_str)),
            make_identity_item("wt.total_layer_rows", *canonicalize_int(cs_data["total_layer_rows"])),
            make_identity_item("wt.stack_hash_strict", *canonicalize_str(cs_data["stack_hash_strict"])),
            make_identity_item("wt.stack_hash_function_only", *canonicalize_str(cs_data["stack_hash_function_only"])),
            make_identity_item("wt.coarse_fill_pattern_sig_hash", cfpsh_v, cfpsh_q),
            make_identity_item("wt.has_embedded_sweeps", sweeps_v, sweeps_q),
        ]
        cosmetic = [
            make_identity_item("wt.type_name", *canonicalize_str(type_name)),
            make_identity_item("wt.coarse_fill_color_rgb", cfc_v, cfc_q),
        ]

        identity_items = sorted((semantic + coordination + cosmetic), key=lambda it: safe_str(it.get("k", "")))
        required_keys = {
            "wt.layer_count",
            "wt.total_thickness_in",
            "wt.stack_hash_loose",
        }
        required_qs = [it.get("q") for it in semantic if safe_str(it.get("k", "")) in required_keys]
        required_not_ok = any(q != ITEM_Q_OK for q in required_qs)
        status = STATUS_BLOCKED if required_not_ok else STATUS_OK
        status_reasons = ["required_identity_not_ok"] if required_not_ok else []
        sig_hash = None if required_not_ok else make_hash(serialize_identity_items(semantic))

        rec = build_record_v2(
            domain=_DOMAIN_WALL,
            record_id="wall_type|{}".format(type_name),
            status=status,
            status_reasons=status_reasons,
            sig_hash=sig_hash,
            identity_items=identity_items,
            required_qs=required_qs,
            label=_label_for_type(type_name),
        )
        rec["join_key_name_identity"] = _build_name_key(ctx, "wall_types", identity_items)
        _ip, _ip_q = purge_lookup(getattr(getattr(wt, "Id", None), "IntegerValue", None), ctx)
        rec["is_purgeable"] = _ip
        rec["is_purgeable_q"] = _ip_q
        _attach_placeholder_metadata(rec, wt, _instance_count_map, _instance_count_map_q, _total_type_count)
        rec["sig_basis"] = {
            "schema": "wall_types.sig_basis.v1",
            "keys_used": [
                "wt.function",
                "wt.wraps_at_inserts",
                "wt.wraps_at_ends",
                "wt.layer_count",
                "wt.total_thickness_in",
                "wt.stack_hash_loose",
            ],
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

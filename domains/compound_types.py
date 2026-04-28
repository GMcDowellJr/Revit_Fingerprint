# -*- coding: utf-8 -*-
"""Compound Types domain family extractor.

Partitioned extractors:
- extract_wall_types (implemented)
- extract_floor_types (stub)
- extract_roof_types (stub)
- extract_ceiling_types (stub)
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.canon import S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
from core.record_v2 import (
    STATUS_OK,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
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

try:
    from Autodesk.Revit.DB import (
        WallType,
        WallKind,
        WallFunction,
        CompoundStructure,
        CompoundStructureLayer,
        MaterialFunctionAssignment,
        BuiltInParameter,
        ShellLayerType,
        Element,
    )
except ImportError:
    WallType = None
    WallKind = None
    WallFunction = None
    CompoundStructure = None
    CompoundStructureLayer = None
    MaterialFunctionAssignment = None
    BuiltInParameter = None
    ShellLayerType = None
    Element = None

_DOMAIN_WALL = "wall_types"
_WALL_KIND_BASIC = 0
_WALL_KIND_STACKED = 1
_WALL_KIND_CURTAIN = 2
_WALL_KIND_NAMES = {
    _WALL_KIND_BASIC: "Basic",
    _WALL_KIND_STACKED: "Stacked",
    _WALL_KIND_CURTAIN: "Curtain",
}
_CORE_BOUNDARY_SENTINEL = "CORE_BOUNDARY"
_LAYER_RECORD_ID_PREFIX = "wall_type_layer"


def _na_or(value, family, allowed_family):
    if family != allowed_family:
        return S_NOT_APPLICABLE
    return value


def _canon_non_sentinel_str(v):
    try:
        if v in (S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE):
            return None, ITEM_Q_UNREADABLE
    except Exception:
        pass
    return canonicalize_str(v)


def _material_identity_from_layer(layer, doc, ctx):
    material_uid = None
    try:
        mid = getattr(layer, "MaterialId", None)
    except Exception:
        mid = None

    try:
        if mid is not None and getattr(mid, "IntegerValue", -1) >= 0:
            me = doc.GetElement(mid)
            material_uid = getattr(me, "UniqueId", None) if me is not None else None
    except Exception:
        material_uid = None

    uid_to_name = (ctx or {}).get(CTX_MATERIAL_UID_TO_NAME, {}) or {}
    uid_to_class = (ctx or {}).get(CTX_MATERIAL_UID_TO_CLASS, {}) or {}
    if not material_uid:
        return S_MISSING, S_MISSING
    return uid_to_name.get(material_uid, S_MISSING), uid_to_class.get(material_uid, S_MISSING)


def _layer_function_str(layer):
    try:
        return safe_str(getattr(layer, "Function", None))
    except Exception:
        return S_UNREADABLE

def _stack_hash_field(v):
    if v is None:
        return ""
    return safe_str(v)


def _read_compound_structure(cs, doc, ctx, family):
    rows = []
    loose_parts = []
    strict_parts = []
    fn_only_parts = []
    total_thickness_in = 0.0
    layer_count = 0

    exterior_boundary_idx = None
    interior_boundary_idx = None
    if ShellLayerType is not None:
        try:
            exterior_boundary_idx = cs.GetCoreBoundaryLayerIndex(ShellLayerType.Exterior)
        except Exception:
            exterior_boundary_idx = None
        try:
            interior_boundary_idx = cs.GetCoreBoundaryLayerIndex(ShellLayerType.Interior)
        except Exception:
            interior_boundary_idx = None

    try:
        layers = list(cs.GetLayers() or [])
    except Exception:
        layers = []

    boundary_indices = []
    for bidx in (exterior_boundary_idx, interior_boundary_idx):
        if isinstance(bidx, int) and bidx >= 0 and bidx < len(layers) and bidx not in boundary_indices:
            boundary_indices.append(bidx)

    layer_row_index = 0

    for i, layer in enumerate(layers):
        if i in boundary_indices:
            boundary_row = {
                "layer_index": layer_row_index,
                "is_core_boundary": True,
                "wl.function": _CORE_BOUNDARY_SENTINEL,
                "wl.thickness_in": None,
                "wl.material_name": None,
                "wl.material_class": None,
                "wl.participates_in_wrapping": None,
                "wl.structural_material": None,
                "wl.is_variable": None,
                "wl.is_structural_deck": None,
                "wl.deck_usage": None,
                "wl.deck_profile_name": None,
            }
            rows.append(boundary_row)
            layer_row_index += 1
            loose_parts.append("{}||".format(_CORE_BOUNDARY_SENTINEL))
            strict_parts.append("{}|||".format(_CORE_BOUNDARY_SENTINEL))
            fn_only_parts.append(_CORE_BOUNDARY_SENTINEL)

        row = {
            "layer_index": layer_row_index,
            "is_core_boundary": False,
            "wl.function": None,
            "wl.thickness_in": None,
            "wl.material_name": None,
            "wl.material_class": None,
            "wl.participates_in_wrapping": None,
            "wl.structural_material": None,
            "wl.is_variable": None,
            "wl.is_structural_deck": None,
            "wl.deck_usage": None,
            "wl.deck_profile_name": None,
        }

        fn_str = _layer_function_str(layer)
        width_in = None
        try:
            width_in = round(float(getattr(layer, "Width", 0.0)) * 12.0, 4)
        except Exception:
            width_in = None

        mat_name, mat_class = _material_identity_from_layer(layer, doc, ctx)

        wrap_participates = None
        try:
            wrap_participates = bool(cs.ParticipatesInWrapping(i))
        except Exception:
            wrap_participates = None

        structural_material = None
        try:
            structural_material = bool(getattr(layer, "IsStructuralMaterial", None))
        except Exception:
            structural_material = None
        structural_material = _na_or(structural_material, family, "wall")

        is_variable = None
        try:
            is_variable = bool(getattr(layer, "IsVariableWidth", None))
        except Exception:
            is_variable = None
        if family == "ceiling":
            is_variable = S_NOT_APPLICABLE

        is_structural_deck = S_NOT_APPLICABLE if family in ("wall", "roof", "ceiling") else None
        deck_usage = S_NOT_APPLICABLE if family in ("wall", "roof", "ceiling") else None
        deck_profile_name = S_NOT_APPLICABLE if family in ("wall", "roof", "ceiling") else None

        row.update(
            {
                "wl.function": fn_str,
                "wl.thickness_in": width_in,
                "wl.material_name": mat_name,
                "wl.material_class": mat_class,
                "wl.participates_in_wrapping": wrap_participates,
                "wl.structural_material": structural_material,
                "wl.is_variable": is_variable,
                "wl.is_structural_deck": is_structural_deck,
                "wl.deck_usage": deck_usage,
                "wl.deck_profile_name": deck_profile_name,
            }
        )

        loose_parts.append("{}|{}|{}".format(
            _stack_hash_field(fn_str),
            _stack_hash_field(mat_class),
            _stack_hash_field(width_in),
        ))
        strict_parts.append("{}|{}|{}|{}".format(
            _stack_hash_field(fn_str),
            _stack_hash_field(mat_class),
            _stack_hash_field(width_in),
            _stack_hash_field(mat_name),
        ))
        fn_only_parts.append(safe_str(fn_str))

        if width_in is not None:
            total_thickness_in += float(width_in)
        layer_count += 1
        rows.append(row)
        layer_row_index += 1

    wraps_at_inserts = S_NOT_APPLICABLE
    wraps_at_ends = S_NOT_APPLICABLE
    if family == "wall":
        try:
            wraps_at_inserts = safe_str(cs.WrapAtInserts)
        except Exception:
            wraps_at_inserts = S_UNREADABLE
        try:
            wraps_at_ends = safe_str(cs.WrapAtEnds)
        except Exception:
            wraps_at_ends = S_UNREADABLE

    return {
        "layer_count": int(layer_count),
        "total_layer_rows": int(len(rows)),
        "total_thickness_in": round(total_thickness_in, 4),
        "stack_hash_loose": make_hash(["\n".join(loose_parts)]),
        "stack_hash_strict": make_hash(["\n".join(strict_parts)]),
        "stack_hash_function_only": make_hash(["\n".join(fn_only_parts)]),
        "wraps_at_inserts": wraps_at_inserts,
        "wraps_at_ends": wraps_at_ends,
        "layer_rows": rows,
    }


def _read_type_name(wall_type):
    try:
        n = Element.Name.GetValue(wall_type) if Element is not None else None
        if n:
            return safe_str(n)
    except Exception:
        pass
    # Try .Name property next
    try:
        n = wall_type.Name
        if n:
            return safe_str(n)
    except Exception:
        pass
    # Fall back to type-name parameter for Dynamo/Revit cross-version reliability
    try:
        p = wall_type.get_Parameter(BuiltInParameter.ALL_MODEL_TYPE_NAME)
        if p is not None:
            n = p.AsString()
            if n:
                return safe_str(n)
    except Exception:
        pass
    return ""


def _read_wall_kind(wt):
    kind_raw = getattr(wt, "Kind", -1)
    try:
        kind_int = int(kind_raw)
        return kind_int, _WALL_KIND_NAMES.get(kind_int, safe_str(kind_raw))
    except Exception:
        pass
    try:
        kind_int = int(getattr(kind_raw, "value__", -1))
        return kind_int, _WALL_KIND_NAMES.get(kind_int, safe_str(kind_raw))
    except Exception:
        pass
    kind_str = safe_str(kind_raw)
    for ki, ks in _WALL_KIND_NAMES.items():
        if safe_str(ks).lower() == safe_str(kind_str).lower():
            return ki, ks
    return -1, kind_str if kind_str else S_UNREADABLE


def _blocked_stub_result():
    return {
        "count": 0,
        "raw_count": 0,
        "hash_v2": None,
        "records": [],
        "record_rows": [],
        "signature_hashes_v2": [],
        "status": "blocked",
        "block_reason": "not_yet_implemented",
        "debug_v2_blocked": True,
        "debug_v2_block_reasons": {"not_yet_implemented": 1},
    }

def _label_for_wall_type(type_name):
    return {
        "display": safe_str(type_name),
        "quality": "human",
        "provenance": "revit.WallType.Name",
        "components": {"type_name": safe_str(type_name)},
    }

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

    fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_sig_hash_v2", None)
    if not isinstance(fp_uid_to_sig_hash, dict):
        fp_uid_to_sig_hash = (ctx or {}).get("fill_pattern_uid_to_hash", {}) or {}

    records = []
    sigs = []
    debug_kind_printed = 0

    for wt in wall_types:
        # Temporary debug at top of for loop, first iteration only
        if debug_kind_printed == 0:
            try:
                kind_raw = getattr(wt, "Kind", "MISSING_ATTR")
                print("[DEBUG] Kind raw: {} type: {}".format(kind_raw, type(kind_raw)))
                print("[DEBUG] Kind dir: {}".format([x for x in dir(kind_raw) if not x.startswith('__')]))
                print("[DEBUG] wt.Name direct: {}".format(getattr(wt, "Name", "NO_NAME_ATTR")))
                for bip in ["SYMBOL_NAME_PARAM", "ALL_MODEL_TYPE_NAME", "DATUM_TEXT"]:
                    try:
                        p = wt.get_Parameter(getattr(BuiltInParameter, bip))
                        print("[DEBUG] BIP {}: {}".format(bip, p.AsString() if p else "None"))
                    except Exception as e:
                        print("[DEBUG] BIP {} error: {}".format(bip, e))
            except Exception as e:
                print("[DEBUG] outer error: {}".format(e))

        type_name = _read_type_name(wt)

        kind_int, kind_str = _read_wall_kind(wt)
        is_basic = (kind_int == _WALL_KIND_BASIC)

        if debug_kind_printed < 5:
            try:
                print("[compound_types.wall_types] kind[{}]: int={} str={}".format(debug_kind_printed, kind_int, kind_str))
            except Exception:
                pass
            debug_kind_printed += 1

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
                label=_label_for_wall_type(type_name),
            )
            rec["layer_rows"] = []
            records.append(rec)
            info["debug_blocked_kind"] += 1
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
                label=_label_for_wall_type(type_name),
            )
            rec["layer_rows"] = []
            records.append(rec)
            info["debug_blocked_no_cs"] += 1
            continue

        cs_data = _read_compound_structure(cs, doc, ctx, "wall")

        # type-level reads
        try:
            wt_function = safe_str(getattr(wt, "Function", None))
            wt_function_q = ITEM_Q_OK
        except Exception:
            wt_function = S_UNREADABLE
            wt_function_q = ITEM_Q_UNREADABLE

        # coarse fill pattern sig hash
        cfpsh_v = None
        cfpsh_q = ITEM_Q_MISSING
        try:
            p = wt.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND)
            pid = p.AsElementId() if p is not None else None
            if pid is None or getattr(pid, "IntegerValue", -1) < 0:
                cfpsh_v, cfpsh_q = (None, ITEM_Q_MISSING)
            else:
                pe = doc.GetElement(pid)
                puid = getattr(pe, "UniqueId", None) if pe is not None else None
                if puid and puid in fp_uid_to_sig_hash:
                    cfpsh_v, cfpsh_q = canonicalize_str(fp_uid_to_sig_hash.get(puid))
                else:
                    cfpsh_v, cfpsh_q = (None, ITEM_Q_MISSING)
        except Exception:
            cfpsh_v, cfpsh_q = (None, ITEM_Q_UNREADABLE)

        # coarse fill color
        cfc_v = None
        cfc_q = ITEM_Q_MISSING
        try:
            p = wt.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_COLOR)
            cint = p.AsInteger() if p is not None else None
            if cint is None:
                cfc_v, cfc_q = (None, ITEM_Q_MISSING)
            else:
                cint = int(cint)
                r = cint & 255
                g = (cint >> 8) & 255
                b = (cint >> 16) & 255
                cfc_v, cfc_q = canonicalize_str("{},{},{}".format(r, g, b))
        except Exception:
            cfc_v, cfc_q = (None, ITEM_Q_UNREADABLE)

        # has embedded sweeps
        sweeps_v = None
        sweeps_q = ITEM_Q_UNREADABLE
        try:
            sweeps = cs.GetWallSweepsInfo()
            sweeps_v, sweeps_q = canonicalize_bool(len(list(sweeps or [])) > 0)
        except Exception:
            sweeps_v, sweeps_q = (None, ITEM_Q_UNREADABLE)

        # semantic
        semantic = [
            make_identity_item("wt.function", wt_function if wt_function != S_UNREADABLE else None, wt_function_q),
            make_identity_item("wt.wraps_at_inserts", *_canon_non_sentinel_str(cs_data["wraps_at_inserts"])),
            make_identity_item("wt.wraps_at_ends", *_canon_non_sentinel_str(cs_data["wraps_at_ends"])),
            make_identity_item("wt.layer_count", *canonicalize_int(cs_data["layer_count"])),
            make_identity_item("wt.total_thickness_in", *canonicalize_float(cs_data["total_thickness_in"], nd=4)),
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
            "wt.function",
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
            label=_label_for_wall_type(type_name),
        )
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
    return info


def extract_floor_types(doc, ctx=None):
    return _blocked_stub_result()


def extract_roof_types(doc, ctx=None):
    return _blocked_stub_result()


def extract_ceiling_types(doc, ctx=None):
    return _blocked_stub_result()

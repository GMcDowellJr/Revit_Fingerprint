# -*- coding: utf-8 -*-
"""Compound-structure layer helpers shared by wall_types, floor_types, roof_types,
and ceiling_types.

Not a domain extractor itself -- no extract() entry point. Holds the
compound-structure-layer read logic and other cross-partition helpers used by
the four compound-type domain files.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
from core.record_v2 import (
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    canonicalize_str,
)
from domains.materials import CTX_MATERIAL_UID_TO_NAME, CTX_MATERIAL_UID_TO_CLASS
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy, compute_projection_status
from core.collect import collect_instances
from core.deps import require_domain, Blocked

try:
    from Autodesk.Revit.DB import (
        CompoundStructure,
        CompoundStructureLayer,
        MaterialFunctionAssignment,
        BuiltInParameter,
        ShellLayerType,
    )
except ImportError:
    CompoundStructure = None
    CompoundStructureLayer = None
    MaterialFunctionAssignment = None
    BuiltInParameter = None
    ShellLayerType = None

# DeckEmbeddingType is Revit 2024+. Catch AttributeError too — older runtimes
# load the module but the name is absent, raising AttributeError, not ImportError.
# All deck-property reads guard on DeckEmbeddingType is not None before use.
try:
    from Autodesk.Revit.DB import DeckEmbeddingType
except (ImportError, AttributeError):
    DeckEmbeddingType = None


_LAYER_FUNCTION_NAMES = {
    0: "None", 1: "Structure", 2: "Substrate", 3: "Insulation",
    4: "Finish1", 5: "Finish2", 6: "Membrane", 7: "StructuralDeck",
}
_WALL_WRAPPING_NAMES = {
    0: "DoNotWrap", 1: "Exterior", 2: "Interior", 3: "Both",
}
_DECK_EMBEDDING_NAMES = {
    0: "BoundLayerAbove",
    1: "StandAlone",
    2: "BoundLayerBelow",
}
_CORE_BOUNDARY_SENTINEL = "CORE_BOUNDARY"


def _enum_name(enum_class, int_val, fallback_map):
    try:
        return enum_class(int_val).name
    except Exception:
        pass
    return fallback_map.get(int_val, str(int_val))


def _build_name_key(ctx, domain_name, identity_items):
    """Canonical Name Identity Projection (PR1): second, independent join_hash variant
    keyed off this record's own label.display-backing item (wt/ft/rt/ct.type_name, already
    a native identity_items key for every compound_types partition). compound_types.py
    never calls build_join_key_from_policy for its own configuration join_hash (Step 0
    A.1) -- these are new call sites, computed from the same identity_items snapshot
    already built for sig_hash/identity_basis at each call site."""
    pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), domain_name)
    name_key, missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
        emit_items=False,
        emit_selectors=True,
    )
    name_key["status"] = compute_projection_status(pol, missing)
    return name_key


def _build_instance_count_map(doc, ctx, bic, where_key):
    _instance_count_map = {}
    try:
        instances = collect_instances(
            doc,
            of_category=bic,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            where_key=where_key,
        )
        for inst in instances:
            try:
                tid = int(getattr(getattr(inst, "GetTypeId", lambda: None)(), "IntegerValue", -1))
                if tid > 0:
                    _instance_count_map[tid] = _instance_count_map.get(tid, 0) + 1
            except Exception:
                continue
        return _instance_count_map, "ok"
    except Exception:
        return {}, "unreadable"


def _attach_placeholder_metadata(rec, type_elem, instance_count_map, instance_count_map_q, total_type_count):
    type_id_int = getattr(getattr(type_elem, "Id", None), "IntegerValue", None)
    if instance_count_map_q == "ok" and type_id_int is not None:
        try:
            rec["instance_count"] = instance_count_map.get(int(type_id_int), 0)
            rec["instance_count_q"] = "ok"
        except Exception:
            rec["instance_count"] = None
            rec["instance_count_q"] = "unreadable"
    else:
        rec["instance_count"] = None
        rec["instance_count_q"] = "unreadable"

    try:
        rec["is_sole_type_in_category"] = (total_type_count == 1)
        rec["is_sole_type_in_category_q"] = "ok"
    except Exception:
        rec["is_sole_type_in_category"] = None
        rec["is_sole_type_in_category_q"] = "unreadable"


def _na_or(value, family, allowed_family):
    if family != allowed_family:
        return S_NOT_APPLICABLE
    return value


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
    raw = getattr(layer, "Function", None)
    try:
        return _enum_name(MaterialFunctionAssignment, int(raw), _LAYER_FUNCTION_NAMES)
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
    has_unreadable_thickness = False

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
            has_unreadable_thickness = True

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

        if family in ("wall", "roof", "ceiling"):
            is_structural_deck = S_NOT_APPLICABLE
            deck_usage = S_NOT_APPLICABLE
            deck_profile_name = S_NOT_APPLICABLE
        else:
            is_structural_deck = False
            deck_usage = S_NOT_APPLICABLE
            deck_profile_name = S_NOT_APPLICABLE
            try:
                fn_int = int(str(getattr(layer, "Function", -1)))
                is_structural_deck = (fn_int == 7)
            except Exception:
                is_structural_deck = False
            if is_structural_deck:
                try:
                    raw_usage = cs.GetDeckEmbeddingType(i)
                    if DeckEmbeddingType is not None:
                        deck_usage = _enum_name(DeckEmbeddingType, int(str(raw_usage)), _DECK_EMBEDDING_NAMES)
                    else:
                        deck_usage = _DECK_EMBEDDING_NAMES.get(int(str(raw_usage)), str(raw_usage))
                except Exception:
                    deck_usage = S_UNREADABLE
                try:
                    profile_id = cs.GetDeckProfileId(i)
                    if profile_id is not None and getattr(profile_id, "IntegerValue", -1) >= 0:
                        profile_el = doc.GetElement(profile_id)
                        if profile_el is not None:
                            deck_profile_name = str(profile_el.Name) if profile_el.Name else S_MISSING
                        else:
                            deck_profile_name = S_MISSING
                    else:
                        deck_profile_name = S_MISSING
                except Exception:
                    deck_profile_name = S_UNREADABLE

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

        if family == "floor" and is_structural_deck:
            loose_parts.append("{}|{}|{}|{}".format(
                _stack_hash_field(fn_str),
                _stack_hash_field(mat_class),
                _stack_hash_field(width_in),
                _stack_hash_field(deck_usage),
            ))
        else:
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
            wraps_at_inserts = _enum_name(None, int(str(cs.WrapAtInserts)), _WALL_WRAPPING_NAMES)
        except Exception:
            wraps_at_inserts = S_UNREADABLE
        try:
            wraps_at_ends = _enum_name(None, int(str(cs.WrapAtEnds)), _WALL_WRAPPING_NAMES)
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
        "has_unreadable_thickness": bool(has_unreadable_thickness),
        "layer_rows": rows,
    }


def _read_type_name(wall_type):
    try:
        n = wall_type.Name
        if n is not None and str(n).strip():
            return str(n).strip()
    except Exception:
        pass
    try:
        p = wall_type.get_Parameter(BuiltInParameter.SYMBOL_NAME_PARAM)
        if p is not None:
            n = p.AsString()
            if n is not None and str(n).strip():
                return str(n).strip()
    except Exception:
        pass
    return ""


def _label_for_type(type_name):
    return {
        "display": safe_str(type_name),
        "quality": "human",
        "provenance": "revit.Name",
        "components": {"type_name": safe_str(type_name)},
    }


def _require_compound_dependencies(ctx, info):
    domains_map = (ctx or {}).get("_domains", None)
    if not isinstance(domains_map, dict) or not domains_map:
        return True
    try:
        require_domain(domains_map, "materials")
        require_domain(domains_map, "fill_patterns_drafting")
        require_domain(domains_map, "fill_patterns_model")
    except Blocked as b:
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = {
            "dependency_blocked": "{}".format(";".join(list(getattr(b, "reasons", []) or [])))
        }
        info["status"] = "blocked"
        return False
    return True


def _coarse_fill_reads(type_elem, doc, fp_uid_to_sig_hash, ctx=None):
    """Read coarse fill pattern sig hash and color from a compound type element.

    Prefer locale-independent BuiltInParameter access, with UI-name fallback
    only when BIP members are unavailable in the current runtime.
    """
    # fill pattern sig hash
    cfpsh_v = None
    cfpsh_q = ITEM_Q_MISSING
    fp_id_to_value = (ctx or {}).get("fill_pattern_id_to_value", {}) or {}
    fp_special_values = (ctx or {}).get("fill_pattern_special_values", {}) or {}
    if not isinstance(fp_special_values, dict):
        fp_special_values = {}
    no_pattern_symbol = fp_special_values.get("no_pattern", None)
    try:
        p = None
        try:
            p = type_elem.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND)
        except Exception:
            p = type_elem.LookupParameter("Coarse Scale Fill Pattern")

        if p is None:
            cfpsh_v, cfpsh_q = (None, ITEM_Q_MISSING)
        else:
            pid = p.AsElementId()
            if pid is None or getattr(pid, "IntegerValue", -1) < 0:
                if no_pattern_symbol:
                    cfpsh_v, cfpsh_q = canonicalize_str(no_pattern_symbol)
                else:
                    cfpsh_v, cfpsh_q = (None, ITEM_Q_MISSING)
            else:
                pid_key = safe_str(getattr(pid, "IntegerValue", ""))
                mapped_value = fp_id_to_value.get(pid_key, None)
                if mapped_value:
                    cfpsh_v, cfpsh_q = canonicalize_str(mapped_value)
                else:
                    pe = doc.GetElement(pid)
                    puid = getattr(pe, "UniqueId", None) if pe is not None else None
                    if puid and puid in fp_uid_to_sig_hash:
                        cfpsh_v, cfpsh_q = canonicalize_str(fp_uid_to_sig_hash.get(puid))
                    else:
                        cfpsh_v, cfpsh_q = (None, ITEM_Q_MISSING)
    except Exception:
        cfpsh_v, cfpsh_q = (None, ITEM_Q_UNREADABLE)

    # fill color
    cfc_v = None
    cfc_q = ITEM_Q_MISSING
    try:
        p = None
        try:
            p = type_elem.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_COLOR)
        except Exception:
            p = type_elem.LookupParameter("Coarse Scale Fill Color")

        if p is None:
            cfc_v, cfc_q = (None, ITEM_Q_MISSING)
        else:
            cint = p.AsInteger()
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

    return cfpsh_v, cfpsh_q, cfc_v, cfc_q

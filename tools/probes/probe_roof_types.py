# Dynamo Python (Revit) — Breadth Probe: roof_types (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "reflection",
#     "domain": "roof_types",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "inventory",
#     "domain": "roof_types",
#     "records": [...]      # aggregated breadth view (q_counts/example) over every RoofType, no sampling
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "roof_types",
#     "records": [...]      # one row per (RoofType, CompoundStructureLayer): resolves layer.MaterialId
#                            # against the document, same as every other domain's crosswalk. Every type and
#                            # every layer is walked -- no sampling.
#   }
# ]
#
# Reworked from "full_records" to the standard inventory/reflection/crosswalk
# contract every other domain probe uses -- see probe_ceiling_types.py for
# the full rationale. The per-layer CompoundStructureLayer.MaterialId
# reference, the one thing full_records carried that inventory/reflection
# alone couldn't reconstruct, is now a real resolved crosswalk instead of an
# inert per-instance field.
#
# RoofType has no Kind distinction (wall-only) and no wrap-at-inserts/ends
# surface (gated to family == "wall" in domains/compound_types.py's
# _read_compound_structure -- always not-applicable elsewhere). RoofType has no Function property in the extractor.
#
# Inputs:
#   IN[0] max_types_to_inspect (int) -- safety cap only. Default: 10000
#   IN[1] max_layers_per_type (int)  -- Default: 30
#   IN[2] write_json (bool)          -- Default: False
#   IN[3] output_directory (str)     -- Default: None (RVT dir, then TEMP)
#        Filename fixed as: probes_<revit_version>_<run_id>.json


import clr
import os
import json
import uuid
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    BuiltInParameter, BuiltInCategory,
    RoofType,

)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_types_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 10000
max_layers_per_type = IN[1] if len(IN) > 1 and IN[1] is not None else 30
write_json = IN[2] if len(IN) > 2 and IN[2] is not None else False
out_path = IN[3] if len(IN) > 3 and IN[3] is not None else None


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _safe_capture(fn):
    try:
        return fn(), None
    except Exception as ex:
        return None, "{}: {}".format(type(ex).__name__, ex)

def _multi_repr(raw):
    if raw is None:
        return {"raw_is_none": True}
    pytype = type(raw).__name__
    tostring_val, tostring_err = _safe_capture(lambda: raw.ToString())
    str_val, str_err = _safe_capture(lambda: str(raw))
    int_direct_val, int_direct_err = _safe_capture(lambda: int(raw))
    int_via_str_val, int_via_str_err = _safe_capture(lambda: int(str(raw)))
    return {
        "python_type": pytype,
        "ToString()": tostring_val,
        "ToString()_error": tostring_err,
        "str()": str_val,
        "str()_error": str_err,
        "int_direct": int_direct_val,
        "int_direct_error": int_direct_err,
        "int_via_str": int_via_str_val,
        "int_via_str_error": int_via_str_err,
    }

def _safe_type_name(elem):
    for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
        try:
            p = elem.get_Parameter(bip)
            if p is not None:
                s = p.AsString()
                if s:
                    return s
        except:
            pass
    try:
        return elem.Name
    except:
        return None

def _safe_param_def_name(p):
    try:
        d = p.Definition
        return d.Name if d is not None else None
    except:
        return None

def _safe_get_datatype(p):
    try:
        d = p.Definition
        if d is None:
            return None
        return d.GetDataType()
    except:
        return None

def _is_length_datatype(dt):
    if dt is None or SpecTypeId is None:
        return False
    try:
        return dt == SpecTypeId.Length
    except:
        return False

def _is_angle_datatype(dt):
    if dt is None or SpecTypeId is None:
        return False
    try:
        return dt == SpecTypeId.Angle
    except:
        return False

def _fmt_display(p, raw_double=None):
    try:
        if raw_double is not None:
            dt = _safe_get_datatype(p)
            if dt is not None:
                return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
            return str(raw_double)
        return p.AsValueString()
    except:
        return _safe(lambda: p.AsValueString(), None)

def _format_param_contract(p):
    if p is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}

    st = _safe(lambda: p.StorageType, None)
    if st is None:
        return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}

    if st == StorageType.String:
        raw = _safe(lambda: p.AsString(), None)
        return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}

    if st == StorageType.Integer:
        raw = _safe(lambda: p.AsInteger(), None)
        disp = _fmt_display(p, None)
        return {
            "q": "ok", "storage": "Integer", "raw": raw,
            "display": disp if disp is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    if st == StorageType.Double:
        raw = _safe(lambda: p.AsDouble(), None)
        disp = _fmt_display(p, raw)
        dt = _safe_get_datatype(p)
        if raw is None:
            norm = None
        elif _is_length_datatype(dt):
            norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
        elif _is_angle_datatype(dt):
            norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
        else:
            norm = raw
        return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}

    if st == StorageType.ElementId:
        eid = _safe(lambda: p.AsElementId(), None)
        if eid is None or eid == ElementId.InvalidElementId:
            return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
        raw = _safe(lambda: eid.IntegerValue, None)
        ref_name = None
        ref = _safe(lambda: doc.GetElement(eid), None)
        if ref is not None:
            ref_name = _safe(lambda: ref.Name, None)
        return {
            "q": "ok", "storage": "ElementId", "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _contract_from_value(q, storage, raw, display, norm):
    return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}

def _to_inches(x_internal):
    if x_internal is None:
        return None
    return _safe(lambda: UnitUtils.ConvertFromInternalUnits(x_internal, UnitTypeId.Inches), x_internal)


# -------------------------
# Discovery -- ALL RoofTypes, no sampling
# -------------------------

all_types = _safe(
    lambda: (FilteredElementCollector(doc).OfClass(RoofType).ToElements()),
    default=[]
)
try:
    all_types = list(all_types)
except:
    all_types = list(all_types)

try:
    max_n = int(max_types_to_inspect)
    if max_n >= 0:
        all_types = all_types[:max_n]
except:
    pass


# Bounded sample for the reflection sweep only -- inventory/crosswalk above
# stay uncapped by design (every type, every layer; see max_layers_per_type
# for the one deliberate per-type layer cap).
_reflect_type_samples = all_types[:60]
_reflect_cs_samples = []
_reflect_layer_samples = []
for _rt in _reflect_type_samples:
    _rcs = _safe(lambda: _rt.GetCompoundStructure(), None)
    if _rcs is not None and len(_reflect_cs_samples) < 60:
        _reflect_cs_samples.append(_rcs)
    if _rcs is not None:
        for _rlayer in _safe(lambda: list(_rcs.GetLayers() or []), default=[]):
            if len(_reflect_layer_samples) < 60:
                _reflect_layer_samples.append(_rlayer)


def _resolve_material(mat_id_int):
    """Resolve a Material ElementId (int) to (name, resolved_bool)."""
    if mat_id_int is None or mat_id_int < 0:
        return (None, False)
    ref = _safe(lambda: doc.GetElement(ElementId(mat_id_int)), None)
    if ref is None:
        return (None, False)
    name = _safe(lambda: ref.Name, None)
    return (name, name is not None)


def _resolve_workset(doc, ws_id_obj):
    """Resolve an Element.WorksetId value to (name, resolved_bool) via
    WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
    distinct .NET type from ElementId (both happen to expose .IntegerValue,
    which is why reflection reports this member as ElementId-storage), and
    Workset is not derived from Element, so doc.GetElement() would never
    resolve it even with the right type assumed."""
    if ws_id_obj is None:
        return (None, False)
    wt_table = _safe(lambda: doc.GetWorksetTable(), None)
    if wt_table is None:
        return (None, False)
    ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
    if ws is None:
        return (None, False)
    name = _safe(lambda: ws.Name, None)
    return (name, name is not None)


param_index = {}


def _ensure_entry(pk):
    if pk not in param_index:
        param_index[pk] = {"storage_types": set(), "q_counts": {}, "example": None}
    return param_index[pk]


def _maybe_set_example(entry, pv):
    if pv is None or not isinstance(pv, dict):
        return
    ex = entry.get("example")
    if ex is None:
        entry["example"] = dict(pv)
        return
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = dict(pv)


def _observe(pk, pv):
    entry = _ensure_entry(pk)
    if not isinstance(pv, dict):
        return
    st = pv.get("storage")
    q = pv.get("q") or "unreadable"
    if st:
        entry["storage_types"].add(st)
    entry["q_counts"][q] = entry["q_counts"].get(q, 0) + 1
    _maybe_set_example(entry, pv)


try:
    _max_l = int(max_layers_per_type)
except:
    _max_l = 30

optional_crosswalk = []
total_records = 0

for t in all_types:
    total_records += 1
    name = _safe(lambda: _safe_type_name(t), None)
    type_id = _safe(lambda: t.Id.IntegerValue, None)
    ws_id_obj = _safe(lambda: t.WorksetId, None)
    ws_name, ws_resolved = _resolve_workset(doc, ws_id_obj)
    ws_id_int = _safe(lambda: ws_id_obj.IntegerValue, None) if ws_id_obj is not None else None

    params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(t.Parameters), default=[])
    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        _observe("p.{}".format(dn), _format_param_contract(p))

    _observe("rt.type_name", _contract_from_value("ok", "String", name, name, name))

    cs = _safe(lambda: t.GetCompoundStructure(), None)
    has_cs = cs is not None
    _observe("rt.has_compound_structure", _contract_from_value("ok", "Integer", int(has_cs), str(has_cs), int(has_cs)))

    if has_cs:
        layers = _safe(lambda: list(cs.GetLayers()), [])
        _observe("rt.layer_count", _contract_from_value("ok", "Integer", len(layers), str(len(layers)), len(layers)))

        # See probe_ceiling_types.py: CompoundStructureLayer has no
        # IsStructuralMaterial/IsVariableWidth in the modern API -- both are a
        # single index on CompoundStructure (StructuralMaterialIndex /
        # VariableLayerIndex), not a per-layer flag.
        struct_mat_idx = _safe(lambda: cs.StructuralMaterialIndex, None)
        var_layer_idx = _safe(lambda: cs.VariableLayerIndex, None)

        total_thickness_ft = 0.0
        thickness_unreadable = False
        for i, layer in enumerate(layers):
            if _max_l is not None and _max_l >= 0 and i >= _max_l:
                break
            w = _safe(lambda: layer.Width, None)
            if w is None:
                thickness_unreadable = True
            else:
                total_thickness_ft += w
            fnv = _safe(lambda: layer.Function, None)
            fn_str = _multi_repr(fnv).get("str()")
            mat_id = _safe(lambda: layer.MaterialId, None)
            mat_id_int = _safe(lambda: mat_id.IntegerValue, None) if mat_id is not None else None
            mat_name, mat_resolved = _resolve_material(mat_id_int)

            optional_crosswalk.append({
                "roof_type.id": type_id,
                "roof_type.name": name,
                "roof_type.workset_id": ws_id_int,
                "roof_type.workset_name": ws_name,
                "layer.index": i,
                "layer.function": fn_str,
                "layer.width_in": _to_inches(w) if w is not None else None,
                "layer.is_structural_material": bool(struct_mat_idx is not None and struct_mat_idx >= 0 and i == struct_mat_idx),
                "layer.is_variable_width": bool(var_layer_idx is not None and var_layer_idx >= 0 and i == var_layer_idx),
                "layer.material_id": mat_id_int,
                "layer.material_name": mat_name,
                "layer.material_resolved": mat_resolved,
            })

        tt_in = None if thickness_unreadable else _to_inches(total_thickness_ft)
        _observe("rt.total_thickness_in", _contract_from_value(
            "unreadable" if thickness_unreadable else "ok", "Double", tt_in, tt_in, tt_in))

        sweeps = _safe(lambda: list(cs.GetWallSweepsInfo()), [])
        sweeps_present = (len(sweeps) > 0)
        _observe("rt.has_embedded_sweeps", _contract_from_value("ok", "Integer", int(sweeps_present), str(sweeps_present), int(sweeps_present)))
    else:
        _observe("rt.layer_count", _contract_from_value("unsupported.not_applicable", "Integer", None, None, None))
        _observe("rt.total_thickness_in", _contract_from_value("unsupported.not_applicable", "Double", None, None, None))
        _observe("rt.has_embedded_sweeps", _contract_from_value("unsupported.not_applicable", "Integer", None, None, None))

    fill_p = _safe(lambda: t.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND), None)
    _observe("rt.coarse_fill_pattern", _format_param_contract(fill_p) if fill_p is not None else _contract_from_value("missing", "None", None, None, None))
    color_p = _safe(lambda: t.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_COLOR), None)
    _observe("rt.coarse_fill_color", _format_param_contract(color_p) if color_p is not None else _contract_from_value("missing", "None", None, None, None))

param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "roof_types",
        "param_key": pk,
        "total_records": total_records,
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
        }
    })


# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection
# -------------------------
# Complements the curated/dynamic capture above with a breadth-only sweep of
# a bounded sample of the discovered types' .NET properties and zero-arg
# methods (inventory/crosswalk above stay uncapped by design -- this sample
# cap is just to keep the .NET reflection walk bounded on large projects).
# This is diagnostics/breadth, not identity.

_REFLECTION_SKIP = set([
    "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
    "Dispose", "GetEnumerator", "Clone",
])

def _reflect_member_names(obj):
    out = []
    if obj is None:
        return out
    try:
        t = obj.GetType()
    except:
        return out
    try:
        for p in t.GetProperties():
            try:
                n = p.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if p.GetIndexParameters():
                    continue
                out.append(("property", n))
            except:
                pass
    except:
        pass
    try:
        for me in t.GetMethods():
            try:
                n = me.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
                    continue
                if me.GetParameters().Length != 0:
                    continue
                if me.IsSpecialName:
                    continue
                out.append(("method", n))
            except:
                pass
    except:
        pass
    seen = set()
    uniq = []
    for kind, n in out:
        if n in seen:
            continue
        seen.add(n)
        uniq.append((kind, n))
    return sorted(uniq, key=lambda x: x[1])

# Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
# docs/method_invocation_candidates_annotated.csv): these 33 method names (34
# (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
# zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
# GetValidTypes, removed post-merge -- see the note below the dict) are
# ground-truth confirmed, against the live
# RevitAPI 2025 documentation (not name/return-type inference), to be
# zero-arg, instance, non-mutating getters. Declared here as data, separate
# from the branching logic in _reflect_try_get below, so it can be reviewed
# as one block and extended later without touching control flow.
#
# Keyed by method NAME only, not (declaring_class, name): this reflection
# sweep is invoked per concrete probed type_label (e.g. "WallType",
# "ProjectInformation", "FamilySymbol"), which is almost never the literal
# Revit API class that actually declares the member -- e.g. Element.GetTypeId
# is reached in this codebase via more than a dozen different concrete
# type_labels across the probe domains, never via type_label=="Element"
# itself, since no probe in this file reflects a bare Element instance.
# Scoping the allowlist by declaring-class name would silently fail to match
# nearly every real call site and defeat the point of this allowlist.
# _reflect_member_names() below already restricts candidate methods to
# public, non-special-name, zero-parameter methods before this allowlist is
# ever consulted, so a name-only match here does not weaken the zero-arg/
# no-side-effect intent the allowlist exists to enforce -- it only widens
# which already-zero-arg, already-name-matched members get invoked. The
# dict value (declaring class) is kept for traceability back to the Step 0
# CSV only; it is not used in the match.
_ALLOWLISTED_REFLECTION_METHODS = {
    "GetTypeId": "Element",
    "GetLayers": "CompoundStructure",
    "GetEntitySchemaGuids": "Element",
    "GetSubelements": "Element",
    "GetFamilyPointLocations": "FamilySymbol",
    "GetModelToProjectionTransforms": "View",
    "GetRenderingAsset": "AppearanceAssetElement",
    "GetExternalFileReference": "Element",
    "GetMonitoredLinkElementIds": "Element",
    "GetMonitoredLocalElementIds": "Element",
    "GetSimilarTypes": "ElementType",
    "GetStructuralSection": "FamilySymbol",
    "GetThermalProperties": "FamilySymbol",
    "GetFillPattern": "FillPatternElement",
    "GetLinePattern": "LinePatternElement",
    "GetCategories": "ParameterFilterElement",
    "GetElementFilter": "ParameterFilterElement",
    "GetReference": "Subelement",
    "GetBackground": "View",
    "GetCalloutParentId": "View",
    "GetCropRegionShapeManager": "View",
    "GetDepthCueing": "View",
    "GetDirectContext3DHandleOverrides": "View",
    "GetFilters": "View",
    "GetOrderedFilters": "View",
    "GetPointCloudOverrides": "View",
    "GetPrimaryViewId": "View",
    "GetReferenceCallouts": "View",
    "GetReferenceElevations": "View",
    "GetReferenceSections": "View",
    "GetSketchyLines": "View",
    "GetTemporaryViewPropertiesId": "View",
    "GetViewDisplayModel": "View",
}

# Element.GetValidTypes / Subelement.GetValidTypes were removed from the
# allowlist above after a live re-run (PR #395 discussion) showed
# Element.GetValidTypes fails 100% of the time -- not with a documented
# Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
# GetModelToProjectionTransforms above, which each match a real
# InvalidOperationException precondition stated on their own RevitAPI doc
# pages), but with a CLR/pythonnet interop binding failure:
# `TypeError: No method matches given arguments for GetValidTypes: (<class
# '...'>)`, confirmed via a standalone diagnostic against live ElementType/
# WallType/View objects. .NET reflection sees exactly one GetValidTypes
# overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
# overload-ambiguity problem either -- the call is rejected by the binder
# before it ever reaches Revit's implementation. This will never succeed
# through `getattr(obj, name)()` regardless of model/version, so keeping it
# allowlisted only adds permanent error noise with zero chance of a real
# value. Subelement.GetValidTypes was never independently tested (no probe
# in this codebase reflects a raw Subelement object as its own type_label),
# but shares the same allowlist name and the same removal; re-evaluate
# independently against a live Subelement instance before re-adding either.

def _reflect_try_get(obj, member_kind, name):
    if member_kind == "method":
        if name not in _ALLOWLISTED_REFLECTION_METHODS:
            # SAFETY: never invoke a reflection-discovered method that is not
            # on the allowlist above. Revit API methods can have side effects
            # (printing, export, regenerate, delete, transaction commits,
            # ...) and there is no reliable way to tell a safe zero-arg
            # query method from a side-effecting one by name alone for
            # anything outside the allowlist's ground-truth-verified set.
            # Record that the method exists without calling it.
            return (True, "<method not invoked>", None)
        try:
            v = getattr(obj, name)()
        except Exception as ex:
            return (False, None, "{}: {}".format(type(ex).__name__, ex))
        return (True, v, None)
    try:
        v = getattr(obj, name)
    except Exception as ex:
        return (False, None, "{}: {}".format(type(ex).__name__, ex))
    return (True, v, None)

def _reflect_contract(raw_v):
    if raw_v is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
    if isinstance(raw_v, bool):
        return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
    if isinstance(raw_v, int):
        return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, float):
        return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, str):
        return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
    try:
        if hasattr(raw_v, "IntegerValue"):
            iv = int(raw_v.IntegerValue)
            return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
    except:
        pass
    try:
        if hasattr(raw_v, "ToString"):
            s = raw_v.ToString()
            if s and "Autodesk.Revit" not in s and "System." not in s:
                return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
    except:
        pass
    try:
        ids = []
        saw_item = False
        for item in raw_v:
            saw_item = True
            if not hasattr(item, "IntegerValue"):
                raise TypeError("non-ElementId item in collection")
            ids.append(int(item.IntegerValue))
        if not saw_item:
            # An empty collection is vacuously "every item has .IntegerValue"
            # -- there's nothing to fail the check against, so item-by-item
            # duck-typing alone can never tell an empty ElementId collection
            # (GetMonitoredLinkElementIds returning [] because a type has no
            # monitored links) apart from an empty collection of anything
            # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
            # IList<Subelement>, both returning [] because that instance
            # happens to have zero). A CLR generic-type reflection check
            # (raw_v.GetType().GetGenericArguments()) was tried here and
            # found not to reliably discriminate types against a live
            # Revit/pythonnet session (still produced the same false
            # positives), so it was dropped rather than kept as an
            # unreliable safety net. Per this project's fail-soft principle
            # (never silently collapse distinct states), an empty collection
            # of unconfirmed item type gets its own explicit q value instead
            # of defaulting to "ok" (would reintroduce this exact bug) or
            # bare "unsupported" (would make it indistinguishable from a
            # totally opaque complex-object failure). storage stays "None"
            # (not "ElementIdList") so find_crosswalk_candidates.py's
            # _is_elementid_typed() correctly does not treat this as a
            # reference candidate.
            return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
        disp = ",".join(str(i) for i in ids)
        return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
    except:
        pass
    return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}

def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
    idx = {}
    for obj in sample_objs:
        if obj is None:
            continue
        for member_kind, name in _reflect_member_names(obj)[:max_members]:
            ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
            key = "refl.{}.{}".format(type_label, name)
            if key not in idx:
                idx[key] = {
                    "domain": domain_name, "member_key": key, "member_kind": member_kind,
                    "type_label": type_label, "example": None,
                    "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
                }
            e = idx[key]
            if not ok:
                e["error_count"] += 1
                continue
            contract = _reflect_contract(raw_v)
            e["ok_count"] += 1
            sig = (str(contract.get("storage")), str(contract.get("norm")))
            if sig not in e["_seen"]:
                e["_seen"].add(sig)
                e["unique_value_count"] += 1
            if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
                e["example"] = contract
    records = []
    for key in sorted(idx.keys()):
        e = idx[key]
        records.append({
            "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
            "type_label": e["type_label"], "example": e["example"],
            "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
        })
    return records

_reflection_records_type = _run_reflection_sweep(_reflect_type_samples, "RoofType", "roof_types")
_reflection_records_cs = _run_reflection_sweep(_reflect_cs_samples, "CompoundStructure", "roof_types")
_reflection_records_layer = _run_reflection_sweep(_reflect_layer_samples, "CompoundStructureLayer", "roof_types")
_reflection_records = _reflection_records_type + _reflection_records_cs + _reflection_records_layer

# -------------------------
# Unified run metadata (release-separated, not date-filename-separated)
# -------------------------

def _probe_revit_version():
    try:
        _uiapp = DocumentManager.Instance.CurrentUIApplication
        _app = _uiapp.Application if _uiapp is not None else None
        v = _safe(lambda: _app.VersionNumber, None)
        return str(v) if v else None
    except:
        return None

def _probe_document_identity():
    return {
        "title": _safe(lambda: doc.Title, None),
        "path_name": _safe(lambda: doc.PathName, None),
        "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
    }

def _probe_run_id():
    try:
        return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    except:
        return uuid.uuid4().hex[:12]

_PROBE_RUN_ID = _probe_run_id()
_PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"

def _probe_wrap(domain, out_payload):
    return {
        "run_metadata": {
            "run_id": _PROBE_RUN_ID,
            "extraction_date": datetime.now().isoformat(),
            "revit_version": _PROBE_REVIT_VERSION,
            "tool_version": None,
            "document": _probe_document_identity(),
            "source": "single_probe",
            "probe": domain,
        },
        "domains": {domain: out_payload},
    }

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "roof_types",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "roof_types",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "roof_types",
        "records": optional_crosswalk
    }
]

file_written = None
write_error = None

if write_json:
    try:
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None
        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            try:
                default_dir = os.path.dirname(rvt_path)
            except:
                default_dir = None
        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("roof_types", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path
    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

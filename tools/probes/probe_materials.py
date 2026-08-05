# -*- coding: utf-8 -*-
# Dynamo Python (Revit) -- Breadth Probe: materials (INVENTORY OUTPUT)
#
# domains/materials.py v1 captures identity + displayed-graphics state only
# and explicitly marks appearance/physical/thermal asset capture as
# "deferred" (see appearance_asset_capture_status / physical_asset_capture_status
# / thermal_asset_capture_status in its record payload). This probe targets
# exactly that gap: alongside a normal Parameter/reflection breadth sweep of
# Material itself, it resolves and reflection-sweeps the associated
# AppearanceAssetElement, StructuralAsset, and ThermalAsset objects so we
# can see what's actually available before deciding whether/how to promote
# any of it into the real extractor.
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "materials",
#     "records": param_inventory,
#     "diagnostics": {...},
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "materials",
#     "records": [...]   # material -> appearance/structural/thermal asset resolution
#   },
#   {
#     "kind": "reflection",
#     "domain": "materials",
#     "records": [...]   # Material + AppearanceAssetElement + StructuralAsset + ThermalAsset
#   }
# ]
#
# Inputs:
#   IN[0] max_materials_to_inspect (int)   Default: 500
#   IN[1] per_class_limit (int)            Default: 10   (bucket = MaterialClass)
#   IN[2] write_json (bool)                Default: False
#   IN[3] output_directory (str)           Default: None

import clr
import os
import json
import uuid
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId, Material,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils, BuiltInParameter,
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

try:
    from Autodesk.Revit.DB import AppearanceAssetElement
except:
    AppearanceAssetElement = None

try:
    from Autodesk.Revit.DB import PropertySetElement
except:
    PropertySetElement = None

doc = DocumentManager.Instance.CurrentDBDocument

max_materials_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
per_class_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 10
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

def _safe_name(obj):
    return _safe(lambda: obj.Name, None) if obj is not None else None

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

    is_none_storage = False
    try:
        is_none_storage = (int(st) == 0)
    except:
        try:
            is_none_storage = (str(st) in ("None", "None_", "0"))
        except:
            is_none_storage = False

    if is_none_storage:
        disp = _safe(lambda: p.AsValueString(), None)
        if disp is not None and str(disp).strip() != "":
            return {"q": "ok", "storage": "None", "raw": None, "display": disp, "norm": disp}
        return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}

    if st == StorageType.String:
        raw = _safe(lambda: p.AsString(), None)
        return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}

    if st == StorageType.Integer:
        raw = _safe(lambda: p.AsInteger(), None)
        disp = _fmt_display(p, None)
        return {
            "q": "ok", "storage": "Integer", "raw": raw,
            "display": disp if disp is not None else (str(raw) if raw is not None else None),
            "norm": raw,
        }

    if st == StorageType.Double:
        raw = _safe(lambda: p.AsDouble(), None)
        disp = _fmt_display(p, raw)
        dt = _safe_get_datatype(p)
        if raw is None:
            norm = None
        elif _is_length_datatype(dt):
            norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
        else:
            norm = raw
        return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}

    if st == StorageType.ElementId:
        eid = _safe(lambda: p.AsElementId(), None)
        if eid is None or eid == ElementId.InvalidElementId:
            return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
        raw = _safe(lambda: eid.IntegerValue, None)
        ref = _safe(lambda: doc.GetElement(eid), None)
        ref_name = _safe(lambda: ref.Name, None) if ref is not None else None
        display = ref_name if ref_name is not None else (str(raw) if raw is not None else None)
        return {"q": "ok", "storage": "ElementId", "raw": raw, "display": display, "norm": raw}

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

# -------------------------
# Discovery + breadth-biased sampling (bucket = MaterialClass)
# -------------------------

all_materials = _safe(
    lambda: list(FilteredElementCollector(doc).OfClass(Material).ToElements()),
    default=[],
)

try:
    max_n = int(max_materials_to_inspect)
    if max_n >= 0:
        all_materials = all_materials[:max_n]
except:
    pass

selected = []
by_class = {}
for m in all_materials:
    cls = _safe(lambda: m.MaterialClass, None) or "<unknown>"
    c = by_class.get(cls, 0)
    try:
        ok = c < int(per_class_limit)
    except:
        ok = c < 10
    if ok:
        selected.append(m)
        by_class[cls] = c + 1

if len(selected) == 0 and len(all_materials) > 0:
    selected = all_materials[:min(50, len(all_materials))]

# -------------------------
# Dynamic Parameter walk (breadth over whatever is actually bound)
# -------------------------

param_index = {}

def _maybe_set_example(entry, pv):
    ex = entry.get("example")
    if ex is None:
        entry["example"] = {k: pv.get(k) for k in ("q", "storage", "raw", "display", "norm")}
        return
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {k: pv.get(k) for k in ("q", "storage", "raw", "display", "norm")}

for m in selected:
    cls = _safe(lambda: m.MaterialClass, None) or "<unknown>"

    params = _safe(lambda: list(m.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(m.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)
        pv = _format_param_contract(p)

        if pk not in param_index:
            param_index[pk] = {
                "storage_types": set(),
                "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
                "example": None,
                "observed_on_classes": set(),
                "_seen_obs": set(),
                "unique_value_count": 0,
            }

        entry = param_index[pk]
        q = pv.get("q") or "unreadable"
        st = pv.get("storage")
        norm = pv.get("norm")

        obs_sig = (pk, str(st), str(norm))
        if obs_sig in entry["_seen_obs"]:
            entry["observed_on_classes"].add(cls)
            _maybe_set_example(entry, pv)
            continue

        entry["_seen_obs"].add(obs_sig)
        entry["unique_value_count"] += 1
        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1
        entry["observed_on_classes"].add(cls)
        _maybe_set_example(entry, pv)

param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "materials",
        "param_key": pk,
        "selected_material_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "unique_value_count": e.get("unique_value_count", 0),
            "observed_on_classes": sorted(list(e["observed_on_classes"]))[:25],
        },
    })

# -------------------------
# Crosswalk: material -> resolved appearance/structural/thermal asset
# -------------------------

optional_crosswalk = []


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


for m in selected:
    ws_id_obj = _safe(lambda: m.WorksetId, None)
    ws_name, ws_resolved = _resolve_workset(doc, ws_id_obj)
    ws_id_int = _safe(lambda: ws_id_obj.IntegerValue, None) if ws_id_obj is not None else None

    row = {
        "material.id": _safe(lambda: m.Id.IntegerValue, None),
        "material.name": _safe_name(m),
        "material.class": _safe(lambda: m.MaterialClass, None),
        "material.workset_id": ws_id_int,
        "material.workset_name": ws_name,
        "appearance_asset.id": None,
        "appearance_asset.name": None,
        "appearance_asset.resolved": False,
        "structural_asset.id": None,
        "structural_asset.name": None,
        "structural_asset.resolved": False,
        "thermal_asset.id": None,
        "thermal_asset.name": None,
        "thermal_asset.resolved": False,
    }

    aid = _safe(lambda: m.AppearanceAssetId, None)
    if aid is not None and _safe(lambda: aid.IntegerValue, -1) >= 0:
        row["appearance_asset.id"] = _safe(lambda: aid.IntegerValue, None)
        ae = _safe(lambda: doc.GetElement(aid), None)
        row["appearance_asset.name"] = _safe_name(ae)
        row["appearance_asset.resolved"] = row["appearance_asset.name"] is not None

    sid = _safe(lambda: m.StructuralAssetId, None)
    if sid is not None and _safe(lambda: sid.IntegerValue, -1) >= 0:
        row["structural_asset.id"] = _safe(lambda: sid.IntegerValue, None)
        se = _safe(lambda: doc.GetElement(sid), None)
        row["structural_asset.name"] = _safe_name(se)
        row["structural_asset.resolved"] = row["structural_asset.name"] is not None

    tid = _safe(lambda: m.ThermalAssetId, None)
    if tid is not None and _safe(lambda: tid.IntegerValue, -1) >= 0:
        row["thermal_asset.id"] = _safe(lambda: tid.IntegerValue, None)
        te = _safe(lambda: doc.GetElement(tid), None)
        row["thermal_asset.name"] = _safe_name(te)
        row["thermal_asset.resolved"] = row["thermal_asset.name"] is not None

    optional_crosswalk.append(row)

# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection
# -------------------------
# Complements the curated/dynamic capture above with a breadth-only sweep of
# the sampled objects' .NET properties and zero-arg methods. This is
# diagnostics/breadth, not identity -- it surfaces members a fixed/curated
# key list or a Parameters-only walk could otherwise miss. Deliberately
# includes the resolved Appearance/Structural/Thermal asset objects since
# domains/materials.py explicitly defers capturing those.

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

_METHOD_NOT_INVOKED_SENTINEL = "<method not invoked>"

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
            return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
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
    if raw_v == _METHOD_NOT_INVOKED_SENTINEL:
        # A non-allowlisted method's placeholder is a real Python str, so
        # without this check it falls into the generic str branch below and
        # comes out as q="ok" -- indistinguishable from genuinely invoked
        # string data. Checked before isinstance(raw_v, str) specifically so
        # it never reaches that branch.
        return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
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
        if hasattr(raw_v, "Red") and hasattr(raw_v, "Green") and hasattr(raw_v, "Blue"):
            s = "{}|{}|{}".format(int(raw_v.Red), int(raw_v.Green), int(raw_v.Blue))
            return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
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
                    "type_label": type_label, "example": None, "example_error": None,
                    "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
                }
            e = idx[key]
            if not ok:
                e["error_count"] += 1
                if e["example_error"] is None and err:
                    e["example_error"] = err
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
            "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
            "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
        })
    return records

_reflect_material_samples = list(selected)

_reflect_appearance_samples = []
_reflect_structural_asset_samples = []
_reflect_thermal_asset_samples = []

for m in selected:
    aid = _safe(lambda: m.AppearanceAssetId, None)
    if aid is not None and _safe(lambda: aid.IntegerValue, -1) >= 0:
        ae = _safe(lambda: doc.GetElement(aid), None)
        if ae is not None and len(_reflect_appearance_samples) < 60:
            _reflect_appearance_samples.append(ae)

    sid = _safe(lambda: m.StructuralAssetId, None)
    if sid is not None and _safe(lambda: sid.IntegerValue, -1) >= 0:
        se = _safe(lambda: doc.GetElement(sid), None)
        sa = _safe(lambda: se.GetStructuralAsset(), None) if se is not None else None
        if sa is not None and len(_reflect_structural_asset_samples) < 60:
            _reflect_structural_asset_samples.append(sa)

    tid = _safe(lambda: m.ThermalAssetId, None)
    if tid is not None and _safe(lambda: tid.IntegerValue, -1) >= 0:
        te = _safe(lambda: doc.GetElement(tid), None)
        ta = _safe(lambda: te.GetThermalAsset(), None) if te is not None else None
        if ta is not None and len(_reflect_thermal_asset_samples) < 60:
            _reflect_thermal_asset_samples.append(ta)

_reflection_records_material = _run_reflection_sweep(_reflect_material_samples, "Material", "materials")
_reflection_records_appearance = _run_reflection_sweep(_reflect_appearance_samples, "AppearanceAssetElement", "materials")
_reflection_records_structural = _run_reflection_sweep(_reflect_structural_asset_samples, "StructuralAsset", "materials")
_reflection_records_thermal = _run_reflection_sweep(_reflect_thermal_asset_samples, "ThermalAsset", "materials")

_reflection_records = (
    _reflection_records_material
    + _reflection_records_appearance
    + _reflection_records_structural
    + _reflection_records_thermal
)

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

# -------------------------
# Assemble labeled output payload
# -------------------------

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "materials",
        "records": param_inventory,
        "diagnostics": {
            "raw_material_count": len(all_materials),
            "selected_material_sample_count": len(selected),
            "appearance_asset_samples": len(_reflect_appearance_samples),
            "structural_asset_samples": len(_reflect_structural_asset_samples),
            "thermal_asset_samples": len(_reflect_thermal_asset_samples),
        },
    },
    {
        "kind": "crosswalk",
        "domain": "materials",
        "records": optional_crosswalk,
    },
    {
        "kind": "reflection",
        "domain": "materials",
        "records": _reflection_records,
    },
]

# -------------------------
# Optional: write to JSON
# -------------------------

file_written = None
write_error = None

if write_json:
    try:
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None
        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("materials", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

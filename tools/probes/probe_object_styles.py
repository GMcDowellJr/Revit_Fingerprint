# -*- coding: utf-8 -*-
# Dynamo Python (Revit) - Breadth Probe: object_styles (INVENTORY OUTPUT)
#
# Object Styles are Category graphics settings (not ElementTypes). This probe
# inventories key Category properties as a parameter-like surface.
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "object_styles",
#     "records": [...],
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "object_styles",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_categories_to_inspect (int)   Default: 500
#   IN[1] enable_crosswalk (bool)          Default: False
#   IN[2] per_bucket_limit (int)           Default: 30  (bucket = CategoryType|is_sub)
#   IN[3] include_subcategories (bool)     Default: True
#   IN[4] write_json (bool)                Default: False
#   IN[5] output_directory (str)           Default: None
#   IN[6] crosswalk_limit (int)            Default: 50

import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    ElementId,
    GraphicsStyleType,
    UnitFormatUtils,
    CategoryType,
)

doc = DocumentManager.Instance.CurrentDBDocument

max_categories_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_bucket_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 30
include_subcategories = IN[3] if len(IN) > 3 and IN[3] is not None else True
write_json = IN[4] if len(IN) > 4 and IN[4] is not None else False
out_path = IN[5] if len(IN) > 5 and IN[5] is not None else None
crosswalk_limit = IN[6] if len(IN) > 6 and IN[6] is not None else 50

# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _color_hex(c):
    if c is None:
        return None
    try:
        return "#{:02X}{:02X}{:02X}".format(int(c.Red), int(c.Green), int(c.Blue))
    except:
        return None

def _rgb_triplet(c):
    # Autodesk.Revit.DB.Color -> "R|G|B"
    if c is None:
        return None
    try:
        r = int(c.Red)
        g = int(c.Green)
        b = int(c.Blue)
        return "{}|{}|{}".format(r, g, b)
    except:
        return None

def _hex_from_rgb_triplet(rgb):
    if not rgb:
        return None
    try:
        parts = rgb.split("|")
        if len(parts) != 3:
            return None
        r = int(parts[0]); g = int(parts[1]); b = int(parts[2])
        return "#{:02X}{:02X}{:02X}".format(r & 0xFF, g & 0xFF, b & 0xFF)
    except:
        return None

def _get_name(obj):
    return _safe(lambda: obj.Name, None) if obj is not None else None

def _eid_name(eid):
    if eid is None or eid == ElementId.InvalidElementId:
        return None
    ref = _safe(lambda: doc.GetElement(eid), None)
    return _get_name(ref)

def _contract_missing(storage):
    return {"q": "missing", "storage": storage, "raw": None, "display": None, "norm": None}

def _contract_unreadable(storage):
    return {"q": "unreadable", "storage": storage, "raw": None, "display": None, "norm": None}

def _contract_unsupported(storage):
    return {"q": "unsupported", "storage": storage, "raw": None, "display": None, "norm": None}

def _contract_string(raw):
    return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}

def _contract_int(raw, display=None, norm=None):
    d = display if display is not None else (str(raw) if raw is not None else None)
    return {"q": "ok", "storage": "Integer", "raw": raw, "display": d, "norm": norm if norm is not None else raw}

def _contract_eid(eid, display_name=None):
    if eid is None or eid == ElementId.InvalidElementId:
        return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
    raw = _safe(lambda: eid.IntegerValue, None)
    dn = display_name if display_name is not None else _eid_name(eid)
    return {"q": "ok", "storage": "ElementId", "raw": raw, "display": dn if dn is not None else (str(raw) if raw is not None else None), "norm": raw}

def _maybe_set_example(entry, pv):
    ex = entry.get("example")
    if ex is None:
        entry["example"] = {k: pv.get(k) for k in ("q", "storage", "raw", "display", "norm")}
        return
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {k: pv.get(k) for k in ("q", "storage", "raw", "display", "norm")}

def _obs_sig(pv):
    st = str(pv.get("storage"))
    norm = pv.get("norm")
    try:
        norm_s = json.dumps(norm, sort_keys=True)
    except:
        norm_s = str(norm)
    return (st, norm_s)

# -------------------------
# Discovery + Sampling
# -------------------------

def _category_type_label(cat):
    """
    Return a human-readable CategoryType label.
    Handles environments where str(CategoryType) collapses to numeric codes.
    """
    ct = _safe(lambda: cat.CategoryType, None)
    if ct is None:
        return None

    # Prefer numeric enum mapping if possible
    try:
        code = int(ct)
        # Common Revit CategoryType enum values:
        # 0 = Model
        # 1 = Annotation
        # 2 = AnalyticalModel
        # 3 = Internal
        m = {
            0: "Model",
            1: "Annotation",
            2: "AnalyticalModel",
            3: "Internal",
        }
        return m.get(code, str(code))
    except:
        pass

    # Fallback: string enum name
    try:
        s = str(ct)
        return s.split(".")[-1] if "." in s else s
    except:
        return None

def _infer_object_styles_tab(cat, parent_cat=None):
    """
    Best-effort classification into the Object Styles UI tabs.
    Heuristic only. Uses normalized CategoryType labels to avoid numeric collapse.
    """
    name = _get_name(cat) or ""
    parent_name = _get_name(parent_cat) or ""

    ct_label = _category_type_label(cat) or "Unknown"

    n = (name + " " + parent_name).lower()
    if "imports in families" in n or "import" in n or "dwg" in n:
        return "Imported"

    if ct_label == "Annotation":
        return "Annotation"
    if ct_label == "AnalyticalModel" or name.lower().startswith("analytical"):
        return "Analytical"
    if ct_label == "Model":
        return "Model"

    return "Other"

def _iter_categories(include_subcats_flag):
    cats = _safe(lambda: doc.Settings.Categories, None)
    if cats is None:
        return []

    out = []
    try:
        for c in cats:
            out.append((c, False, None))
            if include_subcats_flag:
                subs = _safe(lambda: c.SubCategories, None)
                if subs is not None:
                    try:
                        for sc in subs:
                            out.append((sc, True, c))
                    except:
                        pass
    except:
        return []

    return out

all_cats = _iter_categories(include_subcategories)

selected = []
by_bucket = {}
for (cat, is_sub, parent) in all_cats:
    ct = _safe(lambda: cat.CategoryType, None)
    ct_label = str(ct) if ct is not None else "unknown"
    tab = _infer_object_styles_tab(cat, parent)
    bucket = "{}|tab={}|sub={}".format(ct_label, tab, 1 if is_sub else 0)

    c = by_bucket.get(bucket, 0)
    ok = True
    if per_bucket_limit is not None:
        try:
            ok = c < int(per_bucket_limit)
        except:
            ok = c < 30

    if ok:
        selected.append((cat, is_sub, parent))
        by_bucket[bucket] = c + 1

try:
    max_n = int(max_categories_to_inspect)
    if max_n >= 0:
        selected = selected[:max_n]
except:
    pass

if len(selected) == 0 and len(all_cats) > 0:
    # fallback: 1 per bucket
    seen = set()
    for (cat, is_sub, parent) in all_cats:
        ct = _safe(lambda: cat.CategoryType, None)
        ct_label = str(ct) if ct is not None else "unknown"
        bucket = "{}|sub={}".format(ct_label, 1 if is_sub else 0)
        if bucket not in seen:
            selected.append((cat, is_sub, parent))
            seen.add(bucket)
        if len(selected) >= 25:
            break

# -------------------------
# Inventory
# -------------------------

# Param definitions: (param_key, value_fn(cat,is_sub,parent))

def _bool_int(x):
    return 1 if bool(x) else 0

PARAM_DEFS = [
    ("c.name", lambda cat, is_sub, parent: _contract_string(_get_name(cat)) if _get_name(cat) is not None else _contract_missing("String")),
    ("c.parent_name", lambda cat, is_sub, parent: _contract_string(_get_name(parent)) if parent is not None and _get_name(parent) is not None else _contract_missing("String")),
    ("c.is_subcategory", lambda cat, is_sub, parent: _contract_int(1 if is_sub else 0, display="1" if is_sub else "0")),
    ("c.category_type", lambda cat, is_sub, parent: _contract_string(_category_type_label(cat)) if _category_type_label(cat) is not None else _contract_missing("String")),
    ("c.builtin_category", lambda cat, is_sub, parent: _contract_string(str(_safe(lambda: cat.BuiltInCategory, None))) if _safe(lambda: cat.BuiltInCategory, None) is not None else _contract_missing("String")),
    ("c.tab", lambda cat, is_sub, parent: _contract_string(_infer_object_styles_tab(cat, parent))),
    ("c.line_color.rgb",
     lambda cat, is_sub, parent:
         _contract_string(_rgb_triplet(_safe(lambda: cat.LineColor, None)))
         if _rgb_triplet(_safe(lambda: cat.LineColor, None)) is not None
         else _contract_missing("String")),

    ("c.line_color.hex",
     lambda cat, is_sub, parent:
         _contract_string(_hex_from_rgb_triplet(_rgb_triplet(_safe(lambda: cat.LineColor, None))))
         if _rgb_triplet(_safe(lambda: cat.LineColor, None)) is not None
         else _contract_missing("String")),

    ("c.line_weight_projection", lambda cat, is_sub, parent: _contract_int(_safe(lambda: cat.GetLineWeight(GraphicsStyleType.Projection), None)) if _safe(lambda: cat.GetLineWeight(GraphicsStyleType.Projection), None) is not None else _contract_missing("Integer")),
    ("c.line_weight_cut", lambda cat, is_sub, parent: _contract_int(_safe(lambda: cat.GetLineWeight(GraphicsStyleType.Cut), None)) if _safe(lambda: cat.GetLineWeight(GraphicsStyleType.Cut), None) is not None else _contract_missing("Integer")),

    ("c.line_pattern_projection", lambda cat, is_sub, parent: _contract_eid(_safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Projection), None)) if _safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Projection), None) is not None else _contract_missing("ElementId")),
    ("c.line_pattern_cut", lambda cat, is_sub, parent: _contract_eid(_safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Cut), None)) if _safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Cut), None) is not None else _contract_missing("ElementId")),

    ("c.material", lambda cat, is_sub, parent: _contract_eid(_safe(lambda: cat.Material, None)) if _safe(lambda: cat.Material, None) is not None else _contract_missing("ElementId")),

    ("c.allows_visibility_control", lambda cat, is_sub, parent: _contract_int(_bool_int(_safe(lambda: cat.AllowsVisibilityControl, None)), display=str(bool(_safe(lambda: cat.AllowsVisibilityControl, None))), norm=_bool_int(_safe(lambda: cat.AllowsVisibilityControl, None))) if _safe(lambda: cat.AllowsVisibilityControl, None) is not None else _contract_missing("Integer")),
    ("c.can_add_subcategory", lambda cat, is_sub, parent: _contract_int(_bool_int(_safe(lambda: cat.CanAddSubcategory, None)), display=str(bool(_safe(lambda: cat.CanAddSubcategory, None))), norm=_bool_int(_safe(lambda: cat.CanAddSubcategory, None))) if _safe(lambda: cat.CanAddSubcategory, None) is not None else _contract_missing("Integer")),
    ("c.has_material_quantities", lambda cat, is_sub, parent: _contract_int(_bool_int(_safe(lambda: cat.HasMaterialQuantities, None)), display=str(bool(_safe(lambda: cat.HasMaterialQuantities, None))), norm=_bool_int(_safe(lambda: cat.HasMaterialQuantities, None))) if _safe(lambda: cat.HasMaterialQuantities, None) is not None else _contract_missing("Integer")),
    ("c.is_cuttable", lambda cat, is_sub, parent: _contract_int(_bool_int(_safe(lambda: cat.IsCuttable, None)), display=str(bool(_safe(lambda: cat.IsCuttable, None))), norm=_bool_int(_safe(lambda: cat.IsCuttable, None))) if _safe(lambda: cat.IsCuttable, None) is not None else _contract_missing("Integer")),
]

param_index = {}

for (cat, is_sub, parent) in selected:
    ct = _safe(lambda: cat.CategoryType, None)
    ct_label = str(ct) if ct is not None else "unknown"
    tab = _infer_object_styles_tab(cat, parent)
    bucket = "{}|tab={}|sub={}".format(ct_label, tab, 1 if is_sub else 0)

    for (pk, fn) in PARAM_DEFS:
        if pk not in param_index:
            param_index[pk] = {
                "storage_types": set(),
                "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
                "example": None,
                "observed_on_buckets": set()
            }

        entry = param_index[pk]
        
        pv = fn(cat, is_sub, parent)

        st = pv.get("storage")
        q = pv.get("q") or "unreadable"

        if st:
            entry["storage_types"].add(st)

        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1

        entry["observed_on_buckets"].add(bucket)
        _maybe_set_example(entry, pv)

param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "object_styles",
        "param_key": pk,
        "selected_category_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
        }
    })

# -------------------------
# Optional Crosswalk: Category -> (LinePattern, Material)
# -------------------------

optional_crosswalk = []
if enable_crosswalk:
    try:
        lim = int(crosswalk_limit)
    except:
        lim = 50

    for (cat, is_sub, parent) in selected:
        if len(optional_crosswalk) >= lim:
            break

        pid = _safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Projection), None)
        cid = _safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Cut), None)
        mid = _safe(lambda: cat.Material, None)

        row = {
            "category.name": _get_name(cat),
            "category.type": str(_safe(lambda: cat.CategoryType, None)),
            "category.is_subcategory": True if is_sub else False,
            "category.parent_name": _get_name(parent),
            "projection.line_pattern": None,
            "cut.line_pattern": None,
            "material": None,
        }

        if pid is not None and pid != ElementId.InvalidElementId:
            row["projection.line_pattern"] = {"type_id": _safe(lambda: pid.IntegerValue, None), "name": _eid_name(pid)}
        if cid is not None and cid != ElementId.InvalidElementId:
            row["cut.line_pattern"] = {"type_id": _safe(lambda: cid.IntegerValue, None), "name": _eid_name(cid)}
        if mid is not None and mid != ElementId.InvalidElementId:
            row["material"] = {"type_id": _safe(lambda: mid.IntegerValue, None), "name": _eid_name(mid)}

        if row["projection.line_pattern"] is None and row["cut.line_pattern"] is None and row["material"] is None:
            continue

        optional_crosswalk.append(row)

# -------------------------
# Assemble OUT + optional JSON write
# -------------------------


# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection
# -------------------------
# Complements the curated/dynamic capture above with a breadth-only sweep of
# the sampled objects' .NET properties and zero-arg methods. This is
# diagnostics/breadth, not identity -- it surfaces members a fixed/curated
# key list or a Parameters-only walk could otherwise miss.

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
        for m in t.GetMethods():
            try:
                n = m.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
                    continue
                if m.GetParameters().Length != 0:
                    continue
                if m.IsSpecialName:
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
# docs/method_invocation_candidates_annotated.csv): these 32 method names (33
# (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
# zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
# GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
# see the notes below the dict) are ground-truth confirmed, against the live
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

# LinePatternElement.GetLinePattern was removed from the allowlist above
# after a live re-run (PR #398's exception-capture work, which surfaced the
# error text for the first time) showed it fails 100% of the time -- not
# with a documented Revit API exception (unlike GetCalloutParentId/
# GetExternalFileReference/GetModelToProjectionTransforms above, which each
# match a real InvalidOperationException precondition stated on their own
# RevitAPI doc pages), but with the same CLR/pythonnet interop binding
# failure family as Element.GetValidTypes above: `TypeError: No method
# matches given arguments for GetLinePattern: (<class
# 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
# binder before it ever reaches Revit's implementation, so it cannot
# succeed through `getattr(obj, name)()` regardless of model, Revit
# version, or which element is sampled -- keeping it allowlisted only adds
# permanent error noise for zero chance of real data.

_METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
# see the identity check in _reflect_contract below for why this must never be
# comparable-by-value to a real Revit return.

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
    if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
        # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
        # unique object(), never a string, specifically so a genuine reflected
        # property or allowlisted-method return whose real value happens to be
        # the literal text "<method not invoked>" cannot collide with this
        # placeholder and get misclassified/dropped (flagged in PR #398 review:
        # an earlier version of this check compared by value against a string
        # constant, which had exactly that collision risk). Checked before
        # isinstance(raw_v, str) specifically so it never reaches that branch.
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

_reflection_records_0 = _run_reflection_sweep([c for (c, _, _) in selected], "Category", "object_styles")
_reflection_records = _reflection_records_0

OUT_payload = [
    {"kind": "inventory", "domain": "object_styles", "records": param_inventory},
    {"kind": "crosswalk", "domain": "object_styles", "records": optional_crosswalk},
    {"kind": "reflection", "domain": "object_styles", "records": _reflection_records},
]

file_written = None
write_error = None

# -------------------------
# Unified run metadata (release-separated, not date-filename-separated)
# -------------------------
# extraction_date lives as JSON metadata, not as a filename token; the
# filename groups by Revit release (revit_version) plus an opaque run_id so
# repeated runs don't collide. See tools/probes/build_probe_inventory.py,
# which consumes this shape directly.

import uuid as _uuid_mod

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
        return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
    except:
        return _uuid_mod.uuid4().hex[:12]

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


if write_json:
    try:
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None
        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("object_styles", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

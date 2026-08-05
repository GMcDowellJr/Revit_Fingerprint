# Dynamo Python (Revit) — Breadth Probe: arrowheads (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "arrowheads",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "arrowheads",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_arrowheads_to_inspect (int)
#        Maximum number of arrowhead ElementTypes to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit DimensionType → Arrowhead crosswalk.
#        Default: False
#
#   IN[2] per_style_limit (int)
#        Sample at most N arrowhead types per Arrow Style value
#        (set large to effectively scan all).
#        Default: 2
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probes_<revit_version>_<run_id>.json
#        If None, falls back to RVT directory, then TEMP.


import clr
import hashlib
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId, ElementType,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    BuiltInParameter
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_arrowheads_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_style_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 2
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None

# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

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
    """
    Contract:
      "<k>": {
        "q": "ok|missing|unreadable|unsupported",
        "storage": "String|Integer|Double|ElementId|None",
        "raw": ...,
        "display": ...,
        "norm": ...
      }

    Probe choice (important):
      - Integer.norm stays as raw int (do NOT coerce 0/1 to bool),
        because many ints are enums (e.g. Arrow Style).
      - Length -> inches (float) when datatype is Length
      - Angle  -> degrees (float) when datatype is Angle
      - ElementId -> IntegerValue (norm=int), display tries to resolve name cheaply
    """
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
        # keep norm as integer (enum-safe)
        return {
            "q": "ok",
            "storage": "Integer",
            "raw": raw,
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
            if ref_name is None:
                ref_name = _safe(lambda: _safe_type_name(ref), None)

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _looks_like_arrowhead_type(t):
    # Heuristic: must have the canonical arrowhead params
    required = ["Arrow Style", "Tick Size"]
    optional = ["Fill Tick", "Arrow Closed", "Arrow Width Angle", "Heavy End Pen Weight", "Tick Mark Centered"]
    try:
        for pn in required:
            if t.LookupParameter(pn) is None:
                return False
        for pn in optional:
            if t.LookupParameter(pn) is not None:
                return True
        return False
    except:
        return False

def _arrow_style_key(t):
    # group sampling by Arrow Style (raw int + display label)
    p = _safe(lambda: t.LookupParameter("Arrow Style"), None)
    if p is None:
        return ("missing", None)
    pv = _format_param_contract(p)
    raw = pv.get("raw")
    disp = pv.get("display")
    return ("{}|{}".format(raw, disp), pv)


# -------------------------
# Discovery + Sampling
# -------------------------

all_types = _safe(
    lambda: (FilteredElementCollector(doc)
             .WhereElementIsElementType()
             .OfClass(ElementType)
             .ToElements()),
    default=[]
)

try:
    all_types = list(all_types)
except:
    all_types = list(all_types)

hits = []
for t in all_types:
    if _looks_like_arrowhead_type(t):
        hits.append(t)

# Cap AFTER filtering so collector ordering can't hide arrowheads
try:
    max_n = int(max_arrowheads_to_inspect)
    if max_n >= 0:
        hits = hits[:max_n]
except:
    pass

hits = []
for t in all_types:
    if _looks_like_arrowhead_type(t):
        hits.append(t)

# Sample first N per Arrow Style (breadth bias)
selected = []
by_style = {}  # style_key -> count
for t in hits:
    sk, _ = _arrow_style_key(t)
    c = by_style.get(sk, 0)
    if per_style_limit is None:
        per_style_ok = True
    else:
        try:
            per_style_ok = c < int(per_style_limit)
        except:
            per_style_ok = c < 2
    if per_style_ok:
        selected.append(t)
        by_style[sk] = c + 1

# If per_style_limit is 0 or negative, fallback to at least 1 per style
if len(selected) == 0 and len(hits) > 0:
    seen = set()
    for t in hits:
        sk, _ = _arrow_style_key(t)
        if sk not in seen:
            selected.append(t)
            seen.add(sk)


# -------------------------
# Build inventory (union over selected)
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   observed_on_style_keys: set(str)
# }
param_index = {}

def _maybe_set_example(entry, pv):
    # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
    if pv is None:
        return
    ex = entry.get("example")
    if ex is None:
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }
        return
    # upgrade existing non-ok example to ok if we see one
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }

def _push_example(bucket, pv):
    # keep up to 5 distinct examples by (storage, norm, display)
    if pv is None:
        return
    sig = (str(pv.get("storage")), str(pv.get("norm")), str(pv.get("display")))
    for ex in bucket:
        exsig = (str(ex.get("storage")), str(ex.get("norm")), str(ex.get("display")))
        if exsig == sig:
            return
    if len(bucket) < 5:
        bucket.append({
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        })

for t in selected:
    style_key, style_pv = _arrow_style_key(t)

    params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(t.Parameters), default=[])

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
                "observed_on_style_keys": set()
            }

        entry = param_index[pk]

        st = pv.get("storage")
        q = pv.get("q") or "unreadable"

        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1

        entry["observed_on_style_keys"].add(style_key)
        _maybe_set_example(entry, pv)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "arrowheads",
        "param_key": pk,
        "selected_type_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_styles": sorted(list(e["observed_on_style_keys"]))[:25]
        }
    })



# -------------------------
# Optional Crosswalk: DimensionType -> Tick Mark (Arrowhead)
# -------------------------

optional_crosswalk = []

DIM_TICK_PARAM_CANDIDATES = [
    "Tick Mark",
    "Tick mark",
    "Tick Mark Type",
    "Tick Mark Symbol",
]

def _collect_dimension_types_with_tick_param():
    candidates = set([n.strip().lower() for n in DIM_TICK_PARAM_CANDIDATES])
    hits_local = []
    for t in all_types:
        try:
            params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
            if params is None:
                params = _safe(lambda: list(t.Parameters), default=[])
            for p in params:
                dn = _safe(lambda: _safe_param_def_name(p), None)
                if dn and dn.strip().lower() in candidates:
                    hits_local.append(t)
                    break
        except:
            continue
    return hits_local

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


# arrowhead name + workset lookup (from all hits, not just selected)
arrowhead_name_by_id = {}
arrowhead_workset_by_id = {}
for t in hits:
    tid = _safe(lambda: t.Id.IntegerValue, None)
    if tid is not None and tid not in arrowhead_name_by_id:
        arrowhead_name_by_id[tid] = _safe_type_name(t)
        t_ws_id_obj = _safe(lambda: t.WorksetId, None)
        t_ws_name, _t_ws_resolved = _resolve_workset(doc, t_ws_id_obj)
        t_ws_id_int = _safe(lambda: t_ws_id_obj.IntegerValue, None) if t_ws_id_obj is not None else None
        arrowhead_workset_by_id[tid] = (t_ws_id_int, t_ws_name)

if enable_crosswalk:
    # Optional extra input: max crosswalk rows to emit (default 25)
    crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 25

    dim_types = _collect_dimension_types_with_tick_param()

    # Keep crosswalk compact: one representative DimensionType per distinct Arrowhead type_id
    seen_arrowhead_ids = set()

    for dt in dim_types:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        row = {
            "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
            "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
            "tick_param.matched_name": None,
            "tick_param": None,
            "arrowhead.resolved": False,
            "arrowhead.type_id": None,
            "arrowhead.name": None
        }

        p = None
        matched = None
        for cand in DIM_TICK_PARAM_CANDIDATES:
            p = _safe(lambda: dt.LookupParameter(cand), None)
            if p is not None:
                matched = cand
                break

        row["tick_param.matched_name"] = matched
        row["tick_param"] = _format_param_contract(p)

        # Only keep rows that resolve to an Arrowhead ElementId
        if row["tick_param"]["storage"] != "ElementId" or row["tick_param"]["raw"] is None:
            continue

        ah_id = int(row["tick_param"]["raw"])
        if ah_id in seen_arrowhead_ids:
            continue

        row["arrowhead.type_id"] = ah_id
        row["arrowhead.name"] = arrowhead_name_by_id.get(ah_id)

        if row["arrowhead.name"] is None:
            ref = _safe(lambda: doc.GetElement(ElementId(ah_id)), None)
            row["arrowhead.name"] = _safe(lambda: ref.Name, None) if ref is not None else None

        row["arrowhead.resolved"] = True if row["arrowhead.name"] is not None else False

        # Keep only resolved mappings (signal > noise)
        if not row["arrowhead.resolved"]:
            continue

        ah_ws_id_int, ah_ws_name = arrowhead_workset_by_id.get(ah_id, (None, None))
        row["arrowhead.workset_id"] = ah_ws_id_int
        row["arrowhead.workset_name"] = ah_ws_name

        seen_arrowhead_ids.add(ah_id)
        optional_crosswalk.append(row)


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

_reflection_records_0 = _run_reflection_sweep(selected, "ArrowheadType", "arrowheads")
_reflection_records = _reflection_records_0

# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "reflection",
        "domain": "arrowheads",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "arrowheads",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "arrowheads",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
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
        # Choose default directory: RVT folder if possible, else temp
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None

        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            try:
                default_dir = os.path.dirname(rvt_path)
            except:
                default_dir = None

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        # IN[4] is treated as an output directory (not a filename)
        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("arrowheads", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to inventory header (keeps OUT shape stable)
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

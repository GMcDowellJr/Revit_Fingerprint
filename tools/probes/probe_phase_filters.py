# tools/probes/probe_phase_filters.py
#
# Dynamo Python (Revit) — Breadth Probe: phase_filters (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "phase_filters",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "phase_filters",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_phase_filters_to_inspect (int)
#        Maximum number of PhaseFilters to inspect.
#        Default: 200
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit View → PhaseFilter crosswalk.
#        Default: False
#
#   IN[2] max_views_to_scan (int)
#        When crosswalk enabled, scan at most N views for Phase Filter assignments.
#        Default: 2000
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
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    BuiltInParameter, View
)

# PhaseFilter / PhaseStatus are present in common Revit builds,
# but import defensively for Dynamo environments.
try:
    from Autodesk.Revit.DB import PhaseFilter
except:
    PhaseFilter = None

try:
    from Autodesk.Revit.DB import PhaseStatus
except:
    PhaseStatus = None

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_phase_filters_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
max_views_to_scan = IN[2] if len(IN) > 2 and IN[2] is not None else 2000
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

def _safe_elem_name(elem):
    # Prefer Revit's Name property where present.
    try:
        n = elem.Name
        if n:
            return n
    except:
        pass
    # Fall back to common type-name params if available
    for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
        try:
            p = elem.get_Parameter(bip)
            if p is not None:
                s = p.AsString()
                if s:
                    return s
        except:
            pass
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
      {
        "q": "ok|missing|unreadable|unsupported",
        "storage": "String|Integer|Double|ElementId|None",
        "raw": ...,
        "display": ...,
        "norm": ...
      }

    Probe choices:
      - Integer.norm stays integer (enum-safe; do NOT coerce to bool)
      - Length -> inches (float) when datatype is Length
      - Angle  -> degrees (float) when datatype is Angle
      - ElementId -> IntegerValue (norm=int), display tries to resolve element name cheaply
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
            ref_name = _safe(lambda: _safe_elem_name(ref), None)

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}


# -------------------------
# Domain-specific breadth bucket: Phase status presentations
# -------------------------

def _phase_status_bucket(pf):
    if pf is None:
        return "unsupported|pf"

    parts = []
    for status_name in STATUS_ORDER:
        status_enum = _status_enum(status_name)
        if status_enum is None:
            parts.append("{}=?".format(status_name))
            continue

        try:
            pres = pf.GetPhaseStatusPresentation(status_enum)
            token = str(pres)
            token_to_label = {
                # Enum token forms
                "ByCategory": "By Category",
                "NotDisplayed": "Not Displayed",
                "Overridden": "Overridden",

                # Numeric token forms observed in some builds (confirmed by user)
                "0": "By Category",
                "1": "Not Displayed",
                "2": "Overridden",

                # Defensive: if int slips through before str()
                0: "By Category",
                1: "Not Displayed",
                2: "Overridden",
            }
            label = token_to_label.get(token, token)
            parts.append("{}={}".format(status_name, label))
        except:
            parts.append("{}=?".format(status_name))

    return "|".join(parts)

# -------------------------
# Discovery (progressive)
# -------------------------

discovery_notes = []

phase_filters = []

# Step 1 (preferred): class-based collector (category-free)
if PhaseFilter is not None:
    phase_filters = _safe(
        lambda: (FilteredElementCollector(doc)
                 .OfClass(PhaseFilter)
                 .ToElements()),
        default=[]
    )
    discovery_notes.append("collector: OfClass(PhaseFilter)")
else:
    discovery_notes.append("collector: PhaseFilter class import unavailable")

try:
    phase_filters = list(phase_filters)
except:
    phase_filters = list(phase_filters)

# Cap scan explicitly
try:
    nmax = int(max_phase_filters_to_inspect)
    if nmax >= 0:
        phase_filters = phase_filters[:nmax]
except:
    pass


# -------------------------
# Build inventory (union over discovered phase filters)
# -------------------------

# Inventory policy for this domain:
# - PhaseFilter often exposes few/no "Parameters"; the meaningful surface is the
#   per-status presentation setting used by the exporter (GetPhaseStatusPresentation).
# - Therefore we synthesize "probe parameters" aligned to exporter identity items:
#     phase_filter.<status>.presentation_id  (Integer)
#   plus a coordination/name item:
#     phase_filter.name  (String)
#
# We still attempt to include any actual Revit Parameters found on PhaseFilter,
# but those are additive-only and not relied upon for non-empty inventory.

STATUS_ORDER = ["New", "Existing", "Demolished", "Temporary"]

def _status_enum(status_name):
    if PhaseStatus is not None:
        return _safe(lambda: getattr(PhaseStatus, status_name), None)
    # Exporter uses ElementOnPhaseStatus; probe may not have it imported.
    try:
        from Autodesk.Revit.DB import ElementOnPhaseStatus
        return _safe(lambda: getattr(ElementOnPhaseStatus, status_name), None)
    except:
        return None

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
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }

def _add_inventory_obs(param_key, pv, pf_name=None, bucket=None):
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set(),
            "observed_on_names": set()
        }

    entry = param_index[param_key]

    st = pv.get("storage")
    q = pv.get("q") or "unreadable"

    if st:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    if bucket:
        entry["observed_on_buckets"].add(bucket)
    if pf_name:
        entry["observed_on_names"].add(pf_name)

    _maybe_set_example(entry, pv)


# param_key -> accumulator (same shape as before)
param_index = {}

for pf in phase_filters:
    pf_name = _safe(lambda: _safe_elem_name(pf), None)
    bucket = _phase_status_bucket(pf)

    # --- Synthesized, exporter-modeled surfaces (authoritative) ---
    # phase_filter.name
    name_val = _safe(lambda: getattr(pf, "Name", None), None)
    pv_name = {
        "q": "ok" if (name_val is not None and str(name_val) != "") else "missing",
        "storage": "String",
        "raw": name_val,
        "display": name_val,
        "norm": name_val
    }
    _add_inventory_obs("phase_filter.name", pv_name, pf_name=pf_name, bucket=bucket)

    # phase_filter.<status>.presentation_id (Integer)
for status_name in STATUS_ORDER:
    status_enum = _status_enum(status_name)
    k = "phase_filter.{}.presentation_id".format(status_name.lower())

    try:
        if status_enum is None:
            raise Exception("status enum unavailable")

        pres = pf.GetPhaseStatusPresentation(status_enum)
        token = str(pres)

        token_to_label = {
            # Enum token forms
            "ByCategory": "By Category",
            "NotDisplayed": "Not Displayed",
            "Overridden": "Overridden",

            # Numeric token forms observed in some builds (confirmed by user)
            "0": "By Category",
            "1": "Not Displayed",
            "2": "Overridden",

            # Defensive: if int slips through before str()
            0: "By Category",
            1: "Not Displayed",
            2: "Overridden",
        }

        label = token_to_label.get(token, token)

        pv = {
            "q": "ok",
            "storage": "String",
            "raw": label,
            "display": label,
            "norm": label
        }
    except:
        pv = {
            "q": "unreadable",
            "storage": "String",
            "raw": None,
            "display": None,
            "norm": None
        }

    _add_inventory_obs(k, pv, pf_name=pf_name, bucket=bucket)
    # --- Additive-only: actual Revit Parameters (if any) ---
    params = _safe(lambda: list(pf.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(pf.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)
        pv = _format_param_contract(p)
        _add_inventory_obs(pk, pv, pf_name=pf_name, bucket=bucket)


# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "phase_filters",
        "param_key": pk,
        "selected_phase_filter_sample_count": len(phase_filters),
        "discovery": {
            "notes": discovery_notes[:10],
            "modeled_on_exporter": True if pk.startswith("phase_filter.") else False
        },
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            # breadth: cap for readability
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25],
            "observed_on_names": sorted(list(e["observed_on_names"]))[:25]
        }
    })

# -------------------------
# Optional Crosswalk: View -> PhaseFilter
# -------------------------

optional_crosswalk = []

VIEW_PHASE_FILTER_PARAM_CANDIDATES = [
    # UI-facing label (common)
    "Phase Filter",
    "Phase filter",
]

def _get_view_phase_filter_param(v):
    # Prefer BIP if present; fall back to name candidates.
    # Some builds expose the view setting via a built-in parameter.
    for bip in (
        _safe(lambda: BuiltInParameter.VIEW_PHASE_FILTER, None),
    ):
        if bip is None:
            continue
        p = _safe(lambda: v.get_Parameter(bip), None)
        if p is not None:
            return (str(bip), p)

    for cand in VIEW_PHASE_FILTER_PARAM_CANDIDATES:
        p = _safe(lambda: v.LookupParameter(cand), None)
        if p is not None:
            return (cand, p)

    return (None, None)

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


phase_filter_name_by_id = {}
phase_filter_workset_by_id = {}
for pf in phase_filters:
    pid = _safe(lambda: pf.Id.IntegerValue, None)
    if pid is not None and pid not in phase_filter_name_by_id:
        phase_filter_name_by_id[pid] = _safe(lambda: _safe_elem_name(pf), None)
        pf_ws_id_obj = _safe(lambda: pf.WorksetId, None)
        pf_ws_name, _pf_ws_resolved = _resolve_workset(doc, pf_ws_id_obj)
        pf_ws_id_int = _safe(lambda: pf_ws_id_obj.IntegerValue, None) if pf_ws_id_obj is not None else None
        phase_filter_workset_by_id[pid] = (pf_ws_id_int, pf_ws_name)

if enable_crosswalk:
    views = _safe(
        lambda: (FilteredElementCollector(doc)
                 .OfClass(View)
                 .ToElements()),
        default=[]
    )
    try:
        views = list(views)
    except:
        views = list(views)

    # Limit scan explicitly (avoid whole-model view scan on huge files)
    try:
        vcap = int(max_views_to_scan)
        if vcap >= 0:
            views = views[:vcap]
    except:
        pass

    # Keep crosswalk compact: one representative view per distinct phase_filter_id
    seen_pf_ids = set()

    for v in views:
        if v is None:
            continue
        # Skip view templates if easily detectable (older builds may differ)
        is_template = _safe(lambda: v.IsTemplate, False)
        if is_template:
            continue

        matched_name, p = _get_view_phase_filter_param(v)
        pv = _format_param_contract(p)

        # keep only ElementId payloads with a value
        if pv.get("storage") != "ElementId" or pv.get("raw") is None:
            continue

        pf_id = int(pv.get("raw"))
        if pf_id in seen_pf_ids:
            continue

        row = {
            "view.id": _safe(lambda: v.Id.IntegerValue, None),
            "view.name": _safe(lambda: v.Name, None),
            "phase_filter_param.matched_name": matched_name,
            "phase_filter_param": pv,
            "phase_filter.resolved": False,
            "phase_filter.id": pf_id,
            "phase_filter.name": phase_filter_name_by_id.get(pf_id)
        }

        if row["phase_filter.name"] is None:
            ref = _safe(lambda: doc.GetElement(ElementId(pf_id)), None)
            row["phase_filter.name"] = _safe(lambda: _safe_elem_name(ref), None) if ref is not None else None

        row["phase_filter.resolved"] = True if row["phase_filter.name"] is not None else False

        if not row["phase_filter.resolved"]:
            continue

        pf_ws_id_int, pf_ws_name = phase_filter_workset_by_id.get(pf_id, (None, None))
        row["phase_filter.workset_id"] = pf_ws_id_int
        row["phase_filter.workset_name"] = pf_ws_name

        seen_pf_ids.add(pf_id)
        optional_crosswalk.append(row)

# Assemble labeled output payload

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

_reflection_records_0 = _run_reflection_sweep(phase_filters, "PhaseFilter", "phase_filters")
_reflection_records = _reflection_records_0

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "phase_filters",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "phase_filters",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "phase_filters",
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
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

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
            json.dump(_probe_wrap("phase_filters", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to inventory header (keeps OUT shape stable)
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

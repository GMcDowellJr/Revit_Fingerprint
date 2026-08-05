# Dynamo Python (Revit) — Breadth Probe: fill_patterns (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "fill_patterns",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "fill_patterns",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_fill_patterns_to_inspect (int)
#        Maximum number of FillPatternElements to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] per_bucket_limit (int)
#        Sample at most N fill patterns per bucket.
#        Buckets are keyed by: target + is_solid + grid_count
#        Default: 3
#
#   IN[2] max_grids_per_pattern (int)
#        At most N fill grids to inspect per fill pattern when deriving
#        computed evidence (angles/offsets/line patterns).
#        Default: 4
#
#   IN[3] enable_crosswalk (bool)
#        Whether to emit FillPattern -> LinePattern crosswalk (via FillGrid.LinePatternId).
#        Default: False
#
#   IN[4] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[5] output_directory (str)
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
    BuiltInParameter,
    FillPatternElement
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_fill_patterns_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
per_bucket_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 3
max_grids_per_pattern = IN[2] if len(IN) > 2 and IN[2] is not None else 4
enable_crosswalk = IN[3] if len(IN) > 3 and IN[3] is not None else False
write_json = IN[4] if len(IN) > 4 and IN[4] is not None else False
out_path = IN[5] if len(IN) > 5 and IN[5] is not None else None


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _safe_type_name(elem):
    # FillPatternElement.Name is usually valid, but keep parity with reference probe.
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
      {
        "q": "ok|missing|unreadable|unsupported",
        "storage": "String|Integer|Double|ElementId|None",
        "raw": ...,
        "display": ...,
        "norm": ...
      }

    Probe choices:
      - Integer.norm stays as raw int (do NOT coerce 0/1 to bool).
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

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _contract_from_value(q, storage, raw, display, norm):
    # Used for computed / derived evidence that is not a Revit Parameter.
    return {
        "q": q,
        "storage": storage,
        "raw": raw,
        "display": display,
        "norm": norm
    }

def _to_inches(x_internal):
    if x_internal is None:
        return None
    return _safe(lambda: UnitUtils.ConvertFromInternalUnits(x_internal, UnitTypeId.Inches), x_internal)

def _to_degrees(x_internal):
    if x_internal is None:
        return None
    return _safe(lambda: UnitUtils.ConvertFromInternalUnits(x_internal, UnitTypeId.Degrees), x_internal)

def _bucket_key_for_fill_pattern(fpe):
    """
    Bucket = target + is_solid + grid_count (breadth-biased sampling).
    """
    try:
        fp = _safe(lambda: fpe.GetFillPattern(), None)
        if fp is None:
            return "missing_fillpattern"
        tgt = _safe(lambda: str(fp.Target), None)
        solid = _safe(lambda: fp.IsSolidFill, None)
        gc = _safe(lambda: fp.GridCount, None)
        return "{}|solid={}|grids={}".format(tgt, solid, gc)
    except:
        return "unreadable_fillpattern"


# -------------------------
# Discovery + Sampling
# -------------------------
# Progressive strategy:
#   1) Category-free ElementType signature discovery (not viable here; FillPatterns are elements, not ElementTypes)
#   2) Class-based collector: FillPatternElement
#   3) Instance sampling (already inherent; FillPatternElement is an element)

all_fill_patterns = _safe(
    lambda: (FilteredElementCollector(doc)
             .OfClass(FillPatternElement)
             .ToElements()),
    default=[]
)

try:
    all_fill_patterns = list(all_fill_patterns)
except:
    all_fill_patterns = list(all_fill_patterns)

# Cap AFTER discovery (collector ordering shouldn't hide rare buckets)
try:
    max_n = int(max_fill_patterns_to_inspect)
    if max_n >= 0:
        all_fill_patterns = all_fill_patterns[:max_n]
except:
    pass

# Breadth-biased sampling by bucket
selected = []
by_bucket = {}  # bucket_key -> count
for fpe in all_fill_patterns:
    bk = _bucket_key_for_fill_pattern(fpe)
    c = by_bucket.get(bk, 0)

    if per_bucket_limit is None:
        ok = True
    else:
        try:
            ok = c < int(per_bucket_limit)
        except:
            ok = c < 3

    if ok:
        selected.append(fpe)
        by_bucket[bk] = c + 1

# If per_bucket_limit is 0/negative, fallback to at least 1 per bucket
if len(selected) == 0 and len(all_fill_patterns) > 0:
    seen = set()
    for fpe in all_fill_patterns:
        bk = _bucket_key_for_fill_pattern(fpe)
        if bk not in seen:
            selected.append(fpe)
            seen.add(bk)


# -------------------------
# Build inventory (union over selected)
# -------------------------
# Inventory records are per parameter key:
#   - Revit parameters: "p.<DefinitionName>"
#   - Computed evidence: "fp.<key>"
#
# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   breadth: dict (lightweight)
# }
param_index = {}

def _ensure_entry(pk):
    if pk not in param_index:
        param_index[pk] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "breadth": {
                "observed_bucket_keys": set()
            }
        }
    return param_index[pk]

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

def _observe(pk, pv, bucket_key=None):
    entry = _ensure_entry(pk)

    st = pv.get("storage")
    q = pv.get("q") or "unreadable"

    if st:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    if bucket_key is not None:
        entry["breadth"]["observed_bucket_keys"].add(bucket_key)

    _maybe_set_example(entry, pv)


def _add_computed_surface(fpe, bucket_key):
    """
    Captures a conservative, join-key-relevant surface from FillPattern itself.
    This is not production export logic: it's evidence capture for later policy design.
    """
    fp = _safe(lambda: fpe.GetFillPattern(), None)
    if fp is None:
        _observe("fp.q", _contract_from_value("unreadable", "String", None, None, None), bucket_key)
        return

    # name
    fp_name = _safe(lambda: _safe_type_name(fpe), None)
    _observe("fp.name", _contract_from_value("ok", "String", fp_name, fp_name, fp_name), bucket_key)

    # target (Drafting/Model)
    tgt = _safe(lambda: fp.Target, None)

    # Prefer enum name. In pythonnet, enum.ToString() yields "Drafting"/"Model".
    # Some environments stringify enums as underlying integers ("0"/"1"), so map those too.
    tgt_s = None
    if tgt is not None:
        tgt_s = _safe(lambda: tgt.ToString(), None)
        if tgt_s is None:
            tgt_s = str(tgt)
        if tgt_s in ("0", "1"):
            tgt_s = "Drafting" if tgt_s == "0" else "Model"

    _observe("fp.target", _contract_from_value("ok", "String", tgt_s, tgt_s, tgt_s), bucket_key)

    # is_solid (store as Integer 1/0/None)
    is_solid = _safe(lambda: fp.IsSolidFill, None)
    is_solid_i = None
    if is_solid is True:
        is_solid_i = 1
    elif is_solid is False:
        is_solid_i = 0
    _observe("fp.is_solid", _contract_from_value("ok", "Integer", is_solid_i, str(is_solid), is_solid_i), bucket_key)

    # grid_count
    gc = _safe(lambda: fp.GridCount, None)
    _observe("fp.grid_count", _contract_from_value("ok", "Integer", gc, str(gc) if gc is not None else None, gc), bucket_key)

    # Derive a compact signature for the first N grids (angles/offsets/shifts/line patterns).
    # Note: We serialize list-like structures as String payloads to preserve the contract storage types.
    max_g = None
    try:
        max_g = int(max_grids_per_pattern)
    except:
        max_g = 4

    angles_deg = []
    offsets_in = []
    shifts_in = []
    origins_in = []
    line_pattern_ids = []

    # FillPattern.GetFillGrids() returns a collection of FillGrid
    grids = _safe(lambda: fp.GetFillGrids(), default=None)
    if grids is None:
        _observe("fp.grids.q", _contract_from_value("unreadable", "String", None, None, None), bucket_key)
        return

    try:
        grids = list(grids)
    except:
        grids = list(grids)

    for i, g in enumerate(grids):
        if max_g is not None and max_g >= 0 and i >= max_g:
            break

        ang = _safe(lambda: g.Angle, None)
        off = _safe(lambda: g.Offset, None)
        shf = _safe(lambda: g.Shift, None)
        org = _safe(lambda: g.Origin, None)
        lpid = _safe(lambda: g.LinePatternId, None)

        angles_deg.append(_to_degrees(ang))
        offsets_in.append(_to_inches(off))
        shifts_in.append(_to_inches(shf))

        # Origin is UV in feet (internal units) in many Revit contexts; convert components conservatively.
        try:
            if org is not None:
                ou = _safe(lambda: org.U, None)
                ov = _safe(lambda: org.V, None)
                origins_in.append([_to_inches(ou), _to_inches(ov)])
            else:
                origins_in.append(None)
        except:
            origins_in.append(None)

        if lpid is not None and lpid != ElementId.InvalidElementId:
            line_pattern_ids.append(_safe(lambda: lpid.IntegerValue, None))
        else:
            line_pattern_ids.append(None)

    # Serialize as JSON-ish strings for auditability.
    try:
        angles_s = json.dumps(angles_deg)
    except:
        angles_s = str(angles_deg)

    try:
        offsets_s = json.dumps(offsets_in)
    except:
        offsets_s = str(offsets_in)

    try:
        shifts_s = json.dumps(shifts_in)
    except:
        shifts_s = str(shifts_in)

    try:
        origins_s = json.dumps(origins_in)
    except:
        origins_s = str(origins_in)

    try:
        lpids_s = json.dumps(line_pattern_ids)
    except:
        lpids_s = str(line_pattern_ids)

    _observe("fp.grids.angles_deg", _contract_from_value("ok", "String", angles_s, angles_s, angles_s), bucket_key)
    _observe("fp.grids.offsets_in", _contract_from_value("ok", "String", offsets_s, offsets_s, offsets_s), bucket_key)
    _observe("fp.grids.shifts_in", _contract_from_value("ok", "String", shifts_s, shifts_s, shifts_s), bucket_key)
    _observe("fp.grids.origins_in", _contract_from_value("ok", "String", origins_s, origins_s, origins_s), bucket_key)
    _observe("fp.grids.line_pattern_ids", _contract_from_value("ok", "String", lpids_s, lpids_s, lpids_s), bucket_key)


for fpe in selected:
    bk = _bucket_key_for_fill_pattern(fpe)

    # Revit parameters on FillPatternElement
    params = _safe(lambda: list(fpe.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(fpe.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)
        pv = _format_param_contract(p)
        _observe(pk, pv, bk)

    # Computed surface for the FillPattern itself
    _add_computed_surface(fpe, bk)


# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "fill_patterns",
        "param_key": pk,
        "selected_element_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["breadth"]["observed_bucket_keys"]))[:25]
        }
    })


# -------------------------
# Optional Crosswalk: FillPattern -> LinePattern (via FillGrid.LinePatternId)
# -------------------------
optional_crosswalk = []

if enable_crosswalk:
    # Optional extra input: max crosswalk rows to emit (default 25)
    crosswalk_limit = IN[6] if len(IN) > 6 and IN[6] is not None else 25

    seen_line_pattern_ids = set()

    def _resolve_workset(doc, ws_id_obj):
        """Resolve an Element.WorksetId value to (name, resolved_bool) via
        WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
        distinct .NET type from ElementId (both happen to expose
        .IntegerValue, which is why reflection reports this member as
        ElementId-storage), and Workset is not derived from Element, so
        doc.GetElement() would never resolve it even with the right type
        assumed."""
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

    for fpe in selected:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        fp = _safe(lambda: fpe.GetFillPattern(), None)
        if fp is None:
            continue

        fp_id = _safe(lambda: fpe.Id.IntegerValue, None)
        fp_name = _safe(lambda: _safe_type_name(fpe), None)
        fp_target = _safe(lambda: str(fp.Target), None)
        fp_ws_id_obj = _safe(lambda: fpe.WorksetId, None)
        fp_ws_name, _fp_ws_resolved = _resolve_workset(doc, fp_ws_id_obj)
        fp_ws_id_int = _safe(lambda: fp_ws_id_obj.IntegerValue, None) if fp_ws_id_obj is not None else None

        grids = _safe(lambda: fp.GetFillGrids(), default=None)
        if grids is None:
            continue
        try:
            grids = list(grids)
        except:
            grids = list(grids)

        max_g = None
        try:
            max_g = int(max_grids_per_pattern)
        except:
            max_g = 4

        for gi, g in enumerate(grids):
            if len(optional_crosswalk) >= int(crosswalk_limit):
                break
            if max_g is not None and max_g >= 0 and gi >= max_g:
                break

            lp = _safe(lambda: g.LinePatternId, None)
            if lp is None or lp == ElementId.InvalidElementId:
                continue

            lp_id = _safe(lambda: lp.IntegerValue, None)
            if lp_id is None:
                continue

            if lp_id in seen_line_pattern_ids:
                continue

            lp_elem = _safe(lambda: doc.GetElement(lp), None)
            lp_name = _safe(lambda: lp_elem.Name, None) if lp_elem is not None else None

            optional_crosswalk.append({
                "fill_pattern.id": fp_id,
                "fill_pattern.name": fp_name,
                "fill_pattern.target": fp_target,
                "fill_pattern.workset_id": fp_ws_id_int,
                "fill_pattern.workset_name": fp_ws_name,
                "grid.index": gi,
                "line_pattern.id": lp_id,
                "line_pattern.name": lp_name,
            })

            seen_line_pattern_ids.add(lp_id)



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
        for item in raw_v:
            if not hasattr(item, "IntegerValue"):
                raise TypeError("non-ElementId item in collection")
            ids.append(int(item.IntegerValue))
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

_reflection_records_0 = _run_reflection_sweep(selected, "FillPatternElement", "fill_patterns")
_reflection_records = _reflection_records_0

# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "reflection",
        "domain": "fill_patterns",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "fill_patterns",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "fill_patterns",
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

        # IN[5] is treated as an output directory (not a filename)
        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("fill_patterns", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to inventory header (keeps OUT shape stable)
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

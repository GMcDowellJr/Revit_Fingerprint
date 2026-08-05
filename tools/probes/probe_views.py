# tools/probes/probe_views.py
#
# Dynamo Python (Revit) — Breadth Probe: views (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "reflection",
#     "domain": "views",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "inventory",
#     "domain": "views",
#     "records": [...]
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "views",
#     "records": [...]
#   }
# ]
#
# Reworked from the original bespoke findings-dict probe to match the
# reflection/inventory/crosswalk contract every other domain probe uses.
# Two concrete reasons, not just consistency:
#   1. tools/probes/build_probe_inventory.py's merge step requires each OUT
#      entry to be a dict with a "kind"; the old probe emitted
#      OUT = json.dumps(findings, ...) (a single string), so views output
#      was silently counted as an "unrecognized_entry" and never made it
#      into PROBE_INVENTORY.csv/md.
#   2. View IS an Element -- it has real BuiltInParameters/shared
#      parameters, but the old probe never walked them; it only hand-coded
#      two BIPs (VIEW_PHASE, VIEW_PHASE_FILTER) plus a fixed CLR-property
#      list. Running the same param_inventory walk every other domain
#      uses picks up every parameter actually present on sampled views
#      (Phase, Phase Filter, and anything else) without having to name
#      each one in advance -- plus the reflection sweep for everything
#      that isn't a Parameter at all (Discipline, DetailLevel, Scale,
#      ViewTemplateId, IsCallout, ...).
#
# The known API constraint that non-graphical view subtypes (schedules,
# sheets, legends, System Browser, "Internal"/ProjectBrowser views) throw
# on CLR properties like Discipline/CropBoxActive/IsSectionBoxActive is
# preserved -- not by special-casing those view types, but simply by
# letting the reflection sweep's per-member ok_count/error_count surface
# it, same as it does for every other domain.
#
# Inputs:
#   IN[0] max_views_per_viewtype (int)
#        Cap on how many View instances to sample PER distinct ViewType,
#        for the reflection/inventory/crosswalk sweep. Full-corpus counts
#        (by_viewtype_int, template_count, etc.) are still computed over
#        every view regardless of this cap.
#        Default: 5
#
#   IN[1] max_views_total (int)
#        Overall cap on the combined per-viewtype sample, in case a file
#        has many distinct ViewTypes.
#        Default: 300
#
#   IN[2] enable_crosswalk (bool)
#        Whether to emit View -> ViewTemplate crosswalk (has_template,
#        template_name) for the sampled views. Cheap (ElementId lookups on
#        already-sampled views), default on.
#        Default: True
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

try:
    from Autodesk.Revit.DB import ViewSchedule
except:
    ViewSchedule = None

try:
    from Autodesk.Revit.DB import ViewSheet
except:
    ViewSheet = None

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_views_per_viewtype = IN[0] if len(IN) > 0 and IN[0] is not None else 5
max_views_total = IN[1] if len(IN) > 1 and IN[1] is not None else 300
enable_crosswalk = IN[2] if len(IN) > 2 and IN[2] is not None else True
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None


# -------------------------
# Helpers (defensive) -- same param-contract engine every other domain
# probe with real Parameters uses (copied verbatim from probe_phase_filters.py
# so this file stays self-contained).
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default


def _safe_elem_name(elem):
    try:
        n = elem.Name
        if n:
            return n
    except:
        pass
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
            ref_name = _safe(lambda: _safe_elem_name(ref), None)

        return {
            "q": "ok", "storage": "ElementId", "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}


def _pv(q, storage, raw, display=None, norm=None):
    # Coerce anything that isn't already a JSON-native type (None/bool/int/
    # float/str) to a string before it can reach json.dump(). Fixes a real
    # 2026-08-04 failure: workset.unique_id passed ws.UniqueId straight
    # through as `raw` (Workset.UniqueId is System.Guid, not System.String,
    # unlike Element.UniqueId) and json.dump() threw "TypeError: Object of
    # type Guid is not JSON serializable" partway through writing the
    # combined file -- silently corrupting it at the real output path
    # under the old non-atomic writer. Every _pv() caller across these
    # probes funnels through here, so this is the one place that needs to
    # guard against a future call site making the same mistake.
    def _coerce(v):
        if v is None or isinstance(v, (bool, int, float, str)):
            return v
        try:
            return str(v)
        except:
            return None
    raw = _coerce(raw)
    disp = _coerce(display) if display is not None else raw
    nrm = _coerce(norm) if norm is not None else raw
    return {"q": q, "storage": storage, "raw": raw, "display": disp, "norm": nrm}


def _int_enum(val):
    try:
        return int(str(val))
    except:
        return None


def _view_kind_classification(v):
    """is_sheet / is_schedule / is_legend / is_internal -- derived, not a
    raw Parameter or CLR property read, so it lives here rather than being
    left to the reflection sweep to (not) find."""
    is_sheet = bool(ViewSheet is not None and _safe(lambda: isinstance(v, ViewSheet), False))
    is_schedule = bool(ViewSchedule is not None and _safe(lambda: isinstance(v, ViewSchedule), False))
    vt_int = _safe(lambda: _int_enum(v.ViewType), None)
    # Non-graphical ViewType buckets (Legend, SystemBrowser, "Internal"
    # project-browser views, and schedules/sheets above) are the ones that
    # throw on Discipline/CropBoxActive/etc. -- the reflection sweep's
    # per-member error_count captures that directly, keyed by nothing more
    # than vt_int, so there's no need to hand-classify each one here too.
    return is_sheet, is_schedule, vt_int


# -------------------------
# Discovery: sample views across every distinct ViewType found (non-template).
# -------------------------

all_views = _safe(lambda: list(FilteredElementCollector(doc).OfClass(View)), [])

counts_by_viewtype = {}
template_count = 0
live_count = 0

buckets = {}  # vt_key(str) -> [View, ...] (up to max_views_per_viewtype)

for v in all_views:
    is_template = _safe(lambda: bool(v.IsTemplate), False)
    if is_template:
        template_count += 1
        continue
    live_count += 1

    vt_int = _safe(lambda: _int_enum(v.ViewType), None)
    vt_key = str(vt_int) if vt_int is not None else "unknown"
    counts_by_viewtype[vt_key] = counts_by_viewtype.get(vt_key, 0) + 1

    bucket = buckets.setdefault(vt_key, [])
    try:
        cap = int(max_views_per_viewtype)
    except:
        cap = 5
    if len(bucket) < cap:
        bucket.append(v)

selected = []
for vt_key in sorted(buckets.keys()):
    selected.extend(buckets[vt_key])
try:
    total_cap = int(max_views_total)
except:
    total_cap = 300
selected = selected[:total_cap]

# -------------------------
# Inventory: real Parameter walk (BuiltInParameter + shared params) over
# sampled views, exactly like every other domain -- this is what picks up
# Phase / Phase Filter / Sheet Number / etc. without hand-listing BIPs.
# Synthesized rows are added alongside for the handful of View facts that
# are CLR properties, not Parameters, but that are still exporter-modeled
# derived facts rather than raw reflection targets (has_template /
# template_name via crosswalk covers that half; classification stays here).
# -------------------------

param_index = {}


def _maybe_set_example(entry, pv):
    if pv is None:
        return
    ex = entry.get("example")
    if ex is None:
        entry["example"] = pv
        return
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = pv


def _add_inventory_obs(param_key, pv, bucket=None):
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set(),
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
    _maybe_set_example(entry, pv)


for v in selected:
    vt_int = _safe(lambda: _int_enum(v.ViewType), None)
    bucket = str(vt_int) if vt_int is not None else "unknown"

    # --- Real Parameters (union over sampled views) ---
    params = _safe(lambda: list(v.GetOrderedParameters()), None)
    if params is None:
        params = _safe(lambda: list(v.Parameters), [])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pv = _format_param_contract(p)
        _add_inventory_obs("param.{}".format(dn), pv, bucket=bucket)

    # --- Synthesized, exporter-modeled surfaces not covered by Parameters ---
    is_sheet, is_schedule, _vt = _view_kind_classification(v)

    _add_inventory_obs("view.is_sheet", _pv("ok", "Integer", int(is_sheet), display=str(is_sheet)), bucket)
    _add_inventory_obs("view.is_schedule", _pv("ok", "Integer", int(is_schedule), display=str(is_schedule)), bucket)

    is_callout = _safe(lambda: bool(v.IsCallout), None)
    _add_inventory_obs(
        "view.is_callout",
        _pv("ok" if is_callout is not None else "unreadable", "Integer",
            int(is_callout) if is_callout is not None else None, display=str(is_callout)),
        bucket,
    )

    pv_id = _safe(lambda: v.GetPrimaryViewId(), None)
    is_dependent = None
    if pv_id is not None:
        is_dependent = bool(_safe(lambda: pv_id.IntegerValue, -1) != -1)
    _add_inventory_obs(
        "view.is_dependent",
        _pv("ok" if is_dependent is not None else "unreadable", "Integer",
            int(is_dependent) if is_dependent is not None else None, display=str(is_dependent)),
        bucket,
    )

    if is_schedule and ViewSchedule is not None:
        tb = _safe(lambda: bool(v.IsTitleblockRevisionSchedule), None)
        _add_inventory_obs(
            "view.is_titleblock_revision_schedule",
            _pv("ok" if tb is not None else "unreadable", "Integer",
                int(tb) if tb is not None else None, display=str(tb)),
            bucket,
        )

    if is_sheet and ViewSheet is not None:
        sn = _safe(lambda: getattr(v, "SheetNumber", None), None)
        _add_inventory_obs("view.sheet_number", _pv("ok" if sn else "missing", "String", sn), bucket)

# Doc-level counts, folded in as single-bucket synthesized rows.
_add_inventory_obs("doc.view_template_count", _pv("ok", "Integer", template_count), "doc")
_add_inventory_obs("doc.view_live_count", _pv("ok", "Integer", live_count), "doc")
_add_inventory_obs("doc.view_total_count", _pv("ok", "Integer", len(all_views)), "doc")
for vt_key in sorted(counts_by_viewtype.keys()):
    _add_inventory_obs("viewtype.{}.collector_count".format(vt_key), _pv("ok", "Integer", counts_by_viewtype[vt_key]), "doc")

param_inventory = []
for k in sorted(param_index.keys()):
    e = param_index[k]
    param_inventory.append({
        "domain": "views",
        "param_key": k,
        "example": e["example"],
        "observed": {
            "storage_types": sorted(e["storage_types"]),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(e["observed_on_buckets"]),
        },
        "selected_views_sample_count": len(selected),
    })

# -------------------------
# Crosswalk: View -> ViewTemplate (has_template / template_id / template_name),
# one row per sampled view. Cheap -- ElementId lookups on views already in
# `selected`, not an additional collector pass.
# -------------------------

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


optional_crosswalk = []
if enable_crosswalk:
    for v in selected:
        vt_int = _safe(lambda: _int_enum(v.ViewType), None)
        tmpl_id = _safe(lambda: v.ViewTemplateId, None)
        tmpl_int = _safe(lambda: tmpl_id.IntegerValue, None)
        has_template = bool(tmpl_int is not None and int(tmpl_int) != -1)
        template_name = None
        if has_template:
            tmpl_elem = _safe(lambda: doc.GetElement(tmpl_id), None)
            if tmpl_elem is not None:
                template_name = _safe(lambda: _safe_elem_name(tmpl_elem), None)

        ws_id_obj = _safe(lambda: v.WorksetId, None)
        ws_name, ws_resolved = _resolve_workset(doc, ws_id_obj)
        ws_id_int = _safe(lambda: ws_id_obj.IntegerValue, None) if ws_id_obj is not None else None

        # Schedule-only: BodyTextTypeId/HeaderTextTypeId/TitleTextTypeId are
        # real ViewSchedule properties (confirmed against Autodesk's own API
        # docs -- "Defines the default text style used for the .../.../...
        # section of the schedule"), referencing a TextNoteType. Unlike
        # WorksetId, doc.GetElement() IS the right resolution call here --
        # no trap this time, these are genuine ElementIds. None on
        # non-schedule views (the properties don't exist there at all).
        body_tt_id = body_tt_name = None
        header_tt_id = header_tt_name = None
        title_tt_id = title_tt_name = None
        if ViewSchedule is not None and _safe(lambda: isinstance(v, ViewSchedule), False):
            _btt = _safe(lambda: v.BodyTextTypeId, None)
            body_tt_id = _safe(lambda: _btt.IntegerValue, None) if _btt is not None else None
            if body_tt_id is not None and body_tt_id >= 0:
                _btt_elem = _safe(lambda: doc.GetElement(_btt), None)
                body_tt_name = _safe(lambda: _btt_elem.Name, None) if _btt_elem is not None else None

            _htt = _safe(lambda: v.HeaderTextTypeId, None)
            header_tt_id = _safe(lambda: _htt.IntegerValue, None) if _htt is not None else None
            if header_tt_id is not None and header_tt_id >= 0:
                _htt_elem = _safe(lambda: doc.GetElement(_htt), None)
                header_tt_name = _safe(lambda: _htt_elem.Name, None) if _htt_elem is not None else None

            _ttt = _safe(lambda: v.TitleTextTypeId, None)
            title_tt_id = _safe(lambda: _ttt.IntegerValue, None) if _ttt is not None else None
            if title_tt_id is not None and title_tt_id >= 0:
                _ttt_elem = _safe(lambda: doc.GetElement(_ttt), None)
                title_tt_name = _safe(lambda: _ttt_elem.Name, None) if _ttt_elem is not None else None

        optional_crosswalk.append({
            "view.id": _safe(lambda: v.Id.IntegerValue, None),
            "view.name": _safe(lambda: v.Name, None),
            "view.viewtype_int": vt_int,
            "view.has_template": has_template,
            "view.template_id": int(tmpl_int) if (tmpl_int is not None and int(tmpl_int) != -1) else None,
            "view.template_name": template_name,
            "view.workset_id": ws_id_int,
            "view.workset_name": ws_name,
            "schedule.body_text_type_id": body_tt_id,
            "schedule.body_text_type_name": body_tt_name,
            "schedule.header_text_type_id": header_tt_id,
            "schedule.header_text_type_name": header_tt_name,
            "schedule.title_text_type_id": title_tt_id,
            "schedule.title_text_type_name": title_tt_name,
        })

# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection.
# Identical engine to every other domain probe (copied verbatim, not
# imported -- each probe_*.py must remain a self-contained, paste-able
# Dynamo Python node). This is what surfaces Discipline / DetailLevel /
# Scale / CropBoxActive / etc. (and anything not yet known to matter)
# without hand-coding each one, and its per-member error_count is exactly
# what shows the "View must have a Discipline property"-style exceptions
# on non-graphical view subtypes -- expected API behavior, not a probe bug.
# -------------------------

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
            # on the allowlist above (see module-level design rationale in
            # every other probe_*.py).
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


def _reflect_collection_is_elementid_typed(raw_v):
    # Best-effort check of the collection's declared/generic item type (not
    # its contents) via CLR reflection -- the only way to confirm an EMPTY
    # collection actually holds ElementIds, since there are no items to
    # duck-type against. Checks the object's own generic type arguments
    # (e.g. List<ElementId>) and any implemented generic interface (e.g. a
    # concrete type implementing IList<ElementId> without itself being
    # generic). Returns False (not "unknown") on any reflection failure --
    # this function only ever widens confidence, never narrows it.
    try:
        t = raw_v.GetType()
    except:
        return False
    type_candidates = []
    try:
        if t.IsGenericType:
            type_candidates.append(t)
    except:
        pass
    try:
        for iface in t.GetInterfaces():
            if getattr(iface, "IsGenericType", False):
                type_candidates.append(iface)
    except:
        pass
    for tc in type_candidates:
        try:
            for arg in tc.GetGenericArguments():
                if getattr(arg, "FullName", None) == "Autodesk.Revit.DB.ElementId":
                    return True
        except:
            continue
    return False

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
        if not saw_item and not _reflect_collection_is_elementid_typed(raw_v):
            # Empty collection whose item type we can't confirm is ElementId
            # (e.g. Element.GetEntitySchemaGuids() -> IList<Guid> with 0
            # schemas attached) -- iterating an empty collection never runs
            # the per-item .IntegerValue check above, so an unconfirmed
            # empty collection must not be classified as an ElementId list.
            raise TypeError("empty collection of unconfirmed item type")
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


_reflection_records = _run_reflection_sweep(selected, "View", "views")

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "views",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "views",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "views",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
file_written = None
write_error = None

# -------------------------
# Unified run metadata (release-separated, not date-filename-separated)
# -------------------------

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

        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("views", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

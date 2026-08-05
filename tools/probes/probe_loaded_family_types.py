# Dynamo Python (Revit) — Breadth Probe: loaded_family_types (DISCOVERY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "loaded_family_types",
#     "records": records,
#     "summary": summary,
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "reflection",
#     "domain": "loaded_family_types",
#     "records": [...]
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "loaded_family_types",
#     "records": [...]   # FamilySymbol -> its own workset (new -- this domain had no
#                         # crosswalk kind at all before; reuses the existing 60-item
#                         # capped reflection sample, no extra collector cost)
#   }
# ]
#
# Inputs:
#   IN[0] max_families_to_inspect (int) default 500, -1 = no cap
#   IN[1] max_types_per_family (int) default -1, -1 = no cap
#   IN[2] include_empty_values (bool) default True
#   IN[3] write_json (bool) default False
#   IN[4] output_directory (str) optional
#
# Notes:
#  - Read-only project-level discovery.
#  - Does NOT open/edit family documents.
#  - Emits more evidence than future hashes should use.
#  - Intended for gate / join-key / parameter availability analysis.

import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector,
    Family,
    FamilySymbol,
    ElementId,
    StorageType,
    UnitUtils,
    UnitFormatUtils,
    BuiltInParameter
)

try:
    from Autodesk.Revit.DB import SpecTypeId, UnitTypeId
except:
    SpecTypeId = None
    UnitTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_families_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
max_types_per_family = IN[1] if len(IN) > 1 and IN[1] is not None else -1
include_empty_values = IN[2] if len(IN) > 2 and IN[2] is not None else True
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
out_dir = IN[4] if len(IN) > 4 and IN[4] is not None else None


# -------------------------
# Helpers
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _id_int(eid):
    try:
        return eid.IntegerValue
    except:
        return None

def _element_name(e):
    if e is None:
        return None
    n = _safe(lambda: e.Name, None)
    if n:
        return n
    for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
        try:
            p = e.get_Parameter(bip)
            if p:
                s = p.AsString()
                if s:
                    return s
        except:
            pass
    return None

def _cat_info(elem):
    cat = _safe(lambda: elem.Category, None)
    if cat is None:
        return {"category.id": None, "category.name": None, "category.type": None}
    return {
        "category.id": _safe(lambda: cat.Id.IntegerValue, None),
        "category.name": _safe(lambda: cat.Name, None),
        "category.type": _safe(lambda: str(cat.CategoryType), None)
    }

def _param_definition_identity(p):
    d = _safe(lambda: p.Definition, None)
    if d is None:
        return {}

    # BuiltInParameter is often available as integer via p.Id for built-ins.
    pid = _safe(lambda: p.Id.IntegerValue, None)

    guid = None
    try:
        guid = str(p.GUID)
    except:
        guid = None

    data_type = None
    try:
        dt = d.GetDataType()
        data_type = str(dt.TypeId) if hasattr(dt, "TypeId") else str(dt)
    except:
        data_type = None

    return {
        "param.name": _safe(lambda: d.Name, None),
        "param.id": pid,
        "param.guid": guid,
        "param.data_type": data_type,
        "param.parameter_group": _safe(lambda: str(d.ParameterGroup), None),
        "param.is_read_only": _safe(lambda: p.IsReadOnly, None),
        "param.is_shared": True if guid else False
    }

def _format_double(p, raw):
    try:
        d = p.Definition
        dt = d.GetDataType()
        return UnitFormatUtils.Format(doc.GetUnits(), dt, raw, False)
    except:
        return _safe(lambda: p.AsValueString(), None)

def _normalize_double(p, raw):
    # Keep internal value plus display; only add common normalized hints when possible.
    if raw is None:
        return None

    if SpecTypeId is None or UnitTypeId is None:
        return raw

    dt = _safe(lambda: p.Definition.GetDataType(), None)

    try:
        if dt == SpecTypeId.Length:
            return {
                "internal": raw,
                "inches": UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches)
            }
    except:
        pass

    try:
        if dt == SpecTypeId.Angle:
            return {
                "internal": raw,
                "degrees": UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees)
            }
    except:
        pass

    return raw

def _param_value_contract(p):
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
        disp = _safe(lambda: p.AsValueString(), None)
        return {
            "q": "ok",
            "storage": "Integer",
            "raw": raw,
            "display": disp if disp is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    if st == StorageType.Double:
        raw = _safe(lambda: p.AsDouble(), None)
        return {
            "q": "ok",
            "storage": "Double",
            "raw": raw,
            "display": _format_double(p, raw),
            "norm": _normalize_double(p, raw)
        }

    if st == StorageType.ElementId:
        eid = _safe(lambda: p.AsElementId(), None)
        if eid is None or eid == ElementId.InvalidElementId:
            return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}

        ref = _safe(lambda: doc.GetElement(eid), None)
        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": _id_int(eid),
            "display": _element_name(ref),
            "norm": _id_int(eid)
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _parameters_for_element(elem, source):
    rows = []
    params = _safe(lambda: list(elem.GetOrderedParameters()), None)
    if params is None:
        params = _safe(lambda: list(elem.Parameters), [])

    for p in params:
        ident = _param_definition_identity(p)
        val = _param_value_contract(p)

        has_value = val.get("raw") is not None or val.get("display") is not None or val.get("norm") is not None
        if not include_empty_values and not has_value:
            continue

        row = {}
        row.update(ident)
        row.update({
            "param.source": source,
            "param.has_value": has_value,
            "value": val
        })
        rows.append(row)

    rows.sort(key=lambda r: (
        str(r.get("param.source")),
        str(r.get("param.name")),
        str(r.get("param.id")),
        str(r.get("param.guid"))
    ))
    return rows

def _family_symbols(fam):
    ids = _safe(lambda: list(fam.GetFamilySymbolIds()), [])
    syms = []
    for eid in ids:
        s = _safe(lambda eid=eid: doc.GetElement(eid), None)
        if s is not None:
            syms.append(s)
    syms.sort(key=lambda s: str(_element_name(s)))
    return syms

def _family_record(fam, sym):
    fam_cat = _cat_info(fam)
    sym_cat = _cat_info(sym)

    rec = {
        "domain": "loaded_family_types",
        "governance_status": "observed_only",

        "project": {
            "title": _safe(lambda: doc.Title, None),
            "path": _safe(lambda: doc.PathName, None)
        },

        "family": {
            "id": _safe(lambda: fam.Id.IntegerValue, None),
            "unique_id": _safe(lambda: fam.UniqueId, None),
            "name": _safe(lambda: fam.Name, None),
            "is_editable": _safe(lambda: fam.IsEditable, None),
            "is_in_place": _safe(lambda: fam.IsInPlace, None),
            "symbol_count": _safe(lambda: len(list(fam.GetFamilySymbolIds())), None),
            "category": fam_cat
        },

        "type": {
            "id": _safe(lambda: sym.Id.IntegerValue, None),
            "unique_id": _safe(lambda: sym.UniqueId, None),
            "name": _element_name(sym),
            "category": sym_cat
        },

        # These are deliberately separated for later hash/join/gate analysis.
        "identity_items": {
            "family.name": _safe(lambda: fam.Name, None),
            "family.category.name": fam_cat.get("category.name"),
            "type.name": _element_name(sym),
            "type.category.name": sym_cat.get("category.name")
        },

        "join_items": {
            "family_to_type": {
                "left.family_id": _safe(lambda: fam.Id.IntegerValue, None),
                "left.family_name": _safe(lambda: fam.Name, None),
                "right.type_id": _safe(lambda: sym.Id.IntegerValue, None),
                "right.type_name": _element_name(sym)
            }
        },

        "validation_items": {
            "family_parameters": _parameters_for_element(fam, "family"),
            "type_parameters": _parameters_for_element(sym, "type")
        },

        "debug_items": {
            "family_class": _safe(lambda: fam.GetType().FullName, None),
            "type_class": _safe(lambda: sym.GetType().FullName, None)
        }
    }
    return rec


# -------------------------
# Collect loaded families
# -------------------------

families = _safe(lambda: list(FilteredElementCollector(doc).OfClass(Family).ToElements()), [])
families.sort(key=lambda f: str(_safe(lambda: f.Name, "")))

try:
    max_f = int(max_families_to_inspect)
    if max_f >= 0:
        families = families[:max_f]
except:
    pass

records = []
category_counts = {}
family_count = 0
type_count = 0
param_name_counts = {}

_reflect_samples = []

for fam in families:
    family_count += 1
    syms = _family_symbols(fam)

    try:
        max_t = int(max_types_per_family)
        if max_t >= 0:
            syms = syms[:max_t]
    except:
        pass

    for sym in syms:
        type_count += 1
        rec = _family_record(fam, sym)
        records.append(rec)

        if len(_reflect_samples) < 60:
            _reflect_samples.append(sym)

        cat_name = rec["type"]["category"].get("category.name") or rec["family"]["category"].get("category.name") or "<None>"
        category_counts[cat_name] = category_counts.get(cat_name, 0) + 1

        for p in rec["validation_items"]["type_parameters"]:
            pn = p.get("param.name") or "<None>"
            param_name_counts[pn] = param_name_counts.get(pn, 0) + 1


summary = {
    "family_count_inspected": family_count,
    "family_type_record_count": len(records),
    "category_counts": category_counts,
    "distinct_type_parameter_names": len(param_name_counts),
    "top_type_parameter_names": sorted(
        [{"param.name": k, "count": v} for k, v in param_name_counts.items()],
        key=lambda x: (-x["count"], x["param.name"])
    )[:100]
}



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
# docs/method_invocation_candidates_annotated.csv): these 34 method names (35
# (declaring_class, method) pairs from the Step 0 CSV -- GetValidTypes is
# declared on both Element and Subelement, independently confirmed zero-arg/
# instance/non-mutating on each) are ground-truth confirmed, against the live
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
    "GetValidTypes": "Element, Subelement",
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

_reflection_records_0 = _run_reflection_sweep(_reflect_samples, "FamilySymbol", "loaded_family_types")
_reflection_records = _reflection_records_0

# -------------------------
# Crosswalk (new): FamilySymbol -> its own workset. loaded_family_types had
# no crosswalk kind at all before this -- reuses the same 60-item capped
# sample the reflection sweep already collected (_reflect_samples), so this
# adds no extra collector cost.
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
for sym in _reflect_samples:
    sym_id = _safe(lambda: sym.Id.IntegerValue, None)
    if sym_id is None:
        continue
    sym_name = _safe(lambda: sym.Name, None)
    ws_id_obj = _safe(lambda: sym.WorksetId, None)
    ws_name, _ws_resolved = _resolve_workset(doc, ws_id_obj)
    ws_id_int = _safe(lambda: ws_id_obj.IntegerValue, None) if ws_id_obj is not None else None
    optional_crosswalk.append({
        "family_symbol.id": sym_id,
        "family_symbol.name": sym_name,
        "family_symbol.workset_id": ws_id_int,
        "family_symbol.workset_name": ws_name,
    })

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "loaded_family_types",
        "records": records,
        "summary": summary
    },
    {
        "kind": "reflection",
        "domain": "loaded_family_types",
        "records": _reflection_records
    },
    {
        "kind": "crosswalk",
        "domain": "loaded_family_types",
        "records": optional_crosswalk
    },
]


# -------------------------
# Optional JSON write
# -------------------------

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
            default_dir = os.path.dirname(rvt_path)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        target_dir = out_dir if out_dir else default_dir
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
        target_path = os.path.join(target_dir, fixed_name)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("loaded_family_types", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload
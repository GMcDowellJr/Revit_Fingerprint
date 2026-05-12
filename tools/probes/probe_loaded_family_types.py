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


OUT_payload = [
    {
        "kind": "inventory",
        "domain": "loaded_family_types",
        "records": records,
        "summary": summary
    }
]


# -------------------------
# Optional JSON write
# -------------------------

file_written = None
write_error = None

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

        fixed_name = "probe_loaded_family_types_{}.json".format(datetime.now().strftime("%Y-%m-%d"))
        target_path = os.path.join(target_dir, fixed_name)

        with open(target_path, "w") as f:
            json.dump(OUT_payload, f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload
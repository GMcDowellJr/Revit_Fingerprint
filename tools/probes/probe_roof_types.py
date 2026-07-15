# Dynamo Python (Revit) -- Full-Pull Probe: roof_types (INVENTORY + FULL RECORDS)
#
# OUT = [
#   {
#     "kind": "full_records",
#     "domain": "roof_types",
#     "records": [...]      # ONE ROW PER RoofType, ALL parameters + ALL computed fields.
#                            # No sampling, no bucketing -- every roof_types type in the file.
#   },
#   {
#     "kind": "inventory",
#     "domain": "roof_types",
#     "records": [...]      # aggregated breadth view (q_counts/example) over the SAME full set
#   },
#   { "file_written": "<path>|None", "file_write_error": "<error>|None" }  -- appended to full_records[0]
# ]
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
#        Filename fixed as: probe_roof_types_YYYY-MM-DD.json


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


def _build_full_record(t):
    """One record per roof_types type: ALL Revit parameters + ALL computed fields."""
    name = _safe(lambda: _safe_type_name(t), None)

    all_params = {}
    params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(t.Parameters), default=[])
    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        all_params["p.{}".format(dn)] = _format_param_contract(p)

    computed = {}
    computed["rt.type_name"] = _contract_from_value("ok", "String", name, name, name)


    cs = _safe(lambda: t.GetCompoundStructure(), None)
    has_cs = cs is not None
    computed["rt.has_compound_structure"] = _contract_from_value("ok", "Integer", int(has_cs), str(has_cs), int(has_cs))

    if has_cs:
        layers = _safe(lambda: list(cs.GetLayers()), [])
        computed["rt.layer_count"] = _contract_from_value("ok", "Integer", len(layers), str(len(layers)), len(layers))

        max_l = None
        try:
            max_l = int(max_layers_per_type)
        except:
            max_l = 30

        total_thickness_ft = 0.0
        thickness_unreadable = False
        layer_detail = []
        for i, layer in enumerate(layers):
            if max_l is not None and max_l >= 0 and i >= max_l:
                break
            w = _safe(lambda: layer.Width, None)
            if w is None:
                thickness_unreadable = True
            else:
                total_thickness_ft += w
            fnv = _safe(lambda: layer.Function, None)
            fn_r2 = _multi_repr(fnv)
            mat_id = _safe(lambda: layer.MaterialId, None)
            mat_id_int = _safe(lambda: mat_id.IntegerValue, None) if mat_id is not None else None
            layer_detail.append({
                "layer_index": i,
                "function.str()": fn_r2.get("str()"),
                "width_ft": w,
                "width_in": _to_inches(w) if w is not None else None,
                "material_id": mat_id_int,
                "is_structural_material": _safe(lambda: bool(layer.IsStructuralMaterial), None),
                "is_variable_width": _safe(lambda: bool(layer.IsVariableWidth), None),
            })

        tt_in = None if thickness_unreadable else _to_inches(total_thickness_ft)
        computed["rt.total_thickness_in"] = _contract_from_value(
            "unreadable" if thickness_unreadable else "ok", "Double", tt_in, tt_in, tt_in)
        computed["rt.layer_detail"] = layer_detail

        sweeps = _safe(lambda: list(cs.GetWallSweepsInfo()), [])
        sweeps_present = (len(sweeps) > 0)
        computed["rt.has_embedded_sweeps"] = _contract_from_value("ok", "Integer", int(sweeps_present), str(sweeps_present), int(sweeps_present))
    else:
        computed["rt.layer_count"] = _contract_from_value("unsupported.not_applicable", "Integer", None, None, None)
        computed["rt.total_thickness_in"] = _contract_from_value("unsupported.not_applicable", "Double", None, None, None)
        computed["rt.layer_detail"] = []
        computed["rt.has_embedded_sweeps"] = _contract_from_value("unsupported.not_applicable", "Integer", None, None, None)

    fill_p = _safe(lambda: t.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_PATTERN_ID_FOR_LEGEND), None)
    computed["rt.coarse_fill_pattern"] = _format_param_contract(fill_p) if fill_p is not None else _contract_from_value("missing", "None", None, None, None)
    color_p = _safe(lambda: t.get_Parameter(BuiltInParameter.COARSE_SCALE_FILL_COLOR), None)
    computed["rt.coarse_fill_color"] = _format_param_contract(color_p) if color_p is not None else _contract_from_value("missing", "None", None, None, None)

    return {
        "roof_types.name": name,
        "roof_types.id": _safe(lambda: t.Id.IntegerValue, None),
        "params": all_params,
        "computed": computed,
    }


full_records = []
for t in all_types:
    full_records.append(_build_full_record(t))


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

for rec in full_records:
    for pk, pv in rec["params"].items():
        _observe(pk, pv)
    for ck, cv in rec["computed"].items():
        if ck in ("rt.layer_detail",):
            continue
        _observe(ck, cv)

param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "roof_types",
        "param_key": pk,
        "total_records": len(full_records),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
        }
    })


OUT_payload = [
    {
        "kind": "full_records",
        "domain": "roof_types",
        "records": full_records
    },
    {
        "kind": "inventory",
        "domain": "roof_types",
        "records": param_inventory
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

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_roof_types_{}.json".format(date_stamp)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(OUT_payload, f, indent=2, sort_keys=True)

        file_written = target_path
    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

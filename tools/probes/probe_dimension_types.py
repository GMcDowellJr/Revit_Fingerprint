# Dynamo Python (Revit) — Breadth Probe: dimension_types (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "dimension_types",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "dimension_types",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_dim_types_to_inspect (int)
#        Maximum number of DimensionType ElementTypes to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit DimensionType → Tick Mark (Arrowhead) crosswalk.
#        Default: False
#
#   IN[2] per_shape_limit (int)
#        Sample at most N DimensionTypes per Shape value (StyleType/Shape),
#        to bias breadth over quantity.
#        Default: 8
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_dimension_types_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.


import clr
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

try:
    from Autodesk.Revit.DB import DimensionType
except:
    DimensionType = None

doc = DocumentManager.Instance.CurrentDBDocument

max_dim_types_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_shape_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 8
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

def _get_family_name_param(dim_type):
    """
    Read the family-name parameter using the same lookup path as the extractor.
    Returns the raw string value or None when the parameter is absent, unreadable,
    unset, or only whitespace.
    """
    def _normalize_param_string(p):
        if p is None:
            return None
        try:
            if not p.HasValue:
                return None
        except Exception:
            return None
        try:
            v = p.AsString()
        except Exception:
            return None
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    if dim_type is None:
        return None
    # Try BIP first
    try:
        v = _normalize_param_string(dim_type.get_Parameter(BuiltInParameter.SYMBOL_FAMILY_NAME_PARAM))
        if v is not None:
            return v
    except Exception:
        pass
    # Try LookupParameter as last resort
    try:
        v = _normalize_param_string(dim_type.LookupParameter("Family Name"))
        if v is not None:
            return v
    except Exception:
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
    """
    if p is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}

    st = _safe(lambda: p.StorageType, None)
    if st is None:
        return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}

    # Wrapper-safe check for StorageType.None (Revit enum value == 0)
    is_none_storage = False
    try:
        is_none_storage = (int(st) == 0)
    except:
        try:
            is_none_storage = (str(st) in ("None", "None_", "0"))
        except:
            is_none_storage = False

    if is_none_storage:
        # StorageType.None => no primitive backing store. Often a formatting/spec object.
        disp = _safe(lambda: p.AsValueString(), None)

        # Some None-storage params can still provide a display string; capture it if present.
        if disp is not None and str(disp).strip() != "":
            return {"q": "ok", "storage": "None", "raw": None, "display": disp, "norm": disp}

        # If no meaningful display string exists, treat as unsupported for join/semantic use.
        return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}

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
            if ref_name is None:
                ref_name = _safe(lambda: _safe_type_name(ref), None)

        display = ref_name if ref_name is not None else (str(raw) if raw is not None else None)

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": display,
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

# -------------------------
# Dimension shape key (breadth buckets)
# -------------------------

def _shape_family_from_label(label):
    if not label:
        return "unknown"

    s = str(label).lower()

    # coarse but useful buckets for governance discussion
    if "linear" in s or "aligned" in s:
        return "linear"
    if "angular" in s or "angle" in s:
        return "angular"
    if "radial" in s or "radius" in s:
        return "radial"
    if "diameter" in s:
        return "diameter"
    if "arc" in s:
        return "arc"
    if "spot" in s:
        return "spot"
    if "ordinate" in s:
        return "ordinate"

    return "other"


def _get_dim_shape_info(dt):
    """
    Returns (shape_key, shape_label, shape_family, q)

    Goal: label-based bucketing for breadth, not identity.
    Prefer:
      1) Enum name via System.Enum.GetName(type, value)
      2) v.ToString()
      3) A parameter whose display string looks like a dimension style/shape name
    """
    if dt is None:
        return ("missing", None, "unknown", "missing")

    # local helper: turn (label, raw_int) into tuple
    def _pack(label, raw_int, q):
        fam = _shape_family_from_label(label)
        sk = "{}|{}".format(label if label else "unknown", raw_int if raw_int is not None else "na")
        return (sk, label, fam, q)

    # Try properties first
    for attr in ("StyleType", "Shape", "DimensionShape", "DimensionStyleType"):
        try:
            if not hasattr(dt, attr):
                continue

            v = getattr(dt, attr, None)
            if v is None:
                continue

            raw_int = None
            try:
                raw_int = int(v)
            except:
                raw_int = None

            label = None

            # Best: resolve enum name using the DECLARED property type (reflection),
            # not v.GetType() (which may be int/boxed in some bindings).
            try:
                import System

                dt_type = dt.GetType()
                prop = dt_type.GetProperty(attr)
                if prop is not None:
                    prop_type = prop.PropertyType  # should be an enum type when applicable
                    try:
                        if prop_type is not None and prop_type.IsEnum:
                            nm = System.Enum.GetName(prop_type, v)
                            if nm:
                                label = nm
                    except:
                        pass
            except:
                pass

            # Next: ToString()
            if not label:
                try:
                    label = v.ToString()
                except:
                    label = None

            # If we got anything usable, return it.
            if label or raw_int is not None:
                return _pack(label, raw_int, "ok")

        except:
            continue

    # Fallback: scan parameters for something that *looks* like style/shape name
    params = _safe(lambda: list(dt.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(dt.Parameters), default=[])

    best_disp = None
    best_raw = None
    best_q = "missing"

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue

        dn_l = dn.lower()

        # Heuristic: candidates likely to carry shape/style names
        if ("style" not in dn_l) and ("shape" not in dn_l) and ("dimension" not in dn_l):
            continue

        pv = _format_param_contract(p)
        disp = pv.get("display")
        raw = pv.get("raw")

        if pv.get("q") != "ok":
            continue

        # Prefer a display string that isn't purely numeric
        if disp and not str(disp).strip().lstrip("-").isdigit():
            best_disp = disp
            best_raw = raw
            best_q = "ok"
            break

    if best_disp is not None or best_raw is not None:
        try:
            raw_int = int(best_raw) if best_raw is not None else None
        except:
            raw_int = None
        return _pack(best_disp, raw_int, best_q)

    return ("missing", None, "unknown", "missing")

# -------------------------
# Progressive Discovery
# -------------------------

# Step 1 (preferred): class-based, category-free collector for DimensionType
dim_types = []
if DimensionType is not None:
    dim_types = _safe(
        lambda: (FilteredElementCollector(doc)
                 .WhereElementIsElementType()
                 .OfClass(DimensionType)
                 .ToElements()),
        default=[]
    )

try:
    dim_types = list(dim_types)
except:
    dim_types = list(dim_types) if dim_types is not None else []


# Step 2 (fallback): parameter-signature discovery across ElementType
DIM_TYPE_SIGNATURE_PARAMS = [
    # tick/arrowhead is a common anchor across many dimension shapes
    "Tick Mark",
    "Tick mark",
    "Tick Mark Type",
    "Tick Mark Symbol",
    # text appearance is also common
    "Text Size",
    "Text Font",
    "Text",
    # witness/line properties often exist on linear styles
    "Witness Line Control",
    "Dimension Line",
]

def _looks_like_dimension_type(t):
    # NOTE: this is ONLY used when class-based collection fails.
    if t is None:
        return False
    hits = 0
    try:
        for pn in DIM_TYPE_SIGNATURE_PARAMS:
            if t.LookupParameter(pn) is not None:
                hits += 1
        # Require multiple hits to avoid false positives
        return hits >= 3
    except:
        return False

discovery_notes = []

if len(dim_types) == 0:
    discovery_notes.append("fallback:param_signature:DimensionType collector returned 0")
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
        all_types = list(all_types) if all_types is not None else []

    dim_types = []
    for t in all_types:
        if _looks_like_dimension_type(t):
            dim_types.append(t)


# Cap AFTER filtering
try:
    max_n = int(max_dim_types_to_inspect)
    if max_n >= 0:
        dim_types = dim_types[:max_n]
except:
    pass


# -------------------------
# Sampling (breadth bias): first N per Shape
# -------------------------

selected = []
by_shape = {}  # shape_key -> count

for t in dim_types:
    shape_key, shape_label, shape_family, _ = _get_dim_shape_info(t)
    c = by_shape.get(shape_key, 0)

    if per_shape_limit is None:
        shape_ok = True
    else:
        try:
            shape_ok = c < int(per_shape_limit)
        except:
            shape_ok = c < 8

    if shape_ok:
        selected.append(t)
        by_shape[shape_key] = c + 1

# If per_shape_limit is 0 or negative, fallback to at least 1 per shape
if len(selected) == 0 and len(dim_types) > 0:
    seen = set()
    for t in dim_types:
        shape_key, shape_label, shape_family, _ = _get_dim_shape_info(t)
        if shape_key not in seen:
            selected.append(t)
            seen.add(shape_key)


# -------------------------
# Build inventory (union over selected)
# Dedup observations by (param_key, storage, norm)
# -------------------------

# -------------------------
# Synthetic inventory injection (format surface)
# -------------------------

param_index = {}

def _example_score(pv):
    if pv is None:
        return -1
    q = pv.get("q")
    if q == "ok":
        base = 100
    elif q == "missing":
        base = 10
    elif q == "unreadable":
        base = 5
    else:
        base = 0

    disp = pv.get("display")
    raw = pv.get("raw")
    norm = pv.get("norm")

    if disp is not None and str(disp).strip() != "":
        base += 20
    if norm is not None:
        base += 10
    if raw is not None:
        base += 5

    return base

def _maybe_set_example(entry, pv):
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

    cur_score = _example_score(ex)
    new_score = _example_score(pv)

    if new_score > cur_score:
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }
        
# -------------------------
# Synthetic extraction: Primary/Alternate units format options (probe-only)
# -------------------------

import hashlib

def _md5(s):
    try:
        h = hashlib.md5()
        h.update(s.encode("utf-8"))
        return h.hexdigest()
    except:
        return None

def _try_call(obj, member_name):
    if obj is None or not member_name:
        return (False, None, "missing_target_or_member")
    try:
        if hasattr(obj, member_name):
            v = getattr(obj, member_name)
            if callable(v):
                try:
                    return (True, v(), None)
                except Exception as ex:
                    return (False, None, "{}: {}".format(type(ex).__name__, ex))
            return (True, v, None)
    except Exception as ex:
        return (False, None, "{}: {}".format(type(ex).__name__, ex))
    return (False, None, "no_such_member")

def _kv_norm(k, v):
    if v is None:
        return (k, None)
    try:
        if hasattr(v, "IntegerValue"):
            return (k, int(v.IntegerValue))
        if isinstance(v, (bool, int, float, str)):
            return (k, v)
        if hasattr(v, "ToString"):
            s = v.ToString()
            if s and "Autodesk.Revit" not in s and "System." not in s:
                return (k, s)
        s2 = str(v)
        if s2 and "Autodesk.Revit" not in s2 and "System." not in s2:
            return (k, s2)
        return (k, None)
    except:
        return (k, None)

def _format_synth_contract(raw_v):
    if raw_v is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
    if isinstance(raw_v, str):
        return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
    if isinstance(raw_v, bool):
        return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
    if isinstance(raw_v, int):
        return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, float):
        return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    try:
        if hasattr(raw_v, "ToString"):
            s = raw_v.ToString()
            if s and "Autodesk.Revit" not in s and "System." not in s:
                return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
    except:
        pass
    return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}

def _reflect_members(obj, keywords):
    names = []
    if obj is None:
        return names
    try:
        t = obj.GetType()
        try:
            props = t.GetProperties()
            for p in props:
                try:
                    n = p.Name
                    nl = n.lower()
                    for kw in keywords:
                        if kw in nl:
                            names.append(n)
                            break
                except:
                    pass
        except:
            pass
        try:
            meths = t.GetMethods()
            for m in meths:
                try:
                    n = m.Name
                    nl = n.lower()
                    if m.GetParameters().Length != 0:
                        continue
                    for kw in keywords:
                        if kw in nl:
                            names.append(n)
                            break
                except:
                    pass
        except:
            pass
    except:
        pass
    return sorted(list(set(names)))

def _try_extract_format_surface(dim_type):
    out = {"found_members": [], "values": {}, "signatures": {"primary": None, "alternate": None}}
    if dim_type is None:
        return out

    primary_keywords = ["primary", "unit", "format", "round", "symbol", "suppress", "digits", "accuracy"]
    alt_keywords = ["alternate", "alt", "unit", "format", "round", "symbol", "suppress", "digits", "accuracy"]

    root_candidates = [
        "PrimaryUnits", "PrimaryUnit", "PrimaryFormatOptions", "PrimaryFormat",
        "AlternateUnits", "AlternateUnit", "AlternateFormatOptions", "AlternateFormat",
        "GetPrimaryUnits", "GetAlternateUnits", "GetPrimaryFormatOptions", "GetAlternateFormatOptions"
    ]

    roots = []
    for rc in root_candidates:
        ok, v, err = _try_call(dim_type, rc)
        if ok and v is not None:
            roots.append((rc, v))

    if len(roots) == 0:
        out["found_members"] = _reflect_members(dim_type, ["alternate", "alt", "primary", "format", "unit"])
        for n in out["found_members"][:60]:
            ok, v, err = _try_call(dim_type, n)
            if ok:
                key = "x.dim_type.{}".format(n)
                out["values"][key] = _format_synth_contract(_kv_norm(n, v)[1])
        return out

    primary_kvs = []
    alt_kvs = []

    for (root_name, root_obj) in roots:
        is_alt = ("alt" in root_name.lower()) or ("alternate" in root_name.lower())
        leaf_names = _reflect_members(root_obj, alt_keywords if is_alt else primary_keywords)

        for ln in leaf_names:
            out["found_members"].append("{}::{}".format(root_name, ln))

        for ln in leaf_names[:60]:
            ok, v, err = _try_call(root_obj, ln)
            if not ok:
                continue
            k = ("x.alt_units.{}::{}".format(root_name, ln) if is_alt else "x.primary_units.{}::{}".format(root_name, ln))
            _, normv = _kv_norm(ln, v)
            out["values"][k] = _format_synth_contract(normv)
            if is_alt:
                alt_kvs.append((ln, normv))
            else:
                primary_kvs.append((ln, normv))

    def _sig(kvs):
        parts = []
        for (k, v) in sorted(kvs, key=lambda x: x[0]):
            parts.append("{}={}".format(k, "" if v is None else str(v)))
        return _md5("|".join(parts))

    out["signatures"]["primary"] = _sig(primary_kvs) if len(primary_kvs) > 0 else None
    out["signatures"]["alternate"] = _sig(alt_kvs) if len(alt_kvs) > 0 else None

    out["found_members"] = sorted(list(set(out["found_members"])))
    return out

synth_member_samples = []
synth_crosswalk_rows = []

def _upsert_synth_inventory(param_key, contract, shape_key, shape_family):
    if not param_key or contract is None:
        return
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_shapes": set(),
            "observed_on_families": set(),
            "_seen_obs": set(),
            "unique_value_count": 0,
            "ok_but_unset_count": 0
        }

    entry = param_index[param_key]
    q = contract.get("q") or "unreadable"
    st = contract.get("storage")
    norm = contract.get("norm")

    obs_sig = (param_key, str(st), str(norm))
    if obs_sig in entry["_seen_obs"]:
        entry["observed_on_shapes"].add(shape_key)
        entry["observed_on_families"].add(shape_family)
        _maybe_set_example(entry, contract)
        return

    entry["_seen_obs"].add(obs_sig)
    entry["unique_value_count"] += 1

    if st:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    if q == "ok" and (contract.get("raw") is None) and (contract.get("norm") is None):
        entry["ok_but_unset_count"] += 1

    entry["observed_on_shapes"].add(shape_key)
    entry["observed_on_families"].add(shape_family)
    _maybe_set_example(entry, contract)

# Run synthetic extraction on the same sampled DimensionTypes
for dt in selected:
    shape_key, shape_label, shape_family, _ = _get_dim_shape_info(dt)

    fx = _try_extract_format_surface(dt)

    # keep small diagnostics sample only
    if fx.get("found_members"):
        synth_member_samples.append({
            "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
            "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
            "found_members": fx.get("found_members")[:30]
        })

    vals = fx.get("values") or {}
    for k, contract in vals.items():
        # synth keys should not collide with "p.*"
        _upsert_synth_inventory(k, contract, shape_key, shape_family)

    sigs = fx.get("signatures") or {}
    if sigs.get("primary") or sigs.get("alternate"):
        synth_crosswalk_rows.append({
            "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
            "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
            "dim_type.family_name_param": _get_family_name_param(dt),
            "dim_type.shape": shape_key,
            "dim_type.shape_family": shape_family,
            "format_sig.primary": sigs.get("primary"),
            "format_sig.alternate": sigs.get("alternate")
        })

for t in selected:
    shape_key, shape_label, shape_family, _ = _get_dim_shape_info(t)

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
                "ok_but_unset_count": 0,
                "example": None,
                "observed_on_shapes": set(),
                "observed_on_families": set(),
                "_seen_obs": set(),
                "unique_value_count": 0
            }

        entry = param_index[pk]
        q = pv.get("q") or "unreadable"
        st = pv.get("storage")
        norm = pv.get("norm")

        obs_sig = (pk, str(st), str(norm))
        if obs_sig in entry["_seen_obs"]:
            # Probe-local dedupe: do not double-count identical observations
            entry["observed_on_shapes"].add(shape_key)
            entry["observed_on_families"].add(shape_family)
            _maybe_set_example(entry, pv)
            continue

        entry["_seen_obs"].add(obs_sig)
        entry["unique_value_count"] += 1

        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1
        if q == "ok" and (pv.get("raw") is None) and (pv.get("norm") is None):
            entry["ok_but_unset_count"] += 1

        entry["observed_on_shapes"].add(shape_key)
        entry["observed_on_families"].add(shape_family)
        _maybe_set_example(entry, pv)

    # Explicit family name capture — not always in GetOrderedParameters()
    fn_val = _get_family_name_param(t)
    fn_contract = {
        "q": "ok" if fn_val is not None else "unreadable",
        "storage": "String",
        "raw": fn_val,
        "display": fn_val,
        "norm": fn_val
    }
    pk_fn = "x.dim_type.family_name_param"
    if pk_fn not in param_index:
        param_index[pk_fn] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "ok_but_unset_count": 0,
            "example": None,
            "observed_on_shapes": set(),
            "observed_on_families": set(),
            "_seen_obs": set(),
            "unique_value_count": 0
        }
    entry = param_index[pk_fn]
    q = fn_contract.get("q") or "unreadable"
    st = fn_contract.get("storage")
    norm = fn_contract.get("norm")
    obs_sig = (pk_fn, str(st), str(norm))
    if obs_sig not in entry["_seen_obs"]:
        entry["_seen_obs"].add(obs_sig)
        entry["unique_value_count"] += 1
        if st:
            entry["storage_types"].add(st)
        entry["q_counts"][q] = entry["q_counts"].get(q, 0) + 1
        _maybe_set_example(entry, fn_contract)
    entry["observed_on_shapes"].add(shape_key)
    entry["observed_on_families"].add(shape_family)


# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "dimension_types",
        "param_key": pk,
        "selected_type_sample_count": len(selected),
        "example": e.get("example"),
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "ok_but_unset_count": e.get("ok_but_unset_count", 0),
            "unique_value_count": e.get("unique_value_count", 0),
            "observed_on_shapes": sorted(list(e["observed_on_shapes"]))[:25],
            "observed_on_families": sorted(list(e.get("observed_on_families", set())))
        }
    })

# -------------------------
# Optional Crosswalk: DimensionType -> Tick Mark (Arrowhead)
# -------------------------

optional_crosswalk = []

# Always include format-signature crosswalk rows when discovered (probe-only).
# (This is analogous to arrowhead crosswalk but does not require enable_crosswalk.)
optional_crosswalk.extend(synth_crosswalk_rows)


DIM_TICK_PARAM_CANDIDATES = [
    "Tick Mark",
    "Tick mark",
    "Tick Mark Type",
    "Tick Mark Symbol",
]

def _find_tick_param(dt):
    for cand in DIM_TICK_PARAM_CANDIDATES:
        p = _safe(lambda: dt.LookupParameter(cand), None)
        if p is not None:
            return (cand, p)
    return (None, None)

# Keep crosswalk compact: one representative DimensionType per distinct arrowhead id
seen_arrowhead_ids = set()

if enable_crosswalk:
    # Optional extra input: max crosswalk rows to emit (default 50)
    crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 50

    for dt in selected:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        row = {
            "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
            "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
            "dim_type.shape": None,
            "tick_param.matched_name": None,
            "tick_param": None,
            "arrowhead.resolved": False,
            "arrowhead.type_id": None,
            "arrowhead.name": None
        }

        shape_key, shape_label, shape_family, _ = _get_dim_shape_info(dt)
        row["dim_type.shape"] = shape_key
        row["dim_type.shape_label"] = shape_label
        row["dim_type.shape_family"] = shape_family

        matched, p = _find_tick_param(dt)
        row["tick_param.matched_name"] = matched
        row["tick_param"] = _format_param_contract(p)

        if row["tick_param"]["storage"] != "ElementId" or row["tick_param"]["raw"] is None:
            continue

        ah_id = int(row["tick_param"]["raw"])
        if ah_id in seen_arrowhead_ids:
            continue

        row["arrowhead.type_id"] = ah_id

        ref = _safe(lambda: doc.GetElement(ElementId(ah_id)), None)
        if ref is not None:
            row["arrowhead.name"] = _safe(lambda: _safe_type_name(ref), None)

        row["arrowhead.resolved"] = True if row["arrowhead.name"] is not None else False

        if not row["arrowhead.resolved"]:
            continue

        seen_arrowhead_ids.add(ah_id)
        optional_crosswalk.append(row)


# -------------------------
# Assemble labeled output payload
# -------------------------

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "dimension_types",
        "records": param_inventory,
        "diagnostics": {
            "selected_type_sample_count": len(selected),
            "discovered_type_count": len(dim_types),
            "discovery_notes": discovery_notes,
            "format_surface_member_samples": synth_member_samples[:5],
            "format_signature_crosswalk_count": len(synth_crosswalk_rows)
        }
    },
    {
        "kind": "crosswalk",
        "domain": "dimension_types",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
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
        fixed_name = "probe_dimension_types_{}.json".format(date_stamp)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(OUT_payload, f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to inventory header (keeps OUT shape stable)
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

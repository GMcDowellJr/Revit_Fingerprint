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

OUT_payload = [
    {"kind": "inventory", "domain": "object_styles", "records": param_inventory},
    {"kind": "crosswalk", "domain": "object_styles", "records": optional_crosswalk},
]

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

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_object_styles_{}.json".format(date_stamp)

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

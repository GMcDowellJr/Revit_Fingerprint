# -*- coding: utf-8 -*-
# Dynamo Python (Revit) — Breadth Probe: view_category_overrides (EFFECTIVE vs OBJECT STYLES)
#
# This probe answers:
#   "What is the effective per-category graphics surface in view templates
#    and in views NOT controlled by a template, and where does it differ
#    from Object Styles baseline?"
#
# It mirrors the evidence style of probe_object_styles.py, but samples view/template
# contexts and computes "effective" values as:
#   effective = baseline(Object Styles) + (non-default OverrideGraphicSettings deltas)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "view_category_overrides",
#     "records": [...],
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "view_category_overrides",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_views_to_inspect (int)         Default: 50
#   IN[1] max_categories_to_inspect (int)    Default: 300
#   IN[2] include_subcategories (bool)       Default: True
#   IN[3] per_bucket_limit (int)            Default: 20   (bucket = IsTemplate|HasTemplate|ViewType)
#   IN[4] enable_crosswalk (bool)           Default: True
#   IN[5] crosswalk_limit (int)             Default: 200
#   IN[6] write_json (bool)                 Default: False
#   IN[7] output_directory (str)            Default: None
#
# Notes:
# - We focus on Object-Styles-comparable fields:
#     line color, line weights (proj/cut), line patterns (proj/cut), material
#   plus two common view-override-only fields:
#     halftone, transparency
# - We avoid full instance scans. Sampling is bounded.

import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    ElementId,
    FilteredElementCollector,
    View,
    Category,
    OverrideGraphicSettings,
    GraphicsStyleType,
    GraphicsStyle,
)

doc = DocumentManager.Instance.CurrentDBDocument

max_views_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 50
max_categories_to_inspect = IN[1] if len(IN) > 1 and IN[1] is not None else 300
include_subcategories = IN[2] if len(IN) > 2 and IN[2] is not None else True
per_bucket_limit = IN[3] if len(IN) > 3 and IN[3] is not None else 20
enable_crosswalk = IN[4] if len(IN) > 4 and IN[4] is not None else True
crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 200
write_json = IN[6] if len(IN) > 6 and IN[6] is not None else False
out_path = IN[7] if len(IN) > 7 and IN[7] is not None else None


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _get_name(obj):
    return _safe(lambda: obj.Name, None) if obj is not None else None

def _eid_int(eid):
    try:
        if eid is None:
            return None
        if isinstance(eid, ElementId):
            if eid == ElementId.InvalidElementId:
                return None
            return int(eid.IntegerValue)
        return int(eid)
    except:
        return None

def _eid_name(eid):
    if eid is None or eid == ElementId.InvalidElementId:
        return None
    el = _safe(lambda: doc.GetElement(eid), None)
    return _get_name(el)

def _rgb_triplet(c):
    if c is None:
        return None
    try:
        r = int(c.Red); g = int(c.Green); b = int(c.Blue)
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
    raw = _eid_int(eid)
    dn = display_name if display_name is not None else _eid_name(eid)
    return {"q": "ok", "storage": "ElementId", "raw": raw, "display": dn if dn is not None else (str(raw) if raw is not None else None), "norm": raw}

def _maybe_set_example(entry, pv):
    ex = entry.get("example")
    if ex is None:
        entry["example"] = {k: pv.get(k) for k in ("q", "storage", "raw", "display", "norm")}
        return
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {k: pv.get(k) for k in ("q", "storage", "raw", "display", "norm")}

def _bool_int(x):
    return 1 if bool(x) else 0

def _ogs_default():
    return OverrideGraphicSettings()

def _ogs_has_override_for_field(kind, ov, dflt):
    # Compare override value vs default OGS to determine "is override set"
    try:
        if kind == "color":
            return _rgb_triplet(ov) != _rgb_triplet(dflt)
        if kind == "eid":
            return _eid_int(ov) != _eid_int(dflt)
        return ov != dflt
    except:
        return True  # if comparison fails, treat as "override present" for audit, not silent drop


# -------------------------
# Category enumeration (Object Styles anchored via GraphicsStyle)
# -------------------------

def _iter_categories_from_object_styles(include_subcats_flag):
    """
    Anchor category discovery to Object Styles reality by collecting GraphicsStyle elements.
    This avoids reliance on doc.Settings.Categories, which can be empty in some Dynamo contexts.
    Returns: list of (Category, is_subcategory(bool), parent_category_or_None)
    """
    out = []
    seen = set()  # (cat_id_int, parent_id_int_or_None)

    gs_elems = _safe(
        lambda: list(FilteredElementCollector(doc).OfClass(GraphicsStyle).WhereElementIsNotElementType().ToElements()),
        default=[]
    )

    for gs in gs_elems:
        cat = _safe(lambda: gs.GraphicsStyleCategory, None)
        if cat is None:
            continue

        # Determine parent/subcategory if possible
        parent = _safe(lambda: cat.Parent, None) if include_subcats_flag else None
        is_sub = True if (include_subcats_flag and parent is not None) else False

        cat_id = _eid_int(_safe(lambda: cat.Id, None))
        parent_id = _eid_int(_safe(lambda: parent.Id, None)) if parent is not None else None
        key = (cat_id, parent_id)

        if cat_id is None:
            continue
        if key in seen:
            continue
        seen.add(key)

        out.append((cat, is_sub, parent))

    return out

all_cats = _iter_categories_from_object_styles(include_subcategories)

# Apply cap ONLY when user provides a positive integer.
# (Passing 0 in Dynamo previously wiped the list and looked like "no categories".)
try:
    max_n = int(max_categories_to_inspect)
    if max_n > 0:
        all_cats = all_cats[:max_n]
except:
    pass


def _category_path(cat, is_sub, parent):
    n = _get_name(cat)
    if not n:
        return None
    if is_sub and parent is not None and _get_name(parent):
        return "{}|{}".format(_get_name(parent), n)
    return "{}|self".format(n)

# -------------------------
# Baseline (Object Styles) for comparable fields
# -------------------------

def _baseline_for_cat(cat):
    # Baseline == Object Styles properties on Category
    # Each returns a contract payload.
    try:
        c_name = _get_name(cat)
        if c_name is None:
            # Still keep missing for traceability
            return {
                "base.line_color.rgb": _contract_missing("String"),
                "base.line_color.hex": _contract_missing("String"),
                "base.line_weight_projection": _contract_missing("Integer"),
                "base.line_weight_cut": _contract_missing("Integer"),
                "base.line_pattern_projection": _contract_missing("ElementId"),
                "base.line_pattern_cut": _contract_missing("ElementId"),
                "base.material": _contract_missing("ElementId"),
            }

        rgb = _rgb_triplet(_safe(lambda: cat.LineColor, None))
        base = {
            "base.line_color.rgb": _contract_string(rgb) if rgb is not None else _contract_missing("String"),
            "base.line_color.hex": _contract_string(_hex_from_rgb_triplet(rgb)) if rgb is not None else _contract_missing("String"),
            "base.line_weight_projection": _contract_int(_safe(lambda: cat.GetLineWeight(GraphicsStyleType.Projection), None))
                if _safe(lambda: cat.GetLineWeight(GraphicsStyleType.Projection), None) is not None else _contract_missing("Integer"),
            "base.line_weight_cut": _contract_int(_safe(lambda: cat.GetLineWeight(GraphicsStyleType.Cut), None))
                if _safe(lambda: cat.GetLineWeight(GraphicsStyleType.Cut), None) is not None else _contract_missing("Integer"),
            "base.line_pattern_projection": _contract_eid(_safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Projection), None))
                if _safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Projection), None) is not None else _contract_missing("ElementId"),
            "base.line_pattern_cut": _contract_eid(_safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Cut), None))
                if _safe(lambda: cat.GetLinePatternId(GraphicsStyleType.Cut), None) is not None else _contract_missing("ElementId"),
            "base.material": _contract_eid(_safe(lambda: cat.Material, None))
                if _safe(lambda: cat.Material, None) is not None else _contract_missing("ElementId"),
        }
        return base
    except:
        return {
            "base.line_color.rgb": _contract_unreadable("String"),
            "base.line_color.hex": _contract_unreadable("String"),
            "base.line_weight_projection": _contract_unreadable("Integer"),
            "base.line_weight_cut": _contract_unreadable("Integer"),
            "base.line_pattern_projection": _contract_unreadable("ElementId"),
            "base.line_pattern_cut": _contract_unreadable("ElementId"),
            "base.material": _contract_unreadable("ElementId"),
        }


# -------------------------
# View sampling:
#   - View Templates (IsTemplate)
#   - Views with NO ViewTemplateId (not controlled by template)
# -------------------------

all_views = _safe(
    lambda: list(FilteredElementCollector(doc).OfClass(View).WhereElementIsNotElementType().ToElements()),
    default=[]
)

templates = []
non_templated_views = []

for v in all_views:
    try:
        if _safe(lambda: v.IsTemplate, False):
            templates.append(v)
        else:
            vtid = _safe(lambda: v.ViewTemplateId, None)
            if vtid is None or vtid == ElementId.InvalidElementId:
                non_templated_views.append(v)
    except:
        # if unreadable classification, ignore (probe safety)
        pass

# Bucketed sampling: keep a small spread across templates and non-templated views
def _bucket_for_view(v):
    try:
        is_t = bool(_safe(lambda: v.IsTemplate, False))
        has_t = False
        if not is_t:
            vtid = _safe(lambda: v.ViewTemplateId, None)
            has_t = (vtid is not None and vtid != ElementId.InvalidElementId)
        vt = str(_safe(lambda: v.ViewType, None))
        return "is_template={}|has_template={}|viewtype={}".format(1 if is_t else 0, 1 if has_t else 0, vt)
    except:
        return "unknown"

def _select_views(candidates):
    by_bucket = {}
    out = []
    for v in candidates:
        b = _bucket_for_view(v)
        c = by_bucket.get(b, 0)
        ok = True
        try:
            ok = c < int(per_bucket_limit)
        except:
            ok = c < 20
        if ok:
            out.append(v)
            by_bucket[b] = c + 1
    return out

selected_views = _select_views(templates) + _select_views(non_templated_views)

try:
    mv = int(max_views_to_inspect)
    if mv >= 0:
        selected_views = selected_views[:mv]
except:
    pass


# -------------------------
# Effective evaluation:
#   baseline + (override deltas only)
# -------------------------

# Keys we treat as comparable or useful for override signal.
# (key_eff, kind, getter_from_ogs, baseline_key, baseline_extract_fn)
FIELDS = [
    ("eff.projection_line_color.rgb", "color", lambda ogs: ogs.ProjectionLineColor, "base.line_color.rgb", None),
    ("eff.projection_line_pattern", "eid", lambda ogs: ogs.ProjectionLinePatternId, "base.line_pattern_projection", None),
    ("eff.projection_line_weight", "int", lambda ogs: ogs.ProjectionLineWeight, "base.line_weight_projection", None),

    ("eff.cut_line_color.rgb", "color", lambda ogs: ogs.CutLineColor, "base.line_color.rgb", None),
    ("eff.cut_line_pattern", "eid", lambda ogs: ogs.CutLinePatternId, "base.line_pattern_cut", None),
    ("eff.cut_line_weight", "int", lambda ogs: ogs.CutLineWeight, "base.line_weight_cut", None),

    # Override-only (no object styles baseline)
    ("eff.halftone", "bool_int", lambda ogs: ogs.Halftone, None, None),
    ("eff.transparency", "int", lambda ogs: ogs.Transparency, None, None),
]

param_index = {}
optional_crosswalk = []

def _touch_param(pk, pv, bucket):
    if pk not in param_index:
        param_index[pk] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set()
        }
    e = param_index[pk]
    st = pv.get("storage")
    q = pv.get("q") or "unreadable"
    if st:
        e["storage_types"].add(st)
    if q not in e["q_counts"]:
        e["q_counts"][q] = 0
    e["q_counts"][q] += 1
    if bucket:
        e["observed_on_buckets"].add(bucket)
    _maybe_set_example(e, pv)

def _pv_from_field(kind, v):
    if kind == "int":
        if v is None:
            return _contract_missing("Integer")
        try:
            return _contract_int(int(v))
        except:
            return _contract_unreadable("Integer")
    if kind == "bool_int":
        if v is None:
            return _contract_missing("Integer")
        try:
            return _contract_int(_bool_int(v), display=str(bool(v)), norm=_bool_int(v))
        except:
            return _contract_unreadable("Integer")
    if kind == "eid":
        if v is None:
            return _contract_missing("ElementId")
        try:
            return _contract_eid(v)
        except:
            return _contract_unreadable("ElementId")
    if kind == "color":
        if v is None:
            return _contract_missing("String")
        try:
            rgb = _rgb_triplet(v)
            return _contract_string(rgb) if rgb is not None else _contract_missing("String")
        except:
            return _contract_unreadable("String")
    return _contract_unsupported(str(kind))

def _pv_bool_flag(x):
    return _contract_int(1 if x else 0, display="1" if x else "0", norm=1 if x else 0)

def _value_norm_for_compare(pv):
    # Compare on norm only
    return pv.get("norm")

# Sentinel to count GetCategoryOverrides failures (non-silent)
SENTINEL = "api.get_category_overrides"

dflt_ogs = _ogs_default()

# --- SENTINELS / DIAGNOSTICS (no silent empty outputs) ---

def _touch_diag(pk, pv, bucket="probe"):
    # lightweight touch to guarantee at least one inventory record
    _touch_param(pk, pv, bucket)

_touch_diag("diag.total_views_found", _contract_int(len(all_views) if all_views is not None else 0))
_touch_diag("diag.templates_found", _contract_int(len(templates)))
_touch_diag("diag.non_templated_views_found", _contract_int(len(non_templated_views)))
_touch_diag("diag.selected_views", _contract_int(len(selected_views)))

_touch_diag("diag.total_categories_found", _contract_int(len(all_cats)))
_touch_diag("diag.include_subcategories", _contract_int(1 if include_subcategories else 0))
_touch_diag("diag.max_categories_to_inspect", _contract_int(int(max_categories_to_inspect)))

# If selection collapsed to zero, emit an explicit diagnostic flag
if len(selected_views) == 0:
    _touch_diag("diag.ERROR_no_selected_views", _contract_int(1, display="1", norm=1))
if len(all_cats) == 0:
    _touch_diag("diag.ERROR_no_categories", _contract_int(1, display="1", norm=1))

for v in selected_views:
    v_bucket = _bucket_for_view(v)
    v_id = _eid_int(_safe(lambda: v.Id, None))
    v_name = _get_name(v)

    # Build per view counts to emit into inventory buckets
    for (cat, is_sub, parent) in all_cats:
        cat_path = _category_path(cat, is_sub, parent)
        if not cat_path:
            continue

        # baseline
        base = _baseline_for_cat(cat)

        # read overrides (guarded)
        ogs = None
        try:
            ogs = v.GetCategoryOverrides(cat.Id)
        except:
            ogs = None

        if ogs is None:
            _touch_param(SENTINEL, _contract_unreadable("None"), v_bucket)
            continue

        # Determine effective + override flags
        has_any_override = False

        eff_payload = {}
        delta_payload = {}

        for (k_eff, kind, getter, k_base, _) in FIELDS:
            ov = _safe(lambda: getter(ogs), None)
            dv = _safe(lambda: getter(dflt_ogs), None)

            is_override_set = _ogs_has_override_for_field(kind if kind != "bool_int" else "int", ov, dv)

            # effective:
            # - if baseline exists for this field: use override if set, else baseline
            # - if no baseline: use override value if set, else treat as "missing" (no signal)
            if k_base is not None and k_base in base:
                if is_override_set:
                    pv_eff = _pv_from_field(kind if kind != "bool_int" else "bool_int", ov)
                else:
                    # baseline is already a PV contract; but we want it under eff.* key
                    pv_eff = base[k_base]
            else:
                pv_eff = _pv_from_field(kind if kind != "bool_int" else "bool_int", ov) if is_override_set else _contract_missing("None")

            eff_payload[k_eff] = pv_eff

            # delta flag:
            if k_base is not None and k_base in base:
                base_norm = _value_norm_for_compare(base[k_base])
                eff_norm = _value_norm_for_compare(pv_eff)
                is_delta = (base_norm != eff_norm)
            else:
                # for fields with no baseline, delta is "override set"
                is_delta = bool(is_override_set)

            delta_payload["delta." + k_eff.replace("eff.", "")] = _pv_bool_flag(is_delta)

            if is_delta:
                has_any_override = True

            # inventory touch for eff + delta surfaces
            _touch_param(k_eff, pv_eff, v_bucket)
            _touch_param("delta." + k_eff.replace("eff.", ""), delta_payload["delta." + k_eff.replace("eff.", "")], v_bucket)

        # Crosswalk row: only emit when something differs OR when crosswalk is enabled and within cap
        if enable_crosswalk and len(optional_crosswalk) < int(crosswalk_limit):
            if has_any_override:
                row = {
                    "view.id": v_id,
                    "view.name": v_name,
                    "view.bucket": v_bucket,
                    "view.is_template": bool(_safe(lambda: v.IsTemplate, False)),
                    "category.path": cat_path,
                    "category.name": _get_name(cat),
                    "category.is_subcategory": True if is_sub else False,
                    "category.parent_name": _get_name(parent),
                    "has_override": True,
                    "baseline": {
                        "line_color.rgb": base["base.line_color.rgb"],
                        "line_weight_projection": base["base.line_weight_projection"],
                        "line_weight_cut": base["base.line_weight_cut"],
                        "line_pattern_projection": base["base.line_pattern_projection"],
                        "line_pattern_cut": base["base.line_pattern_cut"],
                        "material": base["base.material"],
                    },
                    "effective": {
                        "projection_line_color.rgb": eff_payload["eff.projection_line_color.rgb"],
                        "projection_line_weight": eff_payload["eff.projection_line_weight"],
                        "projection_line_pattern": eff_payload["eff.projection_line_pattern"],
                        "cut_line_color.rgb": eff_payload["eff.cut_line_color.rgb"],
                        "cut_line_weight": eff_payload["eff.cut_line_weight"],
                        "cut_line_pattern": eff_payload["eff.cut_line_pattern"],
                        "halftone": eff_payload["eff.halftone"],
                        "transparency": eff_payload["eff.transparency"],
                    },
                    "delta_flags": delta_payload
                }
                optional_crosswalk.append(row)


# -------------------------
# Inventory assembly (object_styles-like)
# -------------------------

param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "view_category_overrides",
        "param_key": pk,
        "selected_view_sample_count": len(selected_views),
        "selected_category_sample_count": len(all_cats),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
        }
    })

OUT_payload = [
    {"kind": "inventory", "domain": "view_category_overrides", "records": param_inventory},
    {"kind": "crosswalk", "domain": "view_category_overrides", "records": optional_crosswalk},
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
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_view_category_overrides_{}.json".format(date_stamp)

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

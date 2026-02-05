# Dynamo Python (Revit) — Breadth Probe: view_filter_applications (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "view_filter_applications",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "view_filter_applications",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_views_to_inspect (int)
#        Maximum number of View elements to inspect AFTER filtering for views that have filters.
#        Default: 300
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit a compact View/ViewTemplate → ParameterFilterElement crosswalk.
#        Default: False
#
#   IN[2] per_bucket_limit (int)
#        Sample at most N views per bucket where bucket = "<is_template>|<ViewType>".
#        Default: 2
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_view_filter_applications_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.
#
#   IN[5] per_view_filter_limit (int)
#        Sample at most N filters per view/template (preserves order, truncates after N).
#        Default: 25


import clr
import os
import json
import hashlib
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId, StorageType,
    BuiltInParameter, View, OverrideGraphicSettings,
    ParameterFilterElement, Color
)

doc = DocumentManager.Instance.CurrentDBDocument

max_views_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 300
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_bucket_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 2
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
per_view_filter_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 25


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

def _eid_int(eid):
    if eid is None:
        return None
    try:
        if eid == ElementId.InvalidElementId:
            return None
    except:
        pass
    return _safe(lambda: eid.IntegerValue, None)

def _color_rgb_hex(c):
    if c is None:
        return (None, None)
    try:
        r = int(c.Red)
        g = int(c.Green)
        b = int(c.Blue)
        rgb = "{}|{}|{}".format(r, g, b)
        hx = "#{:02X}{:02X}{:02X}".format(r, g, b)
        return (rgb, hx)
    except:
        return (None, None)

def _contract(q, storage, raw, display, norm):
    # storage must be: String | Integer | Double | ElementId | None
    return {
        "q": q,
        "storage": storage,
        "raw": raw,
        "display": display,
        "norm": norm
    }

def _as_int_contract(x):
    if x is None:
        return _contract("missing", "Integer", None, None, None)
    try:
        iv = int(x)
        return _contract("ok", "Integer", iv, str(iv), iv)
    except:
        return _contract("unreadable", "Integer", None, None, None)

def _as_bool_int_contract(x):
    if x is None:
        return _contract("missing", "Integer", None, None, None)
    try:
        iv = 1 if bool(x) else 0
        return _contract("ok", "Integer", iv, "True" if iv == 1 else "False", iv)
    except:
        return _contract("unreadable", "Integer", None, None, None)

def _as_string_contract(x):
    if x is None:
        return _contract("missing", "String", None, None, None)
    try:
        s = str(x)
        return _contract("ok", "String", s, s, s)
    except:
        return _contract("unreadable", "String", None, None, None)

def _as_elementid_contract(eid):
    iv = _eid_int(eid)
    if iv is None:
        return _contract("ok", "ElementId", None, None, None)
    name = None
    ref = _safe(lambda: doc.GetElement(eid), None)
    if ref is not None:
        name = _safe(lambda: ref.Name, None)
        if name is None:
            name = _safe(lambda: _safe_type_name(ref), None)
    disp = name if name is not None else str(iv)
    return _contract("ok", "ElementId", iv, disp, iv)

def _ogs_get(ogs, attr_name):
    """
    Read OverrideGraphicSettings member defensively.

    IMPORTANT:
      - Do NOT invoke callables here. In pythonnet, some members can present as
        callable proxies; calling the wrong overload will throw and produce false 'unreadable'.
      - Treat AttributeError as 'unsupported' and other exceptions as 'unreadable'.

    Returns (q, value) where q is ok|missing|unsupported|unreadable.
    """
    if ogs is None:
        return ("missing", None)

    try:
        return ("ok", getattr(ogs, attr_name))
    except AttributeError:
        return ("unsupported", None)
    except Exception:
        return ("unreadable", None)

def _hash_sig(pairs):
    """
    pairs: list of (k, norm_str) where norm_str is already stable string.
    """
    try:
        s = "|".join(["{}={}".format(k, v) for (k, v) in pairs])
        return hashlib.sha1(s.encode("utf-8")).hexdigest()
    except:
        return None


# -------------------------
# Inventory accumulator (dedup by (param_key, storage_type, norm))
# -------------------------

# param_key -> entry
param_index = {}

def _ensure_param(pk):
    if pk not in param_index:
        param_index[pk] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set(),
            # dedup tracking
            "_obs_best_q_by_sig": {}  # (storage, norm_str) -> q
        }
    return param_index[pk]

def _q_rank(q):
    # best-signal wins for the same (pk, storage, norm)
    # ok > missing > unreadable > unsupported
    ranks = {"ok": 3, "missing": 2, "unreadable": 1, "unsupported": 0}
    return ranks.get(q, 0)

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
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }

def _observe(pk, pv, bucket_key):
    entry = _ensure_param(pk)

    q = pv.get("q") or "unreadable"
    st = pv.get("storage")

    if st:
        entry["storage_types"].add(st)

    # probe-local dedup by (param_key, storage_type, normalized_value)
    norm = pv.get("norm")
    norm_str = "None" if norm is None else str(norm)
    sig = (str(st), norm_str)

    prev_q = entry["_obs_best_q_by_sig"].get(sig)
    if prev_q is None:
        entry["_obs_best_q_by_sig"][sig] = q
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1
    else:
        # upgrade counts if new observation is "better"
        if _q_rank(q) > _q_rank(prev_q):
            # decrement old
            if prev_q not in entry["q_counts"]:
                entry["q_counts"][prev_q] = 0
            entry["q_counts"][prev_q] = max(0, entry["q_counts"][prev_q] - 1)
            # increment new
            if q not in entry["q_counts"]:
                entry["q_counts"][q] = 0
            entry["q_counts"][q] += 1
            entry["_obs_best_q_by_sig"][sig] = q

    entry["observed_on_buckets"].add(bucket_key)
    _maybe_set_example(entry, pv)


# -------------------------
# Discovery + Sampling
# -------------------------

all_views = _safe(
    lambda: (FilteredElementCollector(doc)
             .OfClass(View)
             .WhereElementIsNotElementType()
             .ToElements()),
    default=[]
)

try:
    all_views = list(all_views)
except:
    all_views = list(all_views)

def _view_bucket_key(v):
    is_t = _safe(lambda: v.IsTemplate, False)
    vt = _safe(lambda: v.ViewType, None)
    return "{}|{}".format("T" if is_t else "V", str(vt))

def _view_has_filters(v):
    # Guarded: some view types can throw on GetFilters
    fids = _safe(lambda: list(v.GetFilters()), default=None)
    if fids is None:
        return False
    try:
        return len(fids) > 0
    except:
        return False

# Filter to views/templates that actually have filters
candidates = []
for v in all_views:
    if _view_has_filters(v):
        candidates.append(v)

# Cap AFTER filtering (avoid collector ordering bias)
try:
    max_n = int(max_views_to_inspect)
    if max_n >= 0:
        candidates = candidates[:max_n]
except:
    pass

# Sample breadth-first: first N per bucket = "<is_template>|<ViewType>"
selected = []
bucket_counts = {}
for v in candidates:
    bk = _view_bucket_key(v)
    c = bucket_counts.get(bk, 0)
    if per_bucket_limit is None:
        ok = True
    else:
        try:
            lim = int(per_bucket_limit)
            ok = True if lim < 0 else (c < lim)
        except:
            ok = c < 2
    if ok:
        selected.append(v)
        bucket_counts[bk] = c + 1

# Fallback: ensure at least one if there are any candidates
if len(selected) == 0 and len(candidates) > 0:
    selected = [candidates[0]]


# -------------------------
# Extract application surface (synthetic "parameters")
# -------------------------

OGS_FIELDS = [
    # Lines
    ("vfa.ogs.proj_line_color.rgb", "ProjectionLineColor", "color_rgb"),
    ("vfa.ogs.proj_line_color.hex", "ProjectionLineColor", "color_hex"),
    ("vfa.ogs.cut_line_color.rgb", "CutLineColor", "color_rgb"),
    ("vfa.ogs.cut_line_color.hex", "CutLineColor", "color_hex"),
    ("vfa.ogs.proj_line_pattern_id", "ProjectionLinePatternId", "elementid"),
    ("vfa.ogs.cut_line_pattern_id", "CutLinePatternId", "elementid"),
    ("vfa.ogs.proj_line_weight", "ProjectionLineWeight", "int"),
    ("vfa.ogs.cut_line_weight", "CutLineWeight", "int"),

    # Surface patterns
    ("vfa.ogs.surf_fg_pattern_id", "SurfaceForegroundPatternId", "elementid"),
    ("vfa.ogs.surf_fg_pattern_color.rgb", "SurfaceForegroundPatternColor", "color_rgb"),
    ("vfa.ogs.surf_fg_pattern_color.hex", "SurfaceForegroundPatternColor", "color_hex"),
    ("vfa.ogs.surf_bg_pattern_id", "SurfaceBackgroundPatternId", "elementid"),
    ("vfa.ogs.surf_bg_pattern_color.rgb", "SurfaceBackgroundPatternColor", "color_rgb"),
    ("vfa.ogs.surf_bg_pattern_color.hex", "SurfaceBackgroundPatternColor", "color_hex"),

    # Cut patterns
    ("vfa.ogs.cut_fg_pattern_id", "CutForegroundPatternId", "elementid"),
    ("vfa.ogs.cut_fg_pattern_color.rgb", "CutForegroundPatternColor", "color_rgb"),
    ("vfa.ogs.cut_fg_pattern_color.hex", "CutForegroundPatternColor", "color_hex"),
    ("vfa.ogs.cut_bg_pattern_id", "CutBackgroundPatternId", "elementid"),
    ("vfa.ogs.cut_bg_pattern_color.rgb", "CutBackgroundPatternColor", "color_rgb"),
    ("vfa.ogs.cut_bg_pattern_color.hex", "CutBackgroundPatternColor", "color_hex"),

    # Misc
    ("vfa.ogs.halftone", "Halftone", "bool_int"),
    ("vfa.ogs.transparency", "Transparency", "int"),
]

def _pv_from_ogs_field(ogs, attr_name, kind):
    q, v = _ogs_get(ogs, attr_name)
    if q != "ok":
        # map to contract q-states
        if q == "unsupported":
            return _contract("unsupported", None, None, None, None)
        if q == "missing":
            return _contract("missing", None, None, None, None)
        return _contract("unreadable", None, None, None, None)

    if kind == "int":
        return _as_int_contract(v)

    if kind == "bool_int":
        return _as_bool_int_contract(v)

    if kind == "elementid":
        return _as_elementid_contract(v)

    if kind == "color_rgb":
        rgb, hx = _color_rgb_hex(v)
        if rgb is None:
            return _contract("missing", "String", None, None, None)
        # match line_styles: raw/display/norm are all the rgb triplet
        return _contract("ok", "String", rgb, rgb, rgb)

    if kind == "color_hex":
        rgb, hx = _color_rgb_hex(v)
        if hx is None:
            return _contract("missing", "String", None, None, None)
        # match line_styles: raw/display/norm are all the hex string
        return _contract("ok", "String", hx, hx, hx)

    # should never happen, but remain defensive
    return _contract("unsupported", None, None, None, None)

def _is_defaultish_ogs_value(pk, pv):
    """
    Used only for computing a signature hash (not for inventory).
    Conservatively treat None as default; for ints treat 0 as default; for strings treat None as default.
    """
    if pv is None:
        return True
    if pv.get("q") != "ok":
        return True
    n = pv.get("norm")
    if n is None:
        return True
    # common defaults
    try:
        if pv.get("storage") == "Integer" and int(n) == 0:
            return True
    except:
        pass
    return False

def _collect_applied_filters_in_order(v):
    fids = _safe(lambda: list(v.GetFilters()), default=[])
    try:
        fids = list(fids)
    except:
        pass

    # truncate per view, preserving order
    try:
        lim = int(per_view_filter_limit)
        if lim >= 0:
            fids = fids[:lim]
    except:
        pass

    return fids

for v in selected:
    bk = _view_bucket_key(v)

    # view-level signals
    _observe("vfa.view.is_template", _as_bool_int_contract(_safe(lambda: v.IsTemplate, False)), bk)
    _observe("vfa.view.view_type", _as_string_contract(_safe(lambda: v.ViewType, None)), bk)

    vname = _safe(lambda: v.Name, None)
    _observe("vfa.view.name", _as_string_contract(vname), bk)

    # filter stack
    fids = _collect_applied_filters_in_order(v)
    _observe("vfa.filter_stack.count", _as_int_contract(len(fids)), bk)

    for idx, fid in enumerate(fids):
        # application-level synthetic keys (each produces inventory evidence)
        _observe("vfa.filter.order_index", _as_int_contract(idx), bk)

        _observe("vfa.filter.id", _as_elementid_contract(fid), bk)

        f = _safe(lambda: doc.GetElement(fid), None)
        fname = _safe(lambda: f.Name, None) if f is not None else None
        _observe("vfa.filter.name", _as_string_contract(fname), bk)

        vis = _safe(lambda: v.GetFilterVisibility(fid), default=None)
        if vis is None:
            _observe("vfa.filter.visibility", _contract("unreadable", "Integer", None, None, None), bk)
        else:
            _observe("vfa.filter.visibility", _as_bool_int_contract(vis), bk)

        ogs = _safe(lambda: v.GetFilterOverrides(fid), default=None)
        if ogs is None:
            _observe("vfa.ogs.present", _contract("unreadable", "Integer", None, None, None), bk)
            continue

        _observe("vfa.ogs.present", _as_bool_int_contract(True), bk)

        # capture the override surface (field-by-field)
        sig_pairs = []
        for (pk, attr, kind) in OGS_FIELDS:
            pv = _pv_from_ogs_field(ogs, attr, kind)
            _observe(pk, pv, bk)
            if not _is_defaultish_ogs_value(pk, pv):
                sig_pairs.append((pk, str(pv.get("norm"))))

        sig_hash = _hash_sig(sig_pairs) if len(sig_pairs) > 0 else None
        _observe("vfa.ogs.sig_hash", _as_string_contract(sig_hash), bk)

        has_any_override = True if sig_hash is not None else False
        _observe("vfa.ogs.has_any_override", _as_bool_int_contract(has_any_override), bk)


# -------------------------
# Emit inventory records (stable order)
# -------------------------

param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "view_filter_applications",
        "param_key": pk,
        "selected_view_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
        }
    })


# -------------------------
# Optional Crosswalk: View/ViewTemplate → ParameterFilterElement
# -------------------------

optional_crosswalk = []

if enable_crosswalk:
    # Emit one row per (view, filter) occurrence so overrides are verifiable per template/view.
    # Dedup key: (view.id, filter.id)
    seen_occ = set()

    for v in selected:
        v_is_template = _safe(lambda: v.IsTemplate, False)
        v_viewtype = _safe(lambda: v.ViewType, None)
        v_id = _safe(lambda: v.Id.IntegerValue, None)
        v_name = _safe(lambda: v.Name, None)

        fids = _collect_applied_filters_in_order(v)

        for idx, fid in enumerate(fids):
            fid_int = _eid_int(fid)
            if fid_int is None or v_id is None:
                continue

            occ_key = "{}|{}".format(str(v_id), str(fid_int))
            if occ_key in seen_occ:
                continue

            fe = _safe(lambda: doc.GetElement(fid), None)
            if fe is None:
                continue

            fname = _safe(lambda: fe.Name, None)

            # Pull per-occurrence visibility + overrides (guarded)
            vis = _safe(lambda: v.GetFilterVisibility(fid), default=None)
            ogs = _safe(lambda: v.GetFilterOverrides(fid), default=None)

            # Colors (proof fields)
            pv_pl_rgb = _pv_from_ogs_field(ogs, "ProjectionLineColor", "color_rgb") if ogs is not None else _contract("missing", "String", None, None, None)
            pv_pl_hex = _pv_from_ogs_field(ogs, "ProjectionLineColor", "color_hex") if ogs is not None else _contract("missing", "String", None, None, None)
            pv_cl_rgb = _pv_from_ogs_field(ogs, "CutLineColor", "color_rgb") if ogs is not None else _contract("missing", "String", None, None, None)
            pv_cl_hex = _pv_from_ogs_field(ogs, "CutLineColor", "color_hex") if ogs is not None else _contract("missing", "String", None, None, None)

            row = {
                "view.id": v_id,
                "view.name": v_name,
                "view.is_template": True if v_is_template else False,
                "view.view_type": str(v_viewtype),
                "filter.order_index": idx,
                "filter.id": fid_int,
                "filter.name": fname,
                "filter.class": _safe(lambda: fe.GetType().FullName, None),
                "filter.visibility": vis,
                "ogs.present": True if ogs is not None else False
            }

            # Emit full OGS surface as parameter-like payloads
            ogs_payload = {}
            sig_pairs = []

            if ogs is not None:
                for (pk, attr, kind) in OGS_FIELDS:
                    pv = _pv_from_ogs_field(ogs, attr, kind)
                    ogs_payload[pk] = pv

                    # keep sig_hash aligned with inventory behavior
                    if not _is_defaultish_ogs_value(pk, pv):
                        sig_pairs.append((pk, str(pv.get("norm"))))

            sig_hash = _hash_sig(sig_pairs) if len(sig_pairs) > 0 else None

            row["ogs.overrides"] = ogs_payload
            row["ogs.sig_hash"] = sig_hash
            row["ogs.has_any_override"] = True if sig_hash is not None else False

            # Category sampling if it is a ParameterFilterElement
            if isinstance(fe, ParameterFilterElement):
                row["pfe.is_parameter_filter_element"] = True
                cats = _safe(lambda: list(fe.GetCategories()), default=[])
                try:
                    cats = list(cats)
                except:
                    pass
                row["pfe.category_count"] = len(cats)

                names = []
                for cid in cats:
                    ce = _safe(lambda: doc.GetElement(cid), None)
                    cn = _safe(lambda: ce.Name, None) if ce is not None else None
                    if cn:
                        names.append(cn)
                row["pfe.category_names_sample"] = sorted(list(set(names)))[:25]
            else:
                row["pfe.is_parameter_filter_element"] = False
                row["pfe.category_count"] = None
                row["pfe.category_names_sample"] = []

            seen_occ.add(occ_key)
            optional_crosswalk.append(row)

# -------------------------
# Assemble OUT + optional JSON write
# -------------------------

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "view_filter_applications",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "view_filter_applications",
        "records": optional_crosswalk
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
        fixed_name = "probe_view_filter_applications_{}.json".format(date_stamp)

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

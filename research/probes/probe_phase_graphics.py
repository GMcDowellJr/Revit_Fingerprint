# Dynamo Python (Revit) — Breadth Probe: phase_graphics (INVENTORY OUTPUT)
#
# Goal:
#   Exploratory evidence capture for "phase_graphics" parameter surface.
#   This is NOT production export logic.
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "phase_graphics",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "phase_graphics",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_views_to_inspect (int)
#        Maximum number of Views to inspect (templates preferred).
#        Default: 200
#
#   IN[1] max_phasefilters_to_inspect (int)
#        Maximum number of PhaseFilter elements to inspect (if accessible).
#        Default: 200
#
#   IN[2] enable_crosswalk (bool)
#        Whether to emit ViewTemplate -> PhaseFilter crosswalk (if resolvable).
#        Default: False
#
#   IN[3] per_bucket_limit (int)
#        Sample at most N per bucket (templates vs non-templates).
#        Default: 50
#
#   IN[4] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[5] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_phase_graphics_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.
#
#   IN[6] crosswalk_limit (int)
#        Max crosswalk rows to emit (default 50)


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
    BuiltInParameter, View
)

try:
    # Not present in all Revit API surfaces, but usually available
    from Autodesk.Revit.DB import PhaseFilter
except:
    PhaseFilter = None

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_views_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
max_phasefilters_to_inspect = IN[1] if len(IN) > 1 and IN[1] is not None else 200
enable_crosswalk = IN[2] if len(IN) > 2 and IN[2] is not None else False
per_bucket_limit = IN[3] if len(IN) > 3 and IN[3] is not None else 50
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

def _safe_elem_name(e):
    # prefer Name, but guard hard
    try:
        n = e.Name
        return n
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
      - Integer.norm stays as integer (enum-safe).
      - Length -> inches
      - Angle  -> degrees
      - ElementId -> IntegerValue; display tries to resolve name cheaply
      - StorageType.None -> storage "None" (NOT "0"), q="ok"
    """
    if p is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}

    st = _safe(lambda: p.StorageType, None)
    if st is None:
        return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}

    # Explicit mapping avoids enum->int stringification ("0")
    # NOTE: StorageType has a member named "None" but Python can't parse StorageType.None.
    # Use numeric enum value (0) defensively.
    try:
        st_int = int(st)
    except:
        st_int = None

    if st_int == 0:
        # Often represents non-primitive / complex parameter surfaces.
        # Keep it auditably "present but not value-typed".
        disp = _safe(lambda: p.AsValueString(), None)
        return {"q": "ok", "storage": "None", "raw": None, "display": disp, "norm": None}

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

    # Fallback: label unknown storage types without "0" if possible
    st_label = None
    try:
        st_label = str(st)
        # Some environments stringify enums as ints; keep a clearer label
        if st_label in ("0", "1", "2", "3", "4"):
            st_label = "StorageType({})".format(st_label)
    except:
        st_label = None

    return {"q": "unsupported", "storage": st_label, "raw": None, "display": None, "norm": None}

# -------------------------
# Progressive Discovery
# -------------------------

diagnostics = {
    "phasefilter_class_available": True if PhaseFilter is not None else False,
    "views_collected": 0,
    "view_templates_collected": 0,
    "phasefilters_collected": 0,
    "notes": []
}

# Step 1: View-based discovery (templates preferred, but include non-templates as fallback)
all_views = _safe(
    lambda: list(FilteredElementCollector(doc).OfClass(View).ToElements()),
    default=[]
)

templates = []
non_templates = []

for v in all_views:
    is_t = _safe(lambda: v.IsTemplate, False)
    if is_t:
        templates.append(v)
    else:
        non_templates.append(v)

diagnostics["views_collected"] = len(all_views)
diagnostics["view_templates_collected"] = len(templates)

def _cap(lst, n):
    try:
        n = int(n)
        if n < 0:
            return lst
        return lst[:n]
    except:
        return lst

# Bucket-biased sampling: templates first, then non-templates
selected_views = []
try:
    lim = int(per_bucket_limit)
except:
    lim = 50

selected_views.extend(_cap(templates, min(lim, int(max_views_to_inspect) if max_views_to_inspect is not None else lim)))

# If we still have room, sample non-templates
remaining = None
try:
    remaining = int(max_views_to_inspect) - len(selected_views)
except:
    remaining = 0

if remaining and remaining > 0:
    selected_views.extend(_cap(non_templates, min(lim, remaining)))

# Step 2: PhaseFilter elements (if API exposes them)
phase_filters = []
if PhaseFilter is not None:
    phase_filters = _safe(
        lambda: list(FilteredElementCollector(doc).OfClass(PhaseFilter).ToElements()),
        default=[]
    )
else:
    diagnostics["notes"].append("PhaseFilter class not importable in this context; skipping PhaseFilter element discovery.")

phase_filters = _cap(phase_filters, max_phasefilters_to_inspect)
diagnostics["phasefilters_collected"] = len(phase_filters)

# If both are empty, we still return OUT with empty records and diagnostics carried in observed fields.
if len(selected_views) == 0 and len(phase_filters) == 0:
    diagnostics["notes"].append("No Views (templates or otherwise) were collectible, and no PhaseFilters were collected.")


# -------------------------
# Build inventory (union over selected elements)
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   observed_on_buckets: set(str)
# }
param_index = {}

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

def _index_params_from_elem(elem, bucket_key):
    params = _safe(lambda: list(elem.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(elem.Parameters), default=[])

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
                "example": None,
                "observed_on_buckets": set()
            }

        entry = param_index[pk]
        st = pv.get("storage")
        q = pv.get("q") or "unreadable"

        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1

        entry["observed_on_buckets"].add(bucket_key)
        _maybe_set_example(entry, pv)

# Index selected view parameters
for v in selected_views:
    is_t = _safe(lambda: v.IsTemplate, False)
    vname = _safe(lambda: _safe_elem_name(v), None)
    bucket = "view_template" if is_t else "view"
    # include a tiny bit of identity in bucket key for breadth (capped later)
    bucket_key = "{}|{}".format(bucket, vname if vname else "unnamed")
    _index_params_from_elem(v, bucket_key)

# Index phase filter parameters (if any)
for pf in phase_filters:
    pfname = _safe(lambda: _safe_elem_name(pf), None)
    bucket_key = "phasefilter|{}".format(pfname if pfname else "unnamed")
    _index_params_from_elem(pf, bucket_key)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    observed_buckets = sorted(list(e["observed_on_buckets"]))
    # cap breadth list (signal, not spam)
    observed_buckets = observed_buckets[:25]

    param_inventory.append({
        "domain": "phase_graphics",
        "param_key": pk,
        "selected_view_sample_count": len(selected_views),
        "selected_phasefilter_sample_count": len(phase_filters),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": observed_buckets,
            "diagnostics": diagnostics
        }
    })


# -------------------------
# Optional Crosswalk: ViewTemplate -> PhaseFilter
# -------------------------

optional_crosswalk = []

# Candidate parameter names (varies by localization/templates; keep flexible)
VIEW_PHASE_FILTER_PARAM_CANDIDATES = [
    "Phase Filter",
    "Phase filter",
    "PhaseFilter",
    "View Phase Filter"
]

# Built-in parameter is preferred if present (more stable than name strings)
BIP_PHASE_FILTER = _safe(lambda: BuiltInParameter.VIEW_PHASE_FILTER, None)

def _get_phasefilter_param_from_view(v):
    # Try BIP first
    if BIP_PHASE_FILTER is not None:
        p = _safe(lambda: v.get_Parameter(BIP_PHASE_FILTER), None)
        if p is not None:
            return ("BuiltInParameter.VIEW_PHASE_FILTER", p)

    # Then try name candidates
    for nm in VIEW_PHASE_FILTER_PARAM_CANDIDATES:
        p = _safe(lambda: v.LookupParameter(nm), None)
        if p is not None:
            return (nm, p)

    return (None, None)

if enable_crosswalk:
    # Keep compact: one row per distinct phasefilter id
    seen_pf_ids = set()

    # Prefer templates for crosswalk signal
    crosswalk_views = templates if len(templates) > 0 else selected_views

    for v in crosswalk_views:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        is_t = _safe(lambda: v.IsTemplate, False)
        if not is_t:
            # Crosswalk is primarily meaningful on templates; skip non-templates unless no templates exist
            if len(templates) > 0:
                continue

        matched_name, p = _get_phasefilter_param_from_view(v)
        pv = _format_param_contract(p)

        # Only keep ElementId mappings
        if pv.get("storage") != "ElementId" or pv.get("raw") is None:
            continue

        pf_id = int(pv.get("raw"))
        if pf_id in seen_pf_ids:
            continue

        pf_elem = _safe(lambda: doc.GetElement(ElementId(pf_id)), None)
        pf_name = _safe(lambda: _safe_elem_name(pf_elem), None) if pf_elem is not None else None

        # Keep only resolved if possible (signal > noise)
        resolved = True if pf_name is not None else False
        if not resolved:
            continue

        row = {
            "view_template.id": _safe(lambda: v.Id.IntegerValue, None),
            "view_template.name": _safe(lambda: _safe_elem_name(v), None),
            "phase_filter_param.matched_name": matched_name,
            "phase_filter_param": pv,
            "phasefilter.resolved": resolved,
            "phasefilter.id": pf_id,
            "phasefilter.name": pf_name
        }

        seen_pf_ids.add(pf_id)
        optional_crosswalk.append(row)


# -------------------------
# Assemble OUT + optional write
# -------------------------

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "phase_graphics",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "phase_graphics",
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
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_phase_graphics_{}.json".format(date_stamp)

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

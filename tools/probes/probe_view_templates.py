# Dynamo Python (Revit) — Breadth Probe: view_templates (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "view_templates",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "view_templates",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_templates_to_inspect (int)
#        Maximum number of view templates to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit View (non-template) → ViewTemplate crosswalk.
#        Default: False
#
#   IN[2] per_viewtype_limit (int)
#        Sample at most N templates per ViewType value (breadth bias).
#        Default: 3
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_view_templates_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.
#
#   IN[5] crosswalk_limit (int)  (only used when enable_crosswalk=True)
#        Maximum number of crosswalk rows to emit.
#        Default: 50
#
# Reference pattern: probe_arrowheads.py :contentReference[oaicite:0]{index=0}


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
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_templates_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_viewtype_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 3
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 50


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _safe_view_name(v):
    # Prefer VIEW_NAME; fall back to Name
    try:
        p = v.get_Parameter(BuiltInParameter.VIEW_NAME)
        if p is not None:
            s = p.AsString()
            if s:
                return s
    except:
        pass
    return _safe(lambda: v.Name, None)

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

    Normalization:
      - Integer.norm stays integer (enum-safe)
      - Length -> inches when datatype is Length
      - Angle  -> degrees when datatype is Angle
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

    elif st == StorageType.Integer:
        raw = _safe(lambda: p.AsInteger(), None)
        disp = _fmt_display(p, None)
        return {
            "q": "ok",
            "storage": "Integer",
            "raw": raw,
            "display": disp if disp is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    elif st == StorageType.Double:
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

    elif st == StorageType.ElementId:
        eid = _safe(lambda: p.AsElementId(), None)
        if eid is None or eid == ElementId.InvalidElementId:
            return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}

        raw = _safe(lambda: eid.IntegerValue, None)
        ref = _safe(lambda: doc.GetElement(eid), None)
        ref_name = _safe(lambda: ref.Name, None) if ref else None

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else str(raw),
            "norm": raw
        }

    elif int(st) == 0:
        # StorageType.None (enum value 0): UI composite parameters ("Edit...")
        disp = _safe(lambda: p.AsValueString(), None)
        return {
            "q": "unsupported",
            "storage": "None",
            "raw": None,
            "display": disp,
            "norm": None
        }

    else:
        return {
            "q": "unsupported",
            "storage": str(st),
            "raw": None,
            "display": None,
            "norm": None
        }


def _viewtype_bucket(v):
    vt = _safe(lambda: v.ViewType, None)
    if vt is None:
        return "missing"
    return "{}|{}".format(int(vt), str(vt))


# -------------------------
# Discovery + Sampling (category-free)
# -------------------------

all_views = _safe(
    lambda: (FilteredElementCollector(doc)
             .OfClass(View)
             .ToElements()),
    default=[]
)

try:
    all_views = list(all_views)
except:
    all_views = list(all_views)

templates = []
for v in all_views:
    # Defensive: some view-like things can be odd in certain contexts
    is_t = _safe(lambda: v.IsTemplate, False)
    if is_t:
        templates.append(v)

# Cap AFTER filtering (-1 means unlimited)
try:
    max_n = int(max_templates_to_inspect)
    if max_n >= 0:
        templates = templates[:max_n]
    # max_n < 0 → no cap
except:
    pass

# Sample first N per ViewType (breadth bias)
selected = []
by_vt = {}  # bucket -> count
for v in templates:
    b = _viewtype_bucket(v)
    c = by_vt.get(b, 0)

    # Per-viewtype cap (-1 means unlimited)
    try:
        pvl = int(per_viewtype_limit)
    except:
        pvl = 3

    if pvl < 0:
        per_ok = True
    else:
        per_ok = c < pvl

    if per_ok:
        selected.append(v)
        by_vt[b] = c + 1

# Fallback: at least 1 per bucket
if len(selected) == 0 and len(templates) > 0:
    seen = set()
    for v in templates:
        b = _viewtype_bucket(v)
        if b not in seen:
            selected.append(v)
            seen.add(b)


# -------------------------
# Build inventory (union over selected)
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   observed_on_viewtype_buckets: set(str)
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

for v in selected:

    # Template "Include" state:
    # In Revit API, these are the parameters NOT controlled by the template.
    non_ctrl_ids = _safe(lambda: v.GetNonControlledTemplateParameterIds(), default=None)
    non_ctrl_ints = set()
    if non_ctrl_ids is not None:
        try:
            for eid in list(non_ctrl_ids):
                iv = _safe(lambda: eid.IntegerValue, None)
                if iv is not None:
                    non_ctrl_ints.add(iv)
        except:
            pass

    vt_bucket = _viewtype_bucket(v)

    params = _safe(lambda: list(v.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(v.Parameters), default=[])

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
                "observed_on_viewtype_buckets": set(),
                "template_control_counts": {"controlled": 0, "not_controlled": 0, "unknown": 0},
                "example_template_controlled": None
            }

        entry = param_index[pk]
        st = pv.get("storage")
        q = pv.get("q") or "unreadable"

        # Determine whether this parameter is controlled by the template (Include checkbox)
        pid_int = _safe(lambda: p.Id.IntegerValue, None)
        if pid_int is None:
            ctrl_state = "unknown"
        else:
            # If pid is in non-controlled set, template does NOT control it
            ctrl_state = "not_controlled" if (pid_int in non_ctrl_ints) else "controlled"

        if ctrl_state not in entry["template_control_counts"]:
            entry["template_control_counts"][ctrl_state] = 0
        entry["template_control_counts"][ctrl_state] += 1

        # Prefer a known example (controlled/not_controlled) over unknown
        if entry["example_template_controlled"] is None or entry["example_template_controlled"] == "unknown":
            entry["example_template_controlled"] = ctrl_state


        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1

        entry["observed_on_viewtype_buckets"].add(vt_bucket)
        _maybe_set_example(entry, pv)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "view_templates",
        "param_key": pk,
        "selected_template_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "template_control_counts": e.get("template_control_counts"),
            "template_control_example": e.get("example_template_controlled"),
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_viewtype_buckets"]))[:25]
        }
    })


# -------------------------
# Optional Crosswalk: View (non-template) -> ViewTemplate
# -------------------------

optional_crosswalk = []

# Build template name lookup
template_name_by_id = {}
for t in templates:
    tid = _safe(lambda: t.Id.IntegerValue, None)
    if tid is not None and tid not in template_name_by_id:
        template_name_by_id[tid] = _safe_view_name(t)

if enable_crosswalk:
    # Keep crosswalk compact: one representative non-template view per distinct template id
    seen_template_ids = set()

    for v in all_views:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        is_t = _safe(lambda: v.IsTemplate, False)
        if is_t:
            continue

        tid = _safe(lambda: v.ViewTemplateId, None)
        if tid is None or tid == ElementId.InvalidElementId:
            continue

        tid_int = _safe(lambda: tid.IntegerValue, None)
        if tid_int is None:
            continue
        if tid_int in seen_template_ids:
            continue

        # Resolve template
        tname = template_name_by_id.get(tid_int)
        if tname is None:
            ref = _safe(lambda: doc.GetElement(tid), None)
            tname = _safe(lambda: _safe_view_name(ref), None) if ref is not None else None

        row = {
            "view.id": _safe(lambda: v.Id.IntegerValue, None),
            "view.name": _safe(lambda: _safe_view_name(v), None),
            "view.view_type": _viewtype_bucket(v),
            "template.id": tid_int,
            "template.name": tname,
            "template.resolved": True if tname is not None else False
        }

        # Keep only resolved mappings (signal > noise)
        if not row["template.resolved"]:
            continue

        seen_template_ids.add(tid_int)
        optional_crosswalk.append(row)


# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "inventory",
        "domain": "view_templates",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "view_templates",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
file_written = None
write_error = None

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
        fixed_name = "probe_view_templates_{}.json".format(date_stamp)

        # IN[4] is treated as an output directory (not a filename)
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

# tools/probes/probe_phase_filters.py
#
# Dynamo Python (Revit) — Breadth Probe: phase_filters (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "phase_filters",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "phase_filters",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_phase_filters_to_inspect (int)
#        Maximum number of PhaseFilters to inspect.
#        Default: 200
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit View → PhaseFilter crosswalk.
#        Default: False
#
#   IN[2] max_views_to_scan (int)
#        When crosswalk enabled, scan at most N views for Phase Filter assignments.
#        Default: 2000
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_phase_filters_YYYY-MM-DD.json
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

# PhaseFilter / PhaseStatus are present in common Revit builds,
# but import defensively for Dynamo environments.
try:
    from Autodesk.Revit.DB import PhaseFilter
except:
    PhaseFilter = None

try:
    from Autodesk.Revit.DB import PhaseStatus
except:
    PhaseStatus = None

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_phase_filters_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
max_views_to_scan = IN[2] if len(IN) > 2 and IN[2] is not None else 2000
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

def _safe_elem_name(elem):
    # Prefer Revit's Name property where present.
    try:
        n = elem.Name
        if n:
            return n
    except:
        pass
    # Fall back to common type-name params if available
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
      - Integer.norm stays integer (enum-safe; do NOT coerce to bool)
      - Length -> inches (float) when datatype is Length
      - Angle  -> degrees (float) when datatype is Angle
      - ElementId -> IntegerValue (norm=int), display tries to resolve element name cheaply
    """
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

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}


# -------------------------
# Domain-specific breadth bucket: Phase status presentations
# -------------------------

def _phase_status_bucket(pf):
    if pf is None:
        return "unsupported|pf"

    parts = []
    for status_name in STATUS_ORDER:
        status_enum = _status_enum(status_name)
        if status_enum is None:
            parts.append("{}=?".format(status_name))
            continue

        try:
            pres = pf.GetPhaseStatusPresentation(status_enum)
            token = str(pres)
            token_to_label = {
                # Enum token forms
                "ByCategory": "By Category",
                "NotDisplayed": "Not Displayed",
                "Overridden": "Overridden",

                # Numeric token forms observed in some builds (confirmed by user)
                "0": "By Category",
                "1": "Not Displayed",
                "2": "Overridden",

                # Defensive: if int slips through before str()
                0: "By Category",
                1: "Not Displayed",
                2: "Overridden",
            }
            label = token_to_label.get(token, token)
            parts.append("{}={}".format(status_name, label))
        except:
            parts.append("{}=?".format(status_name))

    return "|".join(parts)

# -------------------------
# Discovery (progressive)
# -------------------------

discovery_notes = []

phase_filters = []

# Step 1 (preferred): class-based collector (category-free)
if PhaseFilter is not None:
    phase_filters = _safe(
        lambda: (FilteredElementCollector(doc)
                 .OfClass(PhaseFilter)
                 .ToElements()),
        default=[]
    )
    discovery_notes.append("collector: OfClass(PhaseFilter)")
else:
    discovery_notes.append("collector: PhaseFilter class import unavailable")

try:
    phase_filters = list(phase_filters)
except:
    phase_filters = list(phase_filters)

# Cap scan explicitly
try:
    nmax = int(max_phase_filters_to_inspect)
    if nmax >= 0:
        phase_filters = phase_filters[:nmax]
except:
    pass


# -------------------------
# Build inventory (union over discovered phase filters)
# -------------------------

# Inventory policy for this domain:
# - PhaseFilter often exposes few/no "Parameters"; the meaningful surface is the
#   per-status presentation setting used by the exporter (GetPhaseStatusPresentation).
# - Therefore we synthesize "probe parameters" aligned to exporter identity items:
#     phase_filter.<status>.presentation_id  (Integer)
#   plus a coordination/name item:
#     phase_filter.name  (String)
#
# We still attempt to include any actual Revit Parameters found on PhaseFilter,
# but those are additive-only and not relied upon for non-empty inventory.

STATUS_ORDER = ["New", "Existing", "Demolished", "Temporary"]

def _status_enum(status_name):
    if PhaseStatus is not None:
        return _safe(lambda: getattr(PhaseStatus, status_name), None)
    # Exporter uses ElementOnPhaseStatus; probe may not have it imported.
    try:
        from Autodesk.Revit.DB import ElementOnPhaseStatus
        return _safe(lambda: getattr(ElementOnPhaseStatus, status_name), None)
    except:
        return None

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

def _add_inventory_obs(param_key, pv, pf_name=None, bucket=None):
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set(),
            "observed_on_names": set()
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
    if pf_name:
        entry["observed_on_names"].add(pf_name)

    _maybe_set_example(entry, pv)


# param_key -> accumulator (same shape as before)
param_index = {}

for pf in phase_filters:
    pf_name = _safe(lambda: _safe_elem_name(pf), None)
    bucket = _phase_status_bucket(pf)

    # --- Synthesized, exporter-modeled surfaces (authoritative) ---
    # phase_filter.name
    name_val = _safe(lambda: getattr(pf, "Name", None), None)
    pv_name = {
        "q": "ok" if (name_val is not None and str(name_val) != "") else "missing",
        "storage": "String",
        "raw": name_val,
        "display": name_val,
        "norm": name_val
    }
    _add_inventory_obs("phase_filter.name", pv_name, pf_name=pf_name, bucket=bucket)

    # phase_filter.<status>.presentation_id (Integer)
for status_name in STATUS_ORDER:
    status_enum = _status_enum(status_name)
    k = "phase_filter.{}.presentation_id".format(status_name.lower())

    try:
        if status_enum is None:
            raise Exception("status enum unavailable")

        pres = pf.GetPhaseStatusPresentation(status_enum)
        token = str(pres)

        token_to_label = {
            # Enum token forms
            "ByCategory": "By Category",
            "NotDisplayed": "Not Displayed",
            "Overridden": "Overridden",

            # Numeric token forms observed in some builds (confirmed by user)
            "0": "By Category",
            "1": "Not Displayed",
            "2": "Overridden",

            # Defensive: if int slips through before str()
            0: "By Category",
            1: "Not Displayed",
            2: "Overridden",
        }

        label = token_to_label.get(token, token)

        pv = {
            "q": "ok",
            "storage": "String",
            "raw": label,
            "display": label,
            "norm": label
        }
    except:
        pv = {
            "q": "unreadable",
            "storage": "String",
            "raw": None,
            "display": None,
            "norm": None
        }

    _add_inventory_obs(k, pv, pf_name=pf_name, bucket=bucket)
    # --- Additive-only: actual Revit Parameters (if any) ---
    params = _safe(lambda: list(pf.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(pf.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)
        pv = _format_param_contract(p)
        _add_inventory_obs(pk, pv, pf_name=pf_name, bucket=bucket)


# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "phase_filters",
        "param_key": pk,
        "selected_phase_filter_sample_count": len(phase_filters),
        "discovery": {
            "notes": discovery_notes[:10],
            "modeled_on_exporter": True if pk.startswith("phase_filter.") else False
        },
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            # breadth: cap for readability
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25],
            "observed_on_names": sorted(list(e["observed_on_names"]))[:25]
        }
    })

# -------------------------
# Optional Crosswalk: View -> PhaseFilter
# -------------------------

optional_crosswalk = []

VIEW_PHASE_FILTER_PARAM_CANDIDATES = [
    # UI-facing label (common)
    "Phase Filter",
    "Phase filter",
]

def _get_view_phase_filter_param(v):
    # Prefer BIP if present; fall back to name candidates.
    # Some builds expose the view setting via a built-in parameter.
    for bip in (
        _safe(lambda: BuiltInParameter.VIEW_PHASE_FILTER, None),
    ):
        if bip is None:
            continue
        p = _safe(lambda: v.get_Parameter(bip), None)
        if p is not None:
            return (str(bip), p)

    for cand in VIEW_PHASE_FILTER_PARAM_CANDIDATES:
        p = _safe(lambda: v.LookupParameter(cand), None)
        if p is not None:
            return (cand, p)

    return (None, None)

phase_filter_name_by_id = {}
for pf in phase_filters:
    pid = _safe(lambda: pf.Id.IntegerValue, None)
    if pid is not None and pid not in phase_filter_name_by_id:
        phase_filter_name_by_id[pid] = _safe(lambda: _safe_elem_name(pf), None)

if enable_crosswalk:
    views = _safe(
        lambda: (FilteredElementCollector(doc)
                 .OfClass(View)
                 .ToElements()),
        default=[]
    )
    try:
        views = list(views)
    except:
        views = list(views)

    # Limit scan explicitly (avoid whole-model view scan on huge files)
    try:
        vcap = int(max_views_to_scan)
        if vcap >= 0:
            views = views[:vcap]
    except:
        pass

    # Keep crosswalk compact: one representative view per distinct phase_filter_id
    seen_pf_ids = set()

    for v in views:
        if v is None:
            continue
        # Skip view templates if easily detectable (older builds may differ)
        is_template = _safe(lambda: v.IsTemplate, False)
        if is_template:
            continue

        matched_name, p = _get_view_phase_filter_param(v)
        pv = _format_param_contract(p)

        # keep only ElementId payloads with a value
        if pv.get("storage") != "ElementId" or pv.get("raw") is None:
            continue

        pf_id = int(pv.get("raw"))
        if pf_id in seen_pf_ids:
            continue

        row = {
            "view.id": _safe(lambda: v.Id.IntegerValue, None),
            "view.name": _safe(lambda: v.Name, None),
            "phase_filter_param.matched_name": matched_name,
            "phase_filter_param": pv,
            "phase_filter.resolved": False,
            "phase_filter.id": pf_id,
            "phase_filter.name": phase_filter_name_by_id.get(pf_id)
        }

        if row["phase_filter.name"] is None:
            ref = _safe(lambda: doc.GetElement(ElementId(pf_id)), None)
            row["phase_filter.name"] = _safe(lambda: _safe_elem_name(ref), None) if ref is not None else None

        row["phase_filter.resolved"] = True if row["phase_filter.name"] is not None else False

        if not row["phase_filter.resolved"]:
            continue

        seen_pf_ids.add(pf_id)
        optional_crosswalk.append(row)

# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "inventory",
        "domain": "phase_filters",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "phase_filters",
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
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_phase_filters_{}.json".format(date_stamp)

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

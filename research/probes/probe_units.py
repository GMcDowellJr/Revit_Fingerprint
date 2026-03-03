# Dynamo Python (Revit) — Breadth Probe: units (INVENTORY OUTPUT)
#
# Objective:
#   Evidence-capture of Project Units surface area (discipline + per-spec format options).
#   This is NOT production export logic. It is exploratory inventory for future policy design.
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "units",
#     "records": [...],
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "units",
#     "records": []
#   }
# ]
#
# Inputs:
#   IN[0] max_specs_to_inspect (int)
#        Max number of SpecTypeId specs to attempt (after discovery).
#        Default: 200
#
#   IN[1] per_discipline_limit (int|None)
#        Max specs to attempt per discovered discipline bucket (breadth bias).
#        Default: 50
#
#   IN[2] write_json (bool)
#        When True, serialize OUT to JSON on disk.
#        Default: False
#
#   IN[3] output_directory (str|None)
#        Directory where JSON will be written; filename fixed:
#        probe_units_YYYY-MM-DD.json
#
# Notes:
#   - Discovery is category-free: reflects SpecTypeId public members to gather ForgeTypeId specs.def _unitutils_get_dis
#   - Discipline resolution is best-effort via UnitUtils (guarded); may be unavailable in some versions.
#   - FormatOptions property availability can vary by spec; q-state captures that.
#

import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import UnitUtils, LabelUtils

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_specs_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
per_discipline_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 50
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

def _try(fn):
    try:
        return (fn(), None)
    except Exception as ex:
        return (None, "{}: {}".format(type(ex).__name__, ex))

def _safe_str(x):
    try:
        return str(x) if x is not None else None
    except:
        return None

def _is_forge_type_id(x):
    if x is None:
        return False
    try:
        tn = x.GetType().FullName
    except:
        tn = _safe(lambda: type(x).__name__, None)
    if not tn:
        return False
    return ("ForgeTypeId" in tn)

def _forge_id_string(ftid):
    # Prefer TypeId string when available; else ToString().
    if ftid is None:
        return None
    s = _safe(lambda: ftid.TypeId, None)
    if s:
        return s
    return _safe_str(ftid)

def _pv_missing():
    return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}

def _pv_unreadable(msg):
    return {"q": "unreadable", "storage": "String", "raw": msg, "display": msg, "norm": msg}

def _pv_unsupported(storage_label):
    return {"q": "unsupported", "storage": storage_label, "raw": None, "display": None, "norm": None}

def _pv_from_string(s, q="ok"):
    return {"q": q, "storage": "String", "raw": s, "display": s, "norm": s}

def _pv_from_int(i, display=None, q="ok"):
    disp = display if display is not None else (_safe_str(i) if i is not None else None)
    return {"q": q, "storage": "Integer", "raw": i, "display": disp, "norm": i}

def _pv_from_double(d, display=None, q="ok"):
    disp = display if display is not None else (_safe_str(d) if d is not None else None)
    return {"q": q, "storage": "Double", "raw": d, "display": disp, "norm": d}

def _pv_from_forge_type_id(ftid, q="ok"):
    s = _forge_id_string(ftid)
    if s is None and q == "ok":
        return _pv_missing()
    return {"q": q, "storage": "String", "raw": s, "display": s, "norm": s}

def _pv_from_bool(b, q="ok"):
    # Keep as Integer 0/1 (probe convention: do not coerce ints to bool; UI bools still comparable as 0/1).
    if b is None and q == "ok":
        return _pv_missing()
    i = 1 if b else 0
    return _pv_from_int(i, display=("True" if b else "False"), q=q)

def _pv_from_enum(e, q="ok"):
    # Store enum as Integer when possible; display keeps string name.
    if e is None and q == "ok":
        return _pv_missing()
    raw_i = _safe(lambda: int(e), None)
    disp = _safe_str(e)
    return {"q": q, "storage": "Integer", "raw": raw_i, "display": disp, "norm": raw_i}

def _unitutils_get_discipline_id(spec_id):
    # Best-effort; API name varies by version.
    # Return string token or None.
    if spec_id is None:
        return None
    for fn_name in ("GetDisciplineId", "GetDiscipline", "GetSpecDisciplineId"):
        fn = _safe(lambda: getattr(UnitUtils, fn_name), None)
        if fn is None:
            continue
        v, err = _try(lambda: fn(spec_id))
        if err is None and v is not None:
            if _is_forge_type_id(v):
                return _forge_id_string(v)
            return _safe_str(v)
    return None

def _label_for_spec_id(spec_id):
    """
    Best-effort UI label for the Project Units 'Units' column.
    API surface varies by Revit version; try multiple entry points.
    """
    if spec_id is None:
        return (None, "missing")

    # Common in newer APIs
    for fn_name in ("GetLabelForSpec", "GetLabelFor"):
        fn = _safe(lambda: getattr(LabelUtils, fn_name), None)
        if fn is None:
            continue
        s, err = _try(lambda: fn(spec_id))
        if err is None and s:
            return (s, None)

    return (None, "unreadable:LabelUtils has no compatible spec label method")


def _label_for_discipline_id(discipline_id):
    """
    Best-effort UI label for the Project Units 'Discipline' dropdown entry.
    """
    if discipline_id is None:
        return (None, "missing")

    for fn_name in ("GetLabelForDiscipline", "GetLabelFor"):
        fn = _safe(lambda: getattr(LabelUtils, fn_name), None)
        if fn is None:
            continue
        s, err = _try(lambda: fn(discipline_id))
        if err is None and s:
            return (s, None)

    return (None, "unreadable:LabelUtils has no compatible discipline label method")

def _units_get_format_options(units_obj, spec_id):
    if units_obj is None or spec_id is None:
        return (None, "missing")
    fmt, err = _try(lambda: units_obj.GetFormatOptions(spec_id))
    if err is not None:
        return (None, "unreadable:{}".format(err))
    if fmt is None:
        return (None, "missing")
    return (fmt, "ok")


# -------------------------
# Schema-driven surface
# -------------------------

# FormatOptions surface to probe.
# Each entry defines:
#   param_key suffix, getter kind, and how to convert to contract pv.
FORMAT_SURFACE = [
    # Methods returning ForgeTypeId
    ("format.unit_type_id", "method_forge", "GetUnitTypeId"),
    ("format.symbol_type_id", "method_forge", "GetSymbolTypeId"),

    # Common properties (varies by spec/version)
    ("format.accuracy", "prop_double", "Accuracy"),
    ("format.rounding_method", "prop_enum", "RoundingMethod"),

    ("format.use_default", "prop_bool", "UseDefault"),
    ("format.use_digit_grouping", "prop_bool", "UseDigitGrouping"),
    ("format.use_plus_prefix", "prop_bool", "UsePlusPrefix"),

    ("format.suppress_leading_zeros", "prop_bool", "SuppressLeadingZeros"),
    ("format.suppress_trailing_zeros", "prop_bool", "SuppressTrailingZeros"),
    ("format.suppress_spaces", "prop_bool", "SuppressSpaces"),
    ("format.suppress_unit_symbol", "prop_bool", "SuppressUnitSymbol"),
]

# Units global surface (UI "Decimal symbol/digit grouping" area)
UNITS_GLOBAL_SURFACE = [
    ("units.decimal_symbol", "prop_any", "DecimalSymbol"),
    ("units.digit_grouping_amount", "prop_any", "DigitGroupingAmount"),
    ("units.digit_grouping_symbol", "prop_any", "DigitGroupingSymbol"),
]


def _pv_from_prop_any(obj, prop_name):
    if obj is None:
        return _pv_missing()
    v, err = _try(lambda: getattr(obj, prop_name))
    if err is not None:
        return _pv_unreadable(err)
    if v is None:
        return _pv_missing()
    if isinstance(v, bool):
        return _pv_from_bool(v)
    if isinstance(v, int):
        return _pv_from_int(v)
    if isinstance(v, float):
        return _pv_from_double(v)
    if _is_forge_type_id(v):
        return _pv_from_forge_type_id(v)
    return _pv_from_string(_safe_str(v))


def _pv_from_format_surface(fmt, kind, accessor):
    if fmt is None:
        return _pv_missing()

    if kind == "method_forge":
        fn = _safe(lambda: getattr(fmt, accessor), None)
        if fn is None:
            return _pv_missing()
        v, err = _try(lambda: fn())
        if err is not None:
            return _pv_unreadable(err)
        if v is None:
            return _pv_missing()
        if _is_forge_type_id(v):
            return _pv_from_forge_type_id(v)
        return _pv_from_string(_safe_str(v))

    if kind == "prop_double":
        v, err = _try(lambda: getattr(fmt, accessor))
        if err is not None:
            return _pv_unreadable(err)
        if v is None:
            return _pv_missing()
        try:
            return _pv_from_double(float(v))
        except:
            return _pv_from_string(_safe_str(v))

    if kind == "prop_bool":
        v, err = _try(lambda: getattr(fmt, accessor))
        if err is not None:
            return _pv_unreadable(err)
        if v is None:
            return _pv_missing()
        try:
            return _pv_from_bool(bool(v))
        except:
            return _pv_from_string(_safe_str(v))

    if kind == "prop_enum":
        v, err = _try(lambda: getattr(fmt, accessor))
        if err is not None:
            return _pv_unreadable(err)
        if v is None:
            return _pv_missing()
        return _pv_from_enum(v)

    return _pv_unsupported(_safe_str(kind))


# -------------------------
# Spec discovery (category-free)
# -------------------------

def _discover_specs():
    """
    Returns: list of tuples (spec_label, spec_id, spec_id_string)
    """
    if SpecTypeId is None:
        return []

    found = []
    seen = set()

    # Reflect SpecTypeId members; keep only ForgeTypeId that look like specs.
    names = _safe(lambda: dir(SpecTypeId), default=[])
    for n in names:
        if not n or n.startswith("_"):
            continue
        v = _safe(lambda: getattr(SpecTypeId, n), None)
        if v is None:
            continue
        if not _is_forge_type_id(v):
            continue
        sid = _forge_id_string(v)
        if not sid or "autodesk.spec" not in sid:
            continue
        if sid in seen:
            continue
        seen.add(sid)
        found.append((n, v, sid))

    # Stable order: by spec_id string
    found = sorted(found, key=lambda t: t[2] or "")

    # Hard fallback subset (keeps probe usable if reflection yields nothing)
    if len(found) == 0:
        fallback = []
        for n in ("Angle", "Area", "Length", "Number", "Slope", "Volume"):
            v = _safe(lambda: getattr(SpecTypeId, n), None)
            if v is None or not _is_forge_type_id(v):
                continue
            sid = _forge_id_string(v)
            if not sid:
                continue
            if sid in seen:
                continue
            seen.add(sid)
            fallback.append((n, v, sid))
        found = sorted(fallback, key=lambda t: t[2] or "")

    return found


# -------------------------
# Inventory aggregation
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict|None,
#   observed_on_disciplines: set(str),
#   observed_on_specs: set(str)
# }
param_index = {}

def _maybe_set_example(entry, pv):
    if entry.get("example") is not None:
        return
    if pv is None:
        return
    q = pv.get("q")
    # Prefer an ok example; else first non-null.
    if q == "ok":
        entry["example"] = pv
        return
    if entry.get("example") is None:
        entry["example"] = pv

def _touch_param(param_key, pv, discipline_key, spec_key):
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_disciplines": set(),
            "observed_on_specs": set(),
        }

    entry = param_index[param_key]

    st = pv.get("storage")
    q = pv.get("q") or "unreadable"

    if st:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    if discipline_key:
        entry["observed_on_disciplines"].add(discipline_key)
    if spec_key:
        entry["observed_on_specs"].add(spec_key)

    _maybe_set_example(entry, pv)


# -------------------------
# Run probe
# -------------------------

u = _safe(lambda: doc.GetUnits(), None)

specs = _discover_specs()

# Cap after discovery to avoid huge enumerations
try:
    max_n = int(max_specs_to_inspect)
    if max_n >= 0 and len(specs) > max_n:
        specs = specs[:max_n]
except:
    pass

# Breadth-bias sampling: enforce per-discipline cap if possible
selected_specs = []
by_disc = {}  # discipline_key -> count

# Crosswalk: one row per selected spec (dedupe by spec_sid)
crosswalk_records = []
_crosswalk_seen = set()

for (label, spec_id, spec_sid) in specs:
    disc_id = _unitutils_get_discipline_id(spec_id) or "(unknown)"
    c = by_disc.get(disc_id, 0)

    allow = True
    if per_discipline_limit is not None:
        try:
            allow = c < int(per_discipline_limit)
        except:
            allow = c < 50

    if not allow:
        continue

    # UI labels (best-effort)
    spec_ui, spec_ui_err = _label_for_spec_id(spec_id)
    disc_ui = None
    disc_ui_err = None

    # Only attempt discipline label if discipline looks like a ForgeTypeId string, not "(unknown)"
    if disc_id and disc_id != "(unknown)":
        # discipline id is a string; we cannot pass it back into LabelUtils without a ForgeTypeId object
        # so we record only the id string here unless we can re-derive a discipline ForgeTypeId elsewhere.
        disc_ui_err = "missing:discipline ForgeTypeId object not available"
    else:
        disc_ui_err = "missing"

    selected_specs.append((label, spec_id, spec_sid, disc_id))

    by_disc[disc_id] = c + 1

    if spec_sid and spec_sid not in _crosswalk_seen:
        _crosswalk_seen.add(spec_sid)
        crosswalk_records.append({
            "spec.id": spec_sid,
            "spec.member_name": label,
            "spec.ui_label": spec_ui,
            "spec.ui_label_q": "ok" if (spec_ui_err is None and spec_ui) else ("missing" if spec_ui_err == "missing" else "unreadable"),
            "spec.ui_label_err": None if (spec_ui_err is None) else spec_ui_err,
            "discipline.id": disc_id,
            "discipline.ui_label": disc_ui,
            "discipline.ui_label_q": "ok" if (disc_ui_err is None and disc_ui) else ("missing" if disc_ui_err and disc_ui_err.startswith("missing") else "unreadable"),
            "discipline.ui_label_err": None if (disc_ui_err is None) else disc_ui_err
        })

# Global Units surface (once)
for (suffix, kind, accessor) in UNITS_GLOBAL_SURFACE:
    pk = "p.{}".format(suffix)
    pv = _pv_from_prop_any(u, accessor)
    _touch_param(pk, pv, "(global)", "(global)")

# Per-spec FormatOptions surface
for (label, spec_id, spec_sid, disc) in selected_specs:
    fmt, fmt_q = _units_get_format_options(u, spec_id)

    # Spec diagnostics (useful when format options aren't reachable)
    _touch_param(
        "p.debug.format_options_status",
        _pv_from_string(fmt_q, q="ok" if (fmt_q == "ok") else "unreadable"),
        disc,
        spec_sid
    )

    for (suffix, kind, accessor) in FORMAT_SURFACE:
        pk = "p.{}".format(suffix)
        if fmt is None:
            if fmt_q.startswith("unreadable:"):
                pv = _pv_unreadable(fmt_q[len("unreadable:"):])
            else:
                pv = _pv_missing()
        else:
            pv = _pv_from_format_surface(fmt, kind, accessor)
        _touch_param(pk, pv, disc, spec_sid)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "units",
        "param_key": pk,
        "spec_sample_count": len(selected_specs),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_disciplines": sorted(list(e["observed_on_disciplines"]))[:50],
            "observed_on_specs": sorted(list(e["observed_on_specs"]))[:50],
        }
    })

optional_crosswalk = crosswalk_records

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "units",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "units",
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
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_units_{}.json".format(date_stamp)

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

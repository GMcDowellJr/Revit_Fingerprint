# Dynamo Python (Revit) — Breadth Probe: arrowheads (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "arrowheads",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "arrowheads",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_arrowheads_to_inspect (int)
#        Maximum number of arrowhead ElementTypes to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit DimensionType → Arrowhead crosswalk.
#        Default: False
#
#   IN[2] per_style_limit (int)
#        Sample at most N arrowhead types per Arrow Style value
#        (set large to effectively scan all).
#        Default: 2
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_arrowheads_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.


import clr
import hashlib
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

doc = DocumentManager.Instance.CurrentDBDocument

max_arrowheads_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_style_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 2
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
      "<k>": {
        "q": "ok|missing|unreadable|unsupported",
        "storage": "String|Integer|Double|ElementId|None",
        "raw": ...,
        "display": ...,
        "norm": ...
      }

    Probe choice (important):
      - Integer.norm stays as raw int (do NOT coerce 0/1 to bool),
        because many ints are enums (e.g. Arrow Style).
      - Length -> inches (float) when datatype is Length
      - Angle  -> degrees (float) when datatype is Angle
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

    if st == StorageType.Integer:
        raw = _safe(lambda: p.AsInteger(), None)
        disp = _fmt_display(p, None)
        # keep norm as integer (enum-safe)
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

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _looks_like_arrowhead_type(t):
    # Heuristic: must have the canonical arrowhead params
    required = ["Arrow Style", "Tick Size"]
    optional = ["Fill Tick", "Arrow Closed", "Arrow Width Angle", "Heavy End Pen Weight", "Tick Mark Centered"]
    try:
        for pn in required:
            if t.LookupParameter(pn) is None:
                return False
        for pn in optional:
            if t.LookupParameter(pn) is not None:
                return True
        return False
    except:
        return False

def _arrow_style_key(t):
    # group sampling by Arrow Style (raw int + display label)
    p = _safe(lambda: t.LookupParameter("Arrow Style"), None)
    if p is None:
        return ("missing", None)
    pv = _format_param_contract(p)
    raw = pv.get("raw")
    disp = pv.get("display")
    return ("{}|{}".format(raw, disp), pv)


# -------------------------
# Discovery + Sampling
# -------------------------

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
    all_types = list(all_types)

hits = []
for t in all_types:
    if _looks_like_arrowhead_type(t):
        hits.append(t)

# Cap AFTER filtering so collector ordering can't hide arrowheads
try:
    max_n = int(max_arrowheads_to_inspect)
    if max_n >= 0:
        hits = hits[:max_n]
except:
    pass

hits = []
for t in all_types:
    if _looks_like_arrowhead_type(t):
        hits.append(t)

# Sample first N per Arrow Style (breadth bias)
selected = []
by_style = {}  # style_key -> count
for t in hits:
    sk, _ = _arrow_style_key(t)
    c = by_style.get(sk, 0)
    if per_style_limit is None:
        per_style_ok = True
    else:
        try:
            per_style_ok = c < int(per_style_limit)
        except:
            per_style_ok = c < 2
    if per_style_ok:
        selected.append(t)
        by_style[sk] = c + 1

# If per_style_limit is 0 or negative, fallback to at least 1 per style
if len(selected) == 0 and len(hits) > 0:
    seen = set()
    for t in hits:
        sk, _ = _arrow_style_key(t)
        if sk not in seen:
            selected.append(t)
            seen.add(sk)


# -------------------------
# Build inventory (union over selected)
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   observed_on_style_keys: set(str)
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
    # upgrade existing non-ok example to ok if we see one
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }

def _push_example(bucket, pv):
    # keep up to 5 distinct examples by (storage, norm, display)
    if pv is None:
        return
    sig = (str(pv.get("storage")), str(pv.get("norm")), str(pv.get("display")))
    for ex in bucket:
        exsig = (str(ex.get("storage")), str(ex.get("norm")), str(ex.get("display")))
        if exsig == sig:
            return
    if len(bucket) < 5:
        bucket.append({
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        })

for t in selected:
    style_key, style_pv = _arrow_style_key(t)

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
                "example": None,
                "observed_on_style_keys": set()
            }

        entry = param_index[pk]

        st = pv.get("storage")
        q = pv.get("q") or "unreadable"

        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1

        entry["observed_on_style_keys"].add(style_key)
        _maybe_set_example(entry, pv)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "arrowheads",
        "param_key": pk,
        "selected_type_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_styles": sorted(list(e["observed_on_style_keys"]))[:25]
        }
    })



# -------------------------
# Optional Crosswalk: DimensionType -> Tick Mark (Arrowhead)
# -------------------------

optional_crosswalk = []

DIM_TICK_PARAM_CANDIDATES = [
    "Tick Mark",
    "Tick mark",
    "Tick Mark Type",
    "Tick Mark Symbol",
]

def _collect_dimension_types_with_tick_param():
    candidates = set([n.strip().lower() for n in DIM_TICK_PARAM_CANDIDATES])
    hits_local = []
    for t in all_types:
        try:
            params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
            if params is None:
                params = _safe(lambda: list(t.Parameters), default=[])
            for p in params:
                dn = _safe(lambda: _safe_param_def_name(p), None)
                if dn and dn.strip().lower() in candidates:
                    hits_local.append(t)
                    break
        except:
            continue
    return hits_local

# arrowhead name lookup (from all hits, not just selected)
arrowhead_name_by_id = {}
for t in hits:
    tid = _safe(lambda: t.Id.IntegerValue, None)
    if tid is not None and tid not in arrowhead_name_by_id:
        arrowhead_name_by_id[tid] = _safe_type_name(t)

if enable_crosswalk:
    # Optional extra input: max crosswalk rows to emit (default 25)
    crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 25

    dim_types = _collect_dimension_types_with_tick_param()

    # Keep crosswalk compact: one representative DimensionType per distinct Arrowhead type_id
    seen_arrowhead_ids = set()

    for dt in dim_types:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        row = {
            "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
            "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
            "tick_param.matched_name": None,
            "tick_param": None,
            "arrowhead.resolved": False,
            "arrowhead.type_id": None,
            "arrowhead.name": None
        }

        p = None
        matched = None
        for cand in DIM_TICK_PARAM_CANDIDATES:
            p = _safe(lambda: dt.LookupParameter(cand), None)
            if p is not None:
                matched = cand
                break

        row["tick_param.matched_name"] = matched
        row["tick_param"] = _format_param_contract(p)

        # Only keep rows that resolve to an Arrowhead ElementId
        if row["tick_param"]["storage"] != "ElementId" or row["tick_param"]["raw"] is None:
            continue

        ah_id = int(row["tick_param"]["raw"])
        if ah_id in seen_arrowhead_ids:
            continue

        row["arrowhead.type_id"] = ah_id
        row["arrowhead.name"] = arrowhead_name_by_id.get(ah_id)

        if row["arrowhead.name"] is None:
            ref = _safe(lambda: doc.GetElement(ElementId(ah_id)), None)
            row["arrowhead.name"] = _safe(lambda: ref.Name, None) if ref is not None else None

        row["arrowhead.resolved"] = True if row["arrowhead.name"] is not None else False

        # Keep only resolved mappings (signal > noise)
        if not row["arrowhead.resolved"]:
            continue

        seen_arrowhead_ids.add(ah_id)
        optional_crosswalk.append(row)

# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "inventory",
        "domain": "arrowheads",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "arrowheads",
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
        fixed_name = "probe_arrowheads_{}.json".format(date_stamp)

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

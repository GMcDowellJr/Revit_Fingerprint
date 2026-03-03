# Dynamo Python (Revit) — Breadth Probe: phases
#
# Output contract (exporter-mode):
# [
#   { "domain": "phases", "kind": "inventory", "records": [...] },
#   { "domain": "phases", "kind": "crosswalk", "records": [...] }
# ]
#
# Inputs:
#   IN[0] max_phases_to_inspect (int)
#        Maximum number of Phase elements to inspect after discovery.
#        Default: 200
#
#   IN[1] include_phase_parameters (bool)
#        When True, include Phase element parameters (p.*) in inventory.
#        When False, only synthetic exporter-aligned fields are captured.
#        Default: True
#
#   IN[2] output_directory (str)
#        Folder path where the probe JSON artifact will be written.
#        If None:
#          1) Uses the active RVT file directory (if available)
#          2) Falls back to TEMP / TMP
#          3) Falls back to current working directory
#
#   IN[3] write_json (bool)
#        When True, writes a JSON artifact named:
#          probe_phases_YYYY-MM-DD.json
#        When False, no file is written (OUT is still returned).
#        Default: True
#
# Notes:
#   - Modeled after exporter domain behavior: phases are global, order matters, and name+sequence are the main
#     cross-project signature candidates; UniqueId is document-scoped evidence only.
#   - Discovery prefers doc.Phases (preserves document ordering), with collector fallback.
#   - Crosswalk is empty here (no natural relationship validated in this probe).

import clr

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

import os
import json
from datetime import datetime

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    Phase
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_phases_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
include_phase_parameters = IN[1] if len(IN) > 1 and IN[1] is not None else True

# JSON write controls
# IN[2] output_directory (str) — target folder for JSON
# IN[3] write_json (bool) — enable/disable file write
output_directory = IN[2] if len(IN) > 2 and IN[2] is not None else None
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else True


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

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
      - ElementId -> IntegerValue (norm=int) and display tries doc element name
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
            ref_name = _safe(lambda: ref.Name, None)

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _phase_key(ph, idx):
    # Exporter-mode breadth indicator: stable order key
    name = _safe(lambda: ph.Name, None)
    uid = _safe(lambda: ph.UniqueId, None)
    seq = _safe(lambda: getattr(ph, "SequenceNumber", None), None)
    if seq is None:
        seq = idx + 1  # stable fallback based on doc order (matches exporter intent)
    return "seq={}|name={}|uid={}".format(
        seq if seq is not None else "?",
        name if name is not None else "?",
        uid if uid is not None else "?"
    )

def _synthetic_value_contract(q, storage, raw, display, norm):
    return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}

def _inv_init():
    return {
        "storage_types": set(),
        "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
        "example": None,
        "observed_on_phase_keys": set(),
        "observed_values_seen": set(),  # probe-local dedupe key set
    }

def _inv_add(inv, param_key, pv, phase_key):
    if param_key not in inv:
        inv[param_key] = _inv_init()

    e = inv[param_key]
    q = pv.get("q") or "unreadable"
    st = pv.get("storage")

    # Dedup rule (probe-local): (param_key, storage_type, normalized_value)
    dedupe_norm = pv.get("norm")
    dedupe_key = "{}|{}|{}".format(param_key, st if st is not None else "None", dedupe_norm)
    if dedupe_key in e["observed_values_seen"]:
        # still count the q state and phase occurrence (evidence), but don't affect example choice
        pass
    else:
        e["observed_values_seen"].add(dedupe_key)

    if st:
        e["storage_types"].add(st)
    if q not in e["q_counts"]:
        e["q_counts"][q] = 0
    e["q_counts"][q] += 1

    if phase_key:
        e["observed_on_phase_keys"].add(phase_key)

    # exactly one example total per parameter: prefer first ok encountered
    ex = e.get("example")
    if ex is None or (ex.get("q") != "ok" and q == "ok"):
        e["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }

# -------------------------
# Discovery (exporter-mode)
# -------------------------

phases = _safe(lambda: list(doc.Phases), default=None)

if phases is None:
    phases = _safe(
        lambda: list(
            FilteredElementCollector(doc)
            .OfClass(Phase)
            .WhereElementIsNotElementType()
            .ToElements()
        ),
        default=[]
    )

raw_count = len(phases)

# Cap after discovery
try:
    max_n = int(max_phases_to_inspect)
    if max_n >= 0:
        phases = phases[:max_n]
except:
    pass

# -------------------------
# Inventory build
# -------------------------

inv = {}

for i, ph in enumerate(phases):
    ph_key = _phase_key(ph, i)

    # Exporter-aligned synthetic fields (these are the key governance candidates)
    name = _safe(lambda: getattr(ph, "Name", None), None)
    if name is None or (isinstance(name, str) and name.strip() == ""):
        pv_name = _synthetic_value_contract("missing", "String", None, None, None)
    else:
        pv_name = _synthetic_value_contract("ok", "String", name, name, name)
    _inv_add(inv, "phase.name", pv_name, ph_key)

    uid = _safe(lambda: getattr(ph, "UniqueId", None), None)
    if uid is None or (isinstance(uid, str) and uid.strip() == ""):
        pv_uid = _synthetic_value_contract("missing", "String", None, None, None)
    else:
        pv_uid = _synthetic_value_contract("ok", "String", uid, uid, uid)
    _inv_add(inv, "phase.uid", pv_uid, ph_key)

    seq = _safe(lambda: getattr(ph, "SequenceNumber", None), None)
    if seq is None:
        # exporter uses stable fallback (i+1) if SequenceNumber absent
        seq = i + 1
        pv_seq = _synthetic_value_contract("unreadable", "Integer", seq, str(seq), seq)
    else:
        pv_seq = _synthetic_value_contract("ok", "Integer", seq, str(seq), seq)
    _inv_add(inv, "phase.sequence_number", pv_seq, ph_key)

    # Optional: inventory actual Phase element parameters (p.* surface)
    if include_phase_parameters:
        params = _safe(lambda: list(ph.GetOrderedParameters()), default=None)
        if params is None:
            params = _safe(lambda: list(ph.Parameters), default=[])

        for p in params:
            dn = _safe(lambda: _safe_param_def_name(p), None)
            if not dn:
                continue
            pk = "p.{}".format(dn)
            pv = _format_param_contract(p)
            _inv_add(inv, pk, pv, ph_key)

# Emit stable inventory records
records = []
for k in sorted(inv.keys()):
    e = inv[k]
    records.append({
        "domain": "phases",
        "param_key": k,
        "example": e["example"],
        "observed": {
            "raw_phase_count": raw_count,
            "selected_phase_sample_count": len(phases),
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_phases": sorted(list(e["observed_on_phase_keys"]))[:25]
        }
    })

OUT = [
    {"domain": "phases", "kind": "inventory", "records": records},
    {"domain": "phases", "kind": "crosswalk", "records": []},
]

# -------------------------
# Optional: write JSON artifact
# -------------------------

file_written = None
file_write_error = None

if write_json:
    try:
        # Prefer explicit folder
        target_dir = output_directory

        # Fallback to RVT folder if available
        if not target_dir:
            rvt_path = _safe(lambda: doc.PathName, None)
            if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
                target_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        # Final fallback: TEMP / TMP / cwd
        if not target_dir:
            target_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        # Ensure folder exists
        if target_dir and (not os.path.exists(target_dir)):
            os.makedirs(target_dir)

        stamp = datetime.now().strftime("%Y-%m-%d")
        filename = "probe_phases_{}.json".format(stamp)
        target_path = os.path.join(target_dir, filename)

        with open(target_path, "w") as f:
            json.dump(OUT, f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        file_write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach audit metadata (does not change required shape)
OUT[0]["file_written"] = file_written
OUT[0]["file_write_error"] = file_write_error


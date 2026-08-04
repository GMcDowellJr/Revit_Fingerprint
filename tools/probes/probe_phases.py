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
#          probes_<revit_version>_<run_id>.json
#        When False, no file is written (OUT is still returned).
#        Default: False
#
#   IN[4] enable_crosswalk (bool)
#        Whether to emit a Phase -> using-views crosswalk (one row per
#        Phase: how many non-template views have that Phase as their
#        VIEW_PHASE parameter, plus a small name sample). Appended as a
#        new position rather than inserted between IN[1] and IN[2] so
#        existing positional callers (output_directory/write_json) don't
#        silently shift.
#        Default: False
#
#   IN[5] max_views_to_scan (int)
#        When crosswalk enabled, scan at most N views (post-collector,
#        pre-template-filter) for VIEW_PHASE assignments.
#        Default: 2000
#
# Notes:
#   - Modeled after exporter domain behavior: phases are global, order matters, and name+sequence are the main
#     cross-project signature candidates; UniqueId is document-scoped evidence only.
#   - Discovery prefers doc.Phases (preserves document ordering), with collector fallback.
#   - Crosswalk: phases are referenced by views (VIEW_PHASE) and, indirectly, by every phased element's
#     Created/Demolished phase -- the latter is corpus-scale element data, out of scope for a single-file
#     breadth probe, so this crosswalk covers the view-level reference only. A phase with
#     used_by_view_count == 0 has no view referencing it directly (not the same as "unused" corpus-wide,
#     since elements can still carry it as Created/Demolished phase).

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
    Phase, View, BuiltInParameter
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
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False

enable_crosswalk = IN[4] if len(IN) > 4 and IN[4] is not None else False
max_views_to_scan = IN[5] if len(IN) > 5 and IN[5] is not None else 2000


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


# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection
# -------------------------
# Complements the curated/dynamic capture above with a breadth-only sweep of
# the sampled objects' .NET properties and zero-arg methods. This is
# diagnostics/breadth, not identity -- it surfaces members a fixed/curated
# key list or a Parameters-only walk could otherwise miss.

_REFLECTION_SKIP = set([
    "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
    "Dispose", "GetEnumerator", "Clone",
])

def _reflect_member_names(obj):
    out = []
    if obj is None:
        return out
    try:
        t = obj.GetType()
    except:
        return out
    try:
        for p in t.GetProperties():
            try:
                n = p.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if p.GetIndexParameters():
                    continue
                out.append(("property", n))
            except:
                pass
    except:
        pass
    try:
        for m in t.GetMethods():
            try:
                n = m.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
                    continue
                if m.GetParameters().Length != 0:
                    continue
                if m.IsSpecialName:
                    continue
                out.append(("method", n))
            except:
                pass
    except:
        pass
    seen = set()
    uniq = []
    for kind, n in out:
        if n in seen:
            continue
        seen.add(n)
        uniq.append((kind, n))
    return sorted(uniq, key=lambda x: x[1])

def _reflect_try_get(obj, member_kind, name):
    if member_kind == "method":
        # SAFETY: never invoke a reflection-discovered method. Revit API
        # methods can have side effects (printing, export, regenerate,
        # delete, transaction commits, ...) and there is no reliable way to
        # tell a safe zero-arg query method from a side-effecting one by
        # name alone. Record that the method exists without calling it.
        return (True, "<method not invoked>", None)
    try:
        v = getattr(obj, name)
    except Exception as ex:
        return (False, None, "{}: {}".format(type(ex).__name__, ex))
    return (True, v, None)

def _reflect_contract(raw_v):
    if raw_v is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
    if isinstance(raw_v, bool):
        return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
    if isinstance(raw_v, int):
        return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, float):
        return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, str):
        return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
    try:
        if hasattr(raw_v, "IntegerValue"):
            iv = int(raw_v.IntegerValue)
            return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
    except:
        pass
    try:
        if hasattr(raw_v, "ToString"):
            s = raw_v.ToString()
            if s and "Autodesk.Revit" not in s and "System." not in s:
                return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
    except:
        pass
    return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}

def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
    idx = {}
    for obj in sample_objs:
        if obj is None:
            continue
        for member_kind, name in _reflect_member_names(obj)[:max_members]:
            ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
            key = "refl.{}.{}".format(type_label, name)
            if key not in idx:
                idx[key] = {
                    "domain": domain_name, "member_key": key, "member_kind": member_kind,
                    "type_label": type_label, "example": None,
                    "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
                }
            e = idx[key]
            if not ok:
                e["error_count"] += 1
                continue
            contract = _reflect_contract(raw_v)
            e["ok_count"] += 1
            sig = (str(contract.get("storage")), str(contract.get("norm")))
            if sig not in e["_seen"]:
                e["_seen"].add(sig)
                e["unique_value_count"] += 1
            if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
                e["example"] = contract
    records = []
    for key in sorted(idx.keys()):
        e = idx[key]
        records.append({
            "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
            "type_label": e["type_label"], "example": e["example"],
            "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
        })
    return records

# -------------------------
# Crosswalk (optional): Phase -> using-views. Reuses the same view-scan/
# get_Parameter pattern probe_phase_filters.py already uses for VIEW_PHASE_FILTER,
# applied here to VIEW_PHASE. One row per discovered Phase (not per view) --
# an aggregated usage count is the directly useful governance signal
# ("is this phase actually referenced by anything"), not a raw per-view join.
# -------------------------

def _get_view_phase_param(v):
    bip = _safe(lambda: BuiltInParameter.VIEW_PHASE, None)
    if bip is not None:
        p = _safe(lambda: v.get_Parameter(bip), None)
        if p is not None:
            return ("VIEW_PHASE", p)
    p = _safe(lambda: v.LookupParameter("Phase"), None)
    if p is not None:
        return ("Phase", p)
    return (None, None)

phase_usage = {}  # phase_id_int -> {"view_count": int, "sample_view_names": [...]}
if enable_crosswalk:
    scan_views = _safe(lambda: list(FilteredElementCollector(doc).OfClass(View).ToElements()), default=[])
    try:
        vcap = int(max_views_to_scan)
        if vcap >= 0:
            scan_views = scan_views[:vcap]
    except:
        pass

    for v in scan_views:
        if v is None:
            continue
        is_template = _safe(lambda: bool(v.IsTemplate), False)
        if is_template:
            continue
        _matched_name, p = _get_view_phase_param(v)
        pv = _format_param_contract(p)
        if pv.get("storage") != "ElementId" or pv.get("raw") is None:
            continue
        ph_id = int(pv.get("raw"))
        entry = phase_usage.setdefault(ph_id, {"view_count": 0, "sample_view_names": []})
        entry["view_count"] += 1
        vname = _safe(lambda: v.Name, None)
        if vname and len(entry["sample_view_names"]) < 5:
            entry["sample_view_names"].append(vname)

optional_crosswalk = []


def _resolve_workset(doc, ws_id_obj):
    """Resolve an Element.WorksetId value to (name, resolved_bool) via
    WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
    distinct .NET type from ElementId (both happen to expose .IntegerValue,
    which is why reflection reports this member as ElementId-storage), and
    Workset is not derived from Element, so doc.GetElement() would never
    resolve it even with the right type assumed."""
    if ws_id_obj is None:
        return (None, False)
    wt_table = _safe(lambda: doc.GetWorksetTable(), None)
    if wt_table is None:
        return (None, False)
    ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
    if ws is None:
        return (None, False)
    name = _safe(lambda: ws.Name, None)
    return (name, name is not None)


for i, ph in enumerate(phases):
    pid = _safe(lambda: ph.Id.IntegerValue, None)
    if pid is None:
        continue
    pname = _safe(lambda: ph.Name, None)
    ph_ws_id_obj = _safe(lambda: ph.WorksetId, None)
    ph_ws_name, _ph_ws_resolved = _resolve_workset(doc, ph_ws_id_obj)
    ph_ws_id_int = _safe(lambda: ph_ws_id_obj.IntegerValue, None) if ph_ws_id_obj is not None else None
    usage = phase_usage.get(pid, {"view_count": 0, "sample_view_names": []})
    optional_crosswalk.append({
        "phase.id": pid,
        "phase.name": pname,
        "phase.workset_id": ph_ws_id_int,
        "phase.workset_name": ph_ws_name,
        "phase.is_used_by_any_view": usage["view_count"] > 0,
        "used_by_view_count": usage["view_count"],
        "sample_view_names": usage["sample_view_names"],
    })

_reflection_records_0 = _run_reflection_sweep(phases, "Phase", "phases")
_reflection_records = _reflection_records_0

OUT = [
    {"domain": "phases", "kind": "inventory", "records": records},
    {"domain": "phases", "kind": "crosswalk", "records": optional_crosswalk},
    {"domain": "phases", "kind": "reflection", "records": _reflection_records},
]

# -------------------------
# Optional: write JSON artifact
# -------------------------

file_written = None
file_write_error = None

# -------------------------
# Unified run metadata (release-separated, not date-filename-separated)
# -------------------------
# extraction_date lives as JSON metadata, not as a filename token; the
# filename groups by Revit release (revit_version) plus an opaque run_id so
# repeated runs don't collide. See tools/probes/build_probe_inventory.py,
# which consumes this shape directly.

import uuid as _uuid_mod

def _probe_revit_version():
    try:
        _uiapp = DocumentManager.Instance.CurrentUIApplication
        _app = _uiapp.Application if _uiapp is not None else None
        v = _safe(lambda: _app.VersionNumber, None)
        return str(v) if v else None
    except:
        return None

def _probe_document_identity():
    return {
        "title": _safe(lambda: doc.Title, None),
        "path_name": _safe(lambda: doc.PathName, None),
        "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
    }

def _probe_run_id():
    try:
        return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
    except:
        return _uuid_mod.uuid4().hex[:12]

_PROBE_RUN_ID = _probe_run_id()
_PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"

def _probe_wrap(domain, out_payload):
    return {
        "run_metadata": {
            "run_id": _PROBE_RUN_ID,
            "extraction_date": datetime.now().isoformat(),
            "revit_version": _PROBE_REVIT_VERSION,
            "tool_version": None,
            "document": _probe_document_identity(),
            "source": "single_probe",
            "probe": domain,
        },
        "domains": {domain: out_payload},
    }


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
        filename = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
        target_path = os.path.join(target_dir, filename)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("phases", OUT), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        file_write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach audit metadata (does not change required shape)
OUT[0]["file_written"] = file_written
OUT[0]["file_write_error"] = file_write_error


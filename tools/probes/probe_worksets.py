# tools/probes/probe_worksets.py
#
# Dynamo Python (Revit) — Breadth Probe: worksets (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "reflection",
#     "domain": "worksets",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "inventory",
#     "domain": "worksets",
#     "records": [...]
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "worksets",
#     "records": [...]
#   }
# ]
#
# Reworked from the original bespoke findings-dict probe to match the
# reflection/inventory/crosswalk contract every other domain probe uses.
# Two concrete reasons, not just consistency:
#   1. tools/probes/build_probe_inventory.py's merge step requires each OUT
#      entry to be a dict with a "kind"; the old probe emitted
#      OUT = json.dumps(findings, ...) (a single string), so every run's
#      worksets output was silently counted as an "unrecognized_entry" and
#      never made it into PROBE_INVENTORY.csv/md.
#   2. The old probe only reported a fixed, hand-picked field list
#      (Name/Kind/Id/UniqueId/IsEditable/Owner/IsDefaultWorkset). The
#      reflection sweep below surfaces the full .NET member surface of
#      Workset without assuming in advance which members matter --
#      consistent with every other domain probe and with the goal of
#      finding everything exportable, not just what a current extractor
#      already asks for.
#
# Inputs:
#   IN[0] max_worksets_per_kind (int)
#        Cap on how many Workset instances to sample PER discovered
#        WorksetKind for the reflection/inventory sweep (all worksets of
#        each kind are still counted; only the sweep sample is capped).
#        Default: 50
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit Workset -> owned-element-count crosswalk (via
#        ElementWorksetFilter -- an indexed filter, not a full-corpus
#        scan, but still opt-in to match the other domains' pattern).
#        Default: False
#
#   IN[2] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[3] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probes_<revit_version>_<run_id>.json
#        If None, falls back to RVT directory, then TEMP.


import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import FilteredElementCollector, FilteredWorksetCollector

try:
    from Autodesk.Revit.DB import WorksetKind
except:
    WorksetKind = None

try:
    from Autodesk.Revit.DB import ElementWorksetFilter
except:
    ElementWorksetFilter = None

doc = DocumentManager.Instance.CurrentDBDocument

max_worksets_per_kind = IN[0] if len(IN) > 0 and IN[0] is not None else 50
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
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


def _pv(q, storage, raw, display=None, norm=None):
    # Coerce anything that isn't already a JSON-native type (None/bool/int/
    # float/str) to a string before it can reach json.dump(). Fixes a real
    # 2026-08-04 failure: workset.unique_id (below) passed ws.UniqueId
    # straight through as `raw` -- Workset.UniqueId is System.Guid, not
    # System.String, unlike Element.UniqueId -- and json.dump() threw
    # "TypeError: Object of type Guid is not JSON serializable" partway
    # through writing the combined file, silently corrupting it at the
    # real output path under the old non-atomic writer. Every _pv() caller
    # in this file funnels through here, so this is the one place that
    # needs to guard against a future call site making the same mistake.
    def _coerce(v):
        if v is None or isinstance(v, (bool, int, float, str)):
            return v
        try:
            return str(v)
        except:
            return None
    raw = _coerce(raw)
    disp = _coerce(display) if display is not None else raw
    nrm = _coerce(norm) if norm is not None else raw
    return {"q": q, "storage": storage, "raw": raw, "display": disp, "norm": nrm}


# -------------------------
# WorksetKind discovery via dir()/getattr introspection, not a hardcoded
# name list and NOT System.Enum.GetNames(). The previous probe hardcoded
# ("UserWorkset", "View", "FamilyWorkset", "StandardWorkset") -- "View" is
# not a WorksetKind member in Revit 2025 and always produced a
# KIND_NOT_FOUND entry.
#
# An earlier version of this rework used System.Enum.GetNames(WorksetKind)
# for "any kind Revit adds gets picked up automatically" -- but a bare
# `import System` does not reliably resolve in this CPython3 Dynamo host
# (confirmed: it silently failed a real run, producing zero Workset
# samples and a single doc.is_workshared row). dir(enum_type)+getattr is
# the pattern already proven to work in this exact environment --
# probe_browser_organization.py's bip_lookup uses it against
# BuiltInParameter (3628 members) successfully in the same run this bug
# was found in -- so use that instead.
# -------------------------

_ENUM_INTROSPECT_SKIP = set([
    "value__", "GetType", "ToString", "Equals", "GetHashCode",
    "CompareTo", "GetTypeCode", "HasFlag",
])


def _discover_enum_members(enum_type):
    """Returns [(name, int_value), ...] for a CLR enum type, or [] if unavailable."""
    out = []
    if enum_type is None:
        return out
    try:
        names = dir(enum_type)
    except:
        return out
    for n in names:
        if n.startswith("_") or n in _ENUM_INTROSPECT_SKIP:
            continue
        try:
            attr = getattr(enum_type, n, None)
            if attr is None:
                continue
            iv = int(str(attr))
        except:
            continue
        out.append((n, iv))
    return sorted(out, key=lambda x: x[1])


workset_kind_members = _discover_enum_members(WorksetKind)  # [(name, int), ...]

# -------------------------
# Discovery: sample Workset instances across every discovered kind
# -------------------------

is_workshared = _safe(lambda: bool(doc.IsWorkshared), None)

selected = []           # live Workset objects (breadth sample, all kinds)
kind_counts = {}        # kind_name -> full collector count (not capped)

if is_workshared and FilteredWorksetCollector is not None and workset_kind_members:
    for kind_name, kind_int in workset_kind_members:
        kind_attr = getattr(WorksetKind, kind_name, None)
        if kind_attr is None:
            continue
        col = _safe(lambda: list(FilteredWorksetCollector(doc).OfKind(kind_attr)), [])
        kind_counts[kind_name] = len(col)
        try:
            n = int(max_worksets_per_kind)
        except:
            n = 50
        selected.extend(col[:n] if n >= 0 else col)

# Active workset -- single most useful doc-level fact, folded into
# inventory below as a synthesized row rather than a bespoke top-level key.
active_workset_id = None
active_workset_name = None
if is_workshared:
    wt = _safe(lambda: doc.GetWorksetTable(), None)
    if wt is not None:
        active_workset_id = _safe(lambda: wt.GetActiveWorksetId(), None)
        if active_workset_id is not None:
            for ws in selected:
                if _safe(lambda: ws.Id, None) == active_workset_id:
                    active_workset_name = _safe(lambda: ws.Name, None)
                    break

# -------------------------
# Inventory: Workset is not an Element (no .Parameters / .GetOrderedParameters),
# so there is no BuiltInParameter/shared-param walk to run here. The
# meaningful surface is synthesized directly from the object model, using
# the same q/storage/raw/display/norm contract the other domains' real
# parameter inventories use -- so downstream tooling (build_probe_inventory.py,
# name_key_coverage, etc.) doesn't need a special case for this domain.
# -------------------------

param_index = {}


def _add_inventory_obs(param_key, pv, bucket=None):
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set(),
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
    if entry["example"] is None or (entry["example"].get("q") != "ok" and q == "ok"):
        entry["example"] = pv


for ws in selected:
    kind_raw = _safe(lambda: ws.Kind, None)
    kind_int = _safe(lambda: int(str(kind_raw)), None)
    bucket = str(kind_int) if kind_int is not None else "unknown"

    name_v = _safe(lambda: ws.Name, None)
    _add_inventory_obs("workset.name", _pv("ok" if name_v else "missing", "String", name_v), bucket)

    _add_inventory_obs(
        "workset.kind",
        _pv("ok" if kind_int is not None else "unreadable", "Integer", kind_int,
            display=str(kind_raw) if kind_raw is not None else None),
        bucket,
    )

    editable_v = _safe(lambda: bool(ws.IsEditable), None)
    _add_inventory_obs(
        "workset.is_editable",
        _pv("ok" if editable_v is not None else "unreadable", "Integer",
            int(editable_v) if editable_v is not None else None, display=str(editable_v)),
        bucket,
    )

    default_v = _safe(lambda: ws.IsDefaultWorkset, None)
    _add_inventory_obs(
        "workset.is_default_workset",
        _pv("ok" if default_v is not None else "unreadable", "String",
            str(default_v) if default_v is not None else None),
        bucket,
    )

    owner_v = _safe(lambda: ws.Owner, None)
    _add_inventory_obs(
        "workset.owner",
        _pv("ok", "String", owner_v, display=owner_v if owner_v else "(none checked out)"),
        bucket,
    )

    uid_v = _safe(lambda: ws.UniqueId, None)
    _add_inventory_obs("workset.unique_id", _pv("ok" if uid_v else "unreadable", "String", uid_v), bucket)

    is_active = bool(active_workset_id is not None and _safe(lambda: ws.Id, None) == active_workset_id)
    _add_inventory_obs("workset.is_active_workset", _pv("ok", "Integer", int(is_active), display=str(is_active)), bucket)

# Doc-level scalar facts -- kept as single-bucket synthesized rows so
# nothing the old bespoke findings dict captured (IsWorkshared gate,
# active workset name) is lost in the standard shape.
_add_inventory_obs(
    "doc.is_workshared",
    _pv("ok" if is_workshared is not None else "unreadable", "Integer",
        int(bool(is_workshared)) if is_workshared is not None else None, display=str(is_workshared)),
    "doc",
)
if active_workset_name:
    _add_inventory_obs("doc.active_workset_name", _pv("ok", "String", active_workset_name), "doc")

for kind_name, kind_int in workset_kind_members:
    _add_inventory_obs(
        "worksetkind.{}.collector_count".format(kind_name),
        _pv("ok", "Integer", kind_counts.get(kind_name, 0)),
        "doc",
    )

param_inventory = []
for k in sorted(param_index.keys()):
    e = param_index[k]
    param_inventory.append({
        "domain": "worksets",
        "param_key": k,
        "example": e["example"],
        "observed": {
            "storage_types": sorted(e["storage_types"]),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(e["observed_on_buckets"]),
        },
        "selected_worksets_sample_count": len(selected),
    })

# -------------------------
# Crosswalk (optional): Workset -> owned-element count via
# ElementWorksetFilter. This is an indexed filter (cheap), not a full
# corpus scan, but still gated behind enable_crosswalk to match the other
# domains' opt-in cross-referencing pattern -- and because it's the one
# part of this probe whose cost scales with selected-workset count.
# -------------------------

optional_crosswalk = []
if enable_crosswalk and ElementWorksetFilter is not None:
    for ws in selected:
        ws_id = _safe(lambda: ws.Id, None)
        if ws_id is None:
            continue
        count = _safe(
            lambda: FilteredElementCollector(doc).WherePasses(ElementWorksetFilter(ws_id)).GetElementCount(),
            None,
        )
        optional_crosswalk.append({
            "workset.id": _safe(lambda: ws_id.IntegerValue, None),
            "workset.name": _safe(lambda: ws.Name, None),
            "workset.kind": _safe(lambda: str(ws.Kind), None),
            "owned_element_count": count,
        })

# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection.
# Identical engine to every other domain probe (copied verbatim, not
# imported -- each probe_*.py must remain a self-contained, paste-able
# Dynamo Python node).
# -------------------------

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


_reflection_records = _run_reflection_sweep(selected, "Workset", "worksets")

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "worksets",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "worksets",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "worksets",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
file_written = None
write_error = None

# -------------------------
# Unified run metadata (release-separated, not date-filename-separated)
# -------------------------

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
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None

        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("worksets", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to reflection header (keeps OUT shape stable with
# every other domain probe, whose file_written/file_write_error live on
# OUT_payload[0])
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

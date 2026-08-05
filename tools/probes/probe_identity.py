# tools/probes/probe_identity.py
#
# Dynamo Python (Revit) — Breadth Probe: identity (PROJECT IDENTITY)
#
# Domain definition (per user clarification):
#   "identity" here means PROJECT / DOCUMENT identity (name, path, worksharing, etc.),
#   plus ProjectInformation element parameter surface.
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "identity",
#     "records": [...],
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "identity",
#     "records": [...]
#   }
# ]
#
# Inputs (IN):
#   IN[0] include_project_information_params (bool)   Default: True
#       When True, inventory the ProjectInformation element's parameters.
#
#   IN[1] include_document_metadata (bool)            Default: True
#       When True, inventory document/app/worksharing/user metadata keys.
#
#   IN[2] include_environment (bool)                  Default: False
#       When True, include selected environment variables (username/computer) if available.
#
#   IN[3] write_json (bool)                           Default: False
#   IN[4] output_directory (str)                      Default: None
#
# Notes:
#   - This probe is exploratory evidence capture, not production export logic.
#   - Defensive style: never throw; partial output is acceptable.

import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    ElementId,
    StorageType,
    FilteredElementCollector,
    ProjectInfo,
    ModelPathUtils,
    BuiltInParameter,
)

doc = DocumentManager.Instance.CurrentDBDocument
uiapp = DocumentManager.Instance.CurrentUIApplication
app = uiapp.Application if uiapp is not None else None

DOMAIN = "identity"

include_project_information_params = IN[0] if len(IN) > 0 and IN[0] is not None else True
include_document_metadata = IN[1] if len(IN) > 1 and IN[1] is not None else True
include_environment = IN[2] if len(IN) > 2 and IN[2] is not None else False
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

def _as_str(x):
    try:
        if x is None:
            return None
        return str(x)
    except:
        return None

def _param_contract_from_value(storage, raw, display=None, norm=None, q="ok"):
    return {
        "q": q,
        "storage": storage if storage is not None else "None",
        "raw": raw,
        "display": display,
        "norm": norm
    }

def _safe_defn_builtin(defn):
    return _safe(lambda: defn.BuiltInParameter, None) if defn is not None else None

def _definition_origin(defn, p):
    """
    Robust classifier that works even when Definition.BuiltInParameter is unavailable/unreliable.

    built_in      : Parameter.Id.IntegerValue < 0   (built-in parameters use negative ids)
    shared        : Parameter.IsShared == True
    project_custom: Parameter.Id.IntegerValue >= 0 and IsShared == False
    """
    pid = _safe(lambda: p.Id.IntegerValue, None)
    if pid is not None:
        try:
            if int(pid) < 0:
                return "built_in"
        except:
            pass

    is_shared = _safe(lambda: p.IsShared, None)
    if is_shared is True:
        return "shared"
    if is_shared is False:
        return "project_custom"
    return None

def _shared_guid_if_any(defn, p):
    # Prefer SharedParameterElement behind p.Id; fall back to p.GUID / defn.GUID if available.
    if _safe(lambda: p.IsShared, False) is not True:
        return None

    # 1) SharedParameterElement (most reliable in practice)
    try:
        spe = doc.GetElement(p.Id)
        if spe is not None:
            # SharedParameterElement.GuidValue (property name in API)
            gv = _safe(lambda: spe.GuidValue, None)
            if gv is not None:
                return _safe(lambda: str(gv), None)
    except:
        pass

    # 2) Parameter.GUID (some contexts expose this)
    guid2 = _safe(lambda: p.GUID, None)
    if guid2 is not None:
        return _safe(lambda: str(guid2), None)

    # 3) Definition.GUID (ExternalDefinition)
    guid3 = _safe(lambda: defn.GUID, None) if defn is not None else None
    if guid3 is not None:
        return _safe(lambda: str(guid3), None)

    return None

def _param_group_legacy_str(defn):
    # Legacy enum (may be INVALID / unreliable)
    pg = _safe(lambda: defn.ParameterGroup, None) if defn is not None else None
    return _as_str(pg)

def _format_param_contract(p):
    """
    Contract:
      {
        "q": "ok | missing | unreadable | unsupported",
        "storage": "String | Integer | Double | ElementId | None",
        "raw": "...",
        "display": "...",
        "norm": "..."
      }

    Normalization guidance (conservative for project identity):
      - strings remain strings
      - ints remain ints
      - doubles remain doubles (no unit conversion unless we later prove it matters)
      - ElementId -> IntegerValue (+ resolved name if cheap)
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
        disp = _safe(lambda: p.AsValueString(), None)
        return {
            "q": "ok",
            "storage": "Integer",
            "raw": raw,
            "display": disp if disp is not None else (_as_str(raw)),
            "norm": raw
        }

    if st == StorageType.Double:
        raw = _safe(lambda: p.AsDouble(), None)
        disp = _safe(lambda: p.AsValueString(), None)
        return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": raw}

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
            "display": ref_name if ref_name is not None else (_as_str(raw)),
            "norm": raw
        }

    return {"q": "unsupported", "storage": _as_str(st), "raw": None, "display": None, "norm": None}

def _add_inventory_record(inv, key, contract, breadth=None):
    """
    inv: dict param_key -> accumulator
    Dedup rule (probe-local): group observations by (param_key, storage, norm)
    For project identity, we still keep 1 record per param_key, with one example.
    """
    if key not in inv:
        inv[key] = {
            "param_key": key,
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "breadth": breadth or {}
        }

    entry = inv[key]
    q = contract.get("q") or "unreadable"
    st = contract.get("storage")

    if st:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    # Keep exactly one example: prefer first ok encountered.
    if entry["example"] is None:
        entry["example"] = {
            "q": contract.get("q"),
            "storage": contract.get("storage"),
            "raw": contract.get("raw"),
            "display": contract.get("display"),
            "norm": contract.get("norm"),
        }
    elif entry["example"].get("q") != "ok" and q == "ok":
        entry["example"] = {
            "q": contract.get("q"),
            "storage": contract.get("storage"),
            "raw": contract.get("raw"),
            "display": contract.get("display"),
            "norm": contract.get("norm"),
        }

    # merge breadth hints (non-destructive)
    if breadth:
        for k, v in breadth.items():
            if k not in entry["breadth"]:
                entry["breadth"][k] = v

def _provenance_layer(param_key):
    # Objective provenance only (no semantic guessing).
    if not param_key:
        return "unknown"

    try:
        k = str(param_key)
    except:
        return "unknown"

    if k.startswith("doc."):
        return "core_project_identity"
    if k.startswith("app.") or k.startswith("env."):
        return "runtime_context"
    if k.startswith("project_info."):
        return "project_metadata"

    return "unknown"

# -------------------------
# Discovery (project identity)
# -------------------------

inventory = {}

if include_document_metadata:
    # Document title / path
    _add_inventory_record(
        inventory,
        "doc.title",
        _param_contract_from_value("String", _safe(lambda: doc.Title, None), display=_safe(lambda: doc.Title, None), norm=_safe(lambda: doc.Title, None)),
        breadth={"source": "Document"}
    )

    _add_inventory_record(
        inventory,
        "doc.path_name",
        _param_contract_from_value("String", _safe(lambda: doc.PathName, None), display=_safe(lambda: doc.PathName, None), norm=_safe(lambda: doc.PathName, None)),
        breadth={"source": "Document"}
    )

    _add_inventory_record(
        inventory,
        "doc.is_workshared",
        _param_contract_from_value("Integer", int(bool(_safe(lambda: doc.IsWorkshared, False))), display=_as_str(_safe(lambda: doc.IsWorkshared, False)), norm=int(bool(_safe(lambda: doc.IsWorkshared, False)))),
        breadth={"source": "Document"}
    )

    # Worksharing central model path (may throw or be unavailable)
    central_user_path = None
    central_model_path = _safe(lambda: doc.GetWorksharingCentralModelPath(), None)
    if central_model_path is not None:
        central_user_path = _safe(lambda: ModelPathUtils.ConvertModelPathToUserVisiblePath(central_model_path), None)

    q = "ok" if central_user_path is not None else "missing"
    _add_inventory_record(
        inventory,
        "doc.central_path",
        _param_contract_from_value("String", central_user_path, display=central_user_path, norm=central_user_path, q=q),
        breadth={"source": "Worksharing"}
    )

    # Application / version info
    _add_inventory_record(
        inventory,
        "app.version_name",
        _param_contract_from_value("String", _safe(lambda: app.VersionName, None), display=_safe(lambda: app.VersionName, None), norm=_safe(lambda: app.VersionName, None)),
        breadth={"source": "Application"}
    )

    _add_inventory_record(
        inventory,
        "app.version_number",
        _param_contract_from_value("String", _safe(lambda: app.VersionNumber, None), display=_safe(lambda: app.VersionNumber, None), norm=_safe(lambda: app.VersionNumber, None)),
        breadth={"source": "Application"}
    )

    _add_inventory_record(
        inventory,
        "app.version_build",
        _param_contract_from_value(
            "String",
            _safe(lambda: app.VersionBuild, None),
            display=_safe(lambda: app.VersionBuild, None),
            norm=_safe(lambda: app.VersionBuild, None)
        ),
        breadth={"source": "Application"}
    )

    # User (best-effort; not guaranteed meaningful for standards, but identity evidence)
    _add_inventory_record(
        inventory,
        "app.username",
        _param_contract_from_value("String", _safe(lambda: app.Username, None), display=_safe(lambda: app.Username, None), norm=_safe(lambda: app.Username, None)),
        breadth={"source": "Application"}
    )

if include_environment:
    # Optional: external environment keys (often helpful in forensic runs; not standards identity)
    env_user = os.environ.get("USERNAME") or os.environ.get("USER")
    env_comp = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME")

    _add_inventory_record(
        inventory,
        "env.username",
        _param_contract_from_value("String", env_user, display=env_user, norm=env_user, q=("ok" if env_user is not None else "missing")),
        breadth={"source": "Environment"}
    )
    _add_inventory_record(
        inventory,
        "env.computer",
        _param_contract_from_value("String", env_comp, display=env_comp, norm=env_comp, q=("ok" if env_comp is not None else "missing")),
        breadth={"source": "Environment"}
    )

if include_project_information_params:
    # ProjectInformation element parameter surface (category-free, single element)
    pi = _safe(lambda: doc.ProjectInformation, None)

    if pi is None:
        _add_inventory_record(
            inventory,
            "project_info._element",
            _param_contract_from_value("None", None, display=None, norm=None, q="missing"),
            breadth={"source": "ProjectInformation"}
        )
    else:
        # stable IDs/names for evidence
        _add_inventory_record(
            inventory,
            "project_info.element_id",
            _param_contract_from_value("Integer", _safe(lambda: pi.Id.IntegerValue, None), display=_as_str(_safe(lambda: pi.Id.IntegerValue, None)), norm=_safe(lambda: pi.Id.IntegerValue, None)),
            breadth={"source": "ProjectInformation"}
        )
        _add_inventory_record(
            inventory,
            "project_info.name",
            _param_contract_from_value("String", _safe(lambda: pi.Name, None), display=_safe(lambda: pi.Name, None), norm=_safe(lambda: pi.Name, None)),
            breadth={"source": "ProjectInformation"}
        )

        # Inventory all parameters on ProjectInformation
        params = _safe(lambda: list(pi.GetOrderedParameters()), default=None)
        if params is None:
            params = _safe(lambda: list(pi.Parameters), default=[])

        for p in params:
            defn = _safe(lambda: p.Definition, None)
            pname = _safe(lambda: defn.Name, None) if defn is not None else None
            if not pname:
                continue

            key = "project_info.p.{}".format(pname)
            contract = _format_param_contract(p)

            # breadth hints: group and whether it's shared
            pg = _safe(lambda: defn.ParameterGroup, None) if defn is not None else None
            is_shared = _safe(lambda: p.IsShared, None)

            origin = _definition_origin(defn, p)
            shared_guid = _shared_guid_if_any(defn, p)

            breadth = {
                "source": "ProjectInformation",
                "is_shared": bool(is_shared) if is_shared is not None else None,
                "definition_origin": origin,
                "shared_guid": shared_guid
            }

            _add_inventory_record(inventory, key, contract, breadth=breadth)

# -------------------------
# Emit inventory records (stable order)
# -------------------------

records = []
for key in sorted(inventory.keys()):
    e = inventory[key]
    records.append({
        "domain": DOMAIN,
        "param_key": e["param_key"],
        "provenance_layer": _provenance_layer(e["param_key"]),
        "selected_type_sample_count": 1,  # this probe is document-scoped
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "breadth": e.get("breadth") or {}
        }
    })

# ProjectInformation -> its own workset. Re-fetches doc.ProjectInformation
# rather than reusing `pi` from the inventory section above, since that's
# only defined when include_project_information_params is True.
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


crosswalk_records = []
_pi_cw = _safe(lambda: doc.ProjectInformation, None)
if _pi_cw is not None:
    _pi_ws_id_obj = _safe(lambda: _pi_cw.WorksetId, None)
    _pi_ws_name, _pi_ws_resolved = _resolve_workset(doc, _pi_ws_id_obj)
    _pi_ws_id_int = _safe(lambda: _pi_ws_id_obj.IntegerValue, None) if _pi_ws_id_obj is not None else None
    crosswalk_records.append({
        "project_info.element_id": _safe(lambda: _pi_cw.Id.IntegerValue, None),
        "project_info.name": _safe(lambda: _pi_cw.Name, None),
        "project_info.workset_id": _pi_ws_id_int,
        "project_info.workset_name": _pi_ws_name,
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

# Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
# docs/method_invocation_candidates_annotated.csv): these 33 method names (34
# (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
# zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
# GetValidTypes, removed post-merge -- see the note below the dict) are
# ground-truth confirmed, against the live
# RevitAPI 2025 documentation (not name/return-type inference), to be
# zero-arg, instance, non-mutating getters. Declared here as data, separate
# from the branching logic in _reflect_try_get below, so it can be reviewed
# as one block and extended later without touching control flow.
#
# Keyed by method NAME only, not (declaring_class, name): this reflection
# sweep is invoked per concrete probed type_label (e.g. "WallType",
# "ProjectInformation", "FamilySymbol"), which is almost never the literal
# Revit API class that actually declares the member -- e.g. Element.GetTypeId
# is reached in this codebase via more than a dozen different concrete
# type_labels across the probe domains, never via type_label=="Element"
# itself, since no probe in this file reflects a bare Element instance.
# Scoping the allowlist by declaring-class name would silently fail to match
# nearly every real call site and defeat the point of this allowlist.
# _reflect_member_names() below already restricts candidate methods to
# public, non-special-name, zero-parameter methods before this allowlist is
# ever consulted, so a name-only match here does not weaken the zero-arg/
# no-side-effect intent the allowlist exists to enforce -- it only widens
# which already-zero-arg, already-name-matched members get invoked. The
# dict value (declaring class) is kept for traceability back to the Step 0
# CSV only; it is not used in the match.
_ALLOWLISTED_REFLECTION_METHODS = {
    "GetTypeId": "Element",
    "GetLayers": "CompoundStructure",
    "GetEntitySchemaGuids": "Element",
    "GetSubelements": "Element",
    "GetFamilyPointLocations": "FamilySymbol",
    "GetModelToProjectionTransforms": "View",
    "GetRenderingAsset": "AppearanceAssetElement",
    "GetExternalFileReference": "Element",
    "GetMonitoredLinkElementIds": "Element",
    "GetMonitoredLocalElementIds": "Element",
    "GetSimilarTypes": "ElementType",
    "GetStructuralSection": "FamilySymbol",
    "GetThermalProperties": "FamilySymbol",
    "GetFillPattern": "FillPatternElement",
    "GetLinePattern": "LinePatternElement",
    "GetCategories": "ParameterFilterElement",
    "GetElementFilter": "ParameterFilterElement",
    "GetReference": "Subelement",
    "GetBackground": "View",
    "GetCalloutParentId": "View",
    "GetCropRegionShapeManager": "View",
    "GetDepthCueing": "View",
    "GetDirectContext3DHandleOverrides": "View",
    "GetFilters": "View",
    "GetOrderedFilters": "View",
    "GetPointCloudOverrides": "View",
    "GetPrimaryViewId": "View",
    "GetReferenceCallouts": "View",
    "GetReferenceElevations": "View",
    "GetReferenceSections": "View",
    "GetSketchyLines": "View",
    "GetTemporaryViewPropertiesId": "View",
    "GetViewDisplayModel": "View",
}

# Element.GetValidTypes / Subelement.GetValidTypes were removed from the
# allowlist above after a live re-run (PR #395 discussion) showed
# Element.GetValidTypes fails 100% of the time -- not with a documented
# Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
# GetModelToProjectionTransforms above, which each match a real
# InvalidOperationException precondition stated on their own RevitAPI doc
# pages), but with a CLR/pythonnet interop binding failure:
# `TypeError: No method matches given arguments for GetValidTypes: (<class
# '...'>)`, confirmed via a standalone diagnostic against live ElementType/
# WallType/View objects. .NET reflection sees exactly one GetValidTypes
# overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
# overload-ambiguity problem either -- the call is rejected by the binder
# before it ever reaches Revit's implementation. This will never succeed
# through `getattr(obj, name)()` regardless of model/version, so keeping it
# allowlisted only adds permanent error noise with zero chance of a real
# value. Subelement.GetValidTypes was never independently tested (no probe
# in this codebase reflects a raw Subelement object as its own type_label),
# but shares the same allowlist name and the same removal; re-evaluate
# independently against a live Subelement instance before re-adding either.

def _reflect_try_get(obj, member_kind, name):
    if member_kind == "method":
        if name not in _ALLOWLISTED_REFLECTION_METHODS:
            # SAFETY: never invoke a reflection-discovered method that is not
            # on the allowlist above. Revit API methods can have side effects
            # (printing, export, regenerate, delete, transaction commits,
            # ...) and there is no reliable way to tell a safe zero-arg
            # query method from a side-effecting one by name alone for
            # anything outside the allowlist's ground-truth-verified set.
            # Record that the method exists without calling it.
            return (True, "<method not invoked>", None)
        try:
            v = getattr(obj, name)()
        except Exception as ex:
            return (False, None, "{}: {}".format(type(ex).__name__, ex))
        return (True, v, None)
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
    try:
        ids = []
        for item in raw_v:
            if not hasattr(item, "IntegerValue"):
                raise TypeError("non-ElementId item in collection")
            ids.append(int(item.IntegerValue))
        disp = ",".join(str(i) for i in ids)
        return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
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

_reflect_pi = globals().get("pi", None)
_reflection_records_0 = _run_reflection_sweep([_reflect_pi] if _reflect_pi is not None else [], "ProjectInformation", "identity")
_reflection_records_1 = _run_reflection_sweep([doc] if doc is not None else [], "Document", "identity")
_reflection_records_2 = _run_reflection_sweep([app] if app is not None else [], "Application", "identity")
_reflection_records = _reflection_records_0 + _reflection_records_1 + _reflection_records_2

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "identity",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": DOMAIN,
        "records": records
    },
    {
        "kind": "crosswalk",
        "domain": DOMAIN,
        "records": crosswalk_records
    }
]

# -------------------------
# Optional JSON write
# -------------------------

file_written = None
write_error = None

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
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None

        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("identity", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

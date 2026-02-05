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

# No natural crosswalk for project identity
crosswalk_records = []

OUT_payload = [
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

if write_json:
    try:
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None

        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_{}_{}.json".format(DOMAIN, date_stamp)

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

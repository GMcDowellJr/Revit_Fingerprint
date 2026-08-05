# tools/probes/probe_browser_organization.py
#
# Dynamo Python (Revit) — Breadth Probe: browser_organization (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "reflection",
#     "domain": "browser_organization",
#     "records": [...],
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "inventory",
#     "domain": "browser_organization",
#     "records": [...]
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "browser_organization",
#     "records": [...]
#   }
# ]
#
# Reworked from the original bespoke findings-dict probe ("probe_browser_
# organization_v8") to match the reflection/inventory/crosswalk contract
# every other domain probe uses. Two concrete reasons, not just consistency:
#   1. tools/probes/build_probe_inventory.py's merge step requires each OUT
#      entry to be a dict with a "kind"; the old probe emitted
#      OUT = json.dumps(findings, ...) (a single string), so this domain's
#      output was silently counted as an "unrecognized_entry" and never
#      made it into PROBE_INVENTORY.csv/md.
#   2. This domain has no single "type" to walk (it's BrowserOrganization
#      settings plus a FolderItemInfo tree), which is exactly the kind of
#      domain most likely to have exportable surface a hand-curated findings
#      dict misses. The reflection sweep at the bottom runs over both
#      BrowserOrganization and FolderItemInfo instances so unknown members
#      get surfaced the same way every other domain probe surfaces them.
#
# The cycle-safe tree walk, BIP reverse lookup, and name-resolution fallback
# chain (FolderItemInfo.Name -> Definition.Name -> BuiltInParameter label)
# from the original probe are preserved essentially unchanged -- that logic
# is hard-won and still the right way to classify/resolve a folder item.
# What changed is where the results land: nested findings sub-dicts become
# flat "inventory" (per-category synthesized surfaces) and "crosswalk"
# (one row per resolved folder item, at whatever depth it was found)
# records, and the old hand-rolled dir()/CLR-member probe of FolderItemInfo
# (_record_folder_item_shape) is dropped in favor of the generic reflection
# sweep, which now covers that job for both BrowserOrganization and
# FolderItemInfo without a bespoke per-type routine.
#
# Inputs:
#   IN[0] max_items_per_level (int)
#        Cap on FolderItemInfo children inspected per tree node.
#        Default: 15 (was a fixed 6 in the original probe)
#
#   IN[1] max_tree_depth (int)
#        Cap on recursion depth from the org root.
#        Default: 4 (same cap the original probe used, now configurable)
#
#   IN[2] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[3] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probes_<revit_version>_<run_id>.json
#        If None, falls back to RVT directory, then TEMP.
#
# Extraction model (unchanged from the original probe):
#   FolderItemInfo.ElementId < 0            -> BIP (built-in parameter)
#   FolderItemInfo.ElementId == current seed or org.Id -> cycle/self
#                                               reference, do not recurse
#   FolderItemInfo.ElementId > 0, != org.Id  -> shared parameter element


import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import ElementId, BuiltInParameter

try:
    from Autodesk.Revit.DB import BrowserOrganization
except:
    BrowserOrganization = None

try:
    from Autodesk.Revit.DB import LabelUtils
except:
    LabelUtils = None

try:
    import System
except:
    System = None

doc = DocumentManager.Instance.CurrentDBDocument

max_items_per_level = IN[0] if len(IN) > 0 and IN[0] is not None else 15
max_tree_depth = IN[1] if len(IN) > 1 and IN[1] is not None else 4
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


def _safe_str(v):
    try:
        return str(v)
    except:
        return "<error>"


def _int_enum(val):
    try:
        return int(str(val))
    except:
        return None


def _clean_name(value):
    text = _safe_str(value)
    if text in ("", "None", "<error>", "???"):
        return None
    return text


def _pv(q, storage, raw, display=None, norm=None):
    # Coerce anything that isn't already a JSON-native type (None/bool/int/
    # float/str) to a string before it can reach json.dump(). Fixes a real
    # 2026-08-04 failure: workset.unique_id passed ws.UniqueId straight
    # through as `raw` (Workset.UniqueId is System.Guid, not System.String,
    # unlike Element.UniqueId) and json.dump() threw "TypeError: Object of
    # type Guid is not JSON serializable" partway through writing the
    # combined file -- silently corrupting it at the real output path
    # under the old non-atomic writer. Every _pv() caller across these
    # probes funnels through here, so this is the one place that needs to
    # guard against a future call site making the same mistake.
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


def _try_get_definition_record(elem):
    """RevitLookup-style definition name/GUID data for ParameterElement-like values."""
    rec = {}
    try:
        get_definition = getattr(elem, "GetDefinition", None)
        if get_definition and callable(get_definition):
            definition = get_definition()
            if definition is not None:
                rec["definition_type"] = type(definition).__name__
                definition_name = _clean_name(getattr(definition, "Name", None))
                if definition_name:
                    rec["definition_name"] = definition_name
                try:
                    bip = getattr(definition, "BuiltInParameter", None)
                    if bip is not None:
                        rec["definition_built_in_parameter"] = _safe_str(bip)
                except:
                    pass
                try:
                    guid = getattr(definition, "GUID", None)
                    if guid:
                        rec["guid"] = _safe_str(guid)
                        rec["is_shared_param"] = True
                except:
                    pass
    except Exception as e:
        rec["definition_error"] = str(e)
    return rec


def _builtin_label(bip_value):
    if System is None or LabelUtils is None:
        return None
    try:
        bip = System.Enum.ToObject(BuiltInParameter, bip_value)
        return _clean_name(LabelUtils.GetLabelFor(bip))
    except:
        return None


def _best_name(rec, item_name=None):
    """Pick display name, preserving real folder names before descriptor fallbacks."""
    candidates = []
    if item_name:
        candidates.append(("folder_item_name", item_name))
    if rec.get("definition_name"):
        candidates.append(("definition_name", rec["definition_name"]))
    if rec.get("element_name"):
        candidates.append(("element_name", rec["element_name"]))
    if rec.get("bip_label"):
        candidates.append(("bip_label", rec["bip_label"]))
    if rec.get("bip_name"):
        candidates.append(("bip_name", rec["bip_name"]))

    rec["name_candidates"] = [
        {"source": source, "value": value} for source, value in candidates if _clean_name(value)
    ]
    if rec["name_candidates"]:
        first = rec["name_candidates"][0]
        rec["display_name"] = first["value"]
        rec["display_name_source"] = first["source"]


def _resolve_folder_item(item_eid_int, org_id_int, current_seed_eid_int, doc, bip_lookup):
    """Classify and resolve a FolderItemInfo.ElementId."""
    rec = {"eid_int": item_eid_int}
    is_cycle_reference = item_eid_int == current_seed_eid_int
    is_self_reference = item_eid_int == org_id_int and not is_cycle_reference

    if item_eid_int < 0:
        rec["kind"] = "builtin_parameter"
        rec["bip_int"] = item_eid_int
        rec["bip_name"] = bip_lookup.get(item_eid_int, "UNKNOWN")
        rec["bip_label"] = _builtin_label(item_eid_int)
    else:
        rec["kind"] = "element"
        rec["skip"] = False
        try:
            elem = doc.GetElement(ElementId(item_eid_int))
            if elem is not None:
                rec["element_type"] = type(elem).__name__
                rec["element_name"] = _safe_str(getattr(elem, "Name", None))
                rec.update(_try_get_definition_record(elem))
                try:
                    guid = getattr(elem, "GUID", None)
                    if guid:
                        rec["guid"] = _safe_str(guid)
                        rec["is_shared_param"] = True
                except:
                    pass
            else:
                rec["element_type"] = "null"
        except Exception as e:
            rec["element_resolve_error"] = str(e)

    if is_cycle_reference:
        rec["kind"] = "cycle_reference"
        rec["skip"] = True
    elif is_self_reference:
        rec["kind"] = "self_reference"
        rec["skip"] = True
    return rec


# Collected as a side effect of the tree walk, consumed at the very end by
# the reflection sweep and the flat crosswalk emission -- kept module-level
# rather than threaded through every call for the same reason the original
# probe threaded `visited` through recursion: simplicity over purity in a
# throwaway breadth probe.
_all_folder_items = []      # raw FolderItemInfo objects (for reflection)
_folder_item_records = []   # flat crosswalk rows (category, depth, resolved)
_name_fallback_used_count = 0


def _walk_tree(org, org_id_int, doc, bip_lookup, seed_eid_int, category, depth=0, visited=None):
    """Walk the folder tree, skipping self-references and visited nodes."""
    global _name_fallback_used_count

    if visited is None:
        visited = set()
    if depth > max_tree_depth or seed_eid_int in visited:
        return {"note": "max_depth_or_visited", "eid": seed_eid_int}
    visited.add(seed_eid_int)

    items = _safe(lambda: list(org.GetFolderItems(ElementId(seed_eid_int))), None)
    if items is None:
        return {"error": "GetFolderItems failed", "eid": seed_eid_int}

    try:
        cap = int(max_items_per_level)
    except:
        cap = 15

    result = {"eid": seed_eid_int, "items": []}
    for item in items[:cap]:
        _all_folder_items.append(item)

        eid_int = _safe(lambda: item.ElementId.IntegerValue, None)
        if eid_int is None:
            continue

        resolved = _resolve_folder_item(eid_int, org_id_int, seed_eid_int, doc, bip_lookup)
        item_name = _safe_str(_safe(lambda: item.Name, None))
        resolved["folder_item_name"] = item_name
        _best_name(resolved, item_name)
        if item_name == "???" and resolved.get("display_name"):
            _name_fallback_used_count += 1

        _folder_item_records.append({
            "category": category,
            "depth": depth,
            "eid_int": eid_int,
            "kind": resolved.get("kind"),
            "folder_item_name": item_name,
            "display_name": resolved.get("display_name"),
            "display_name_source": resolved.get("display_name_source"),
            "definition_name": resolved.get("definition_name"),
            "definition_built_in_parameter": resolved.get("definition_built_in_parameter"),
            "bip_name": resolved.get("bip_name"),
            "bip_label": resolved.get("bip_label"),
            "is_shared_param": resolved.get("is_shared_param", False),
            "skip": resolved.get("skip", False),
        })

        if not resolved.get("skip") and resolved.get("kind") == "element" and eid_int not in visited:
            resolved["subtree"] = _walk_tree(org, org_id_int, doc, bip_lookup, eid_int, category, depth + 1, visited)
        result["items"].append(resolved)

    return result


# -------------------------
# Inventory accumulator (same q/storage/raw/display/norm contract every
# other domain's synthesized/param inventory rows use).
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


# -------------------------
# Run
# -------------------------

_browserorg_objs = []  # for reflection sweep

doc_is_family = _safe(lambda: bool(doc.IsFamilyDocument), None)
_add_inventory_obs(
    "doc.is_family_document",
    _pv("ok" if doc_is_family is not None else "unreadable", "Integer",
        int(doc_is_family) if doc_is_family is not None else None, display=str(doc_is_family)),
    "doc",
)

if BrowserOrganization is not None:
    bip_lookup = {}
    for name in dir(BuiltInParameter):
        attr = _safe(lambda: getattr(BuiltInParameter, name, None), None)
        if attr is None:
            continue
        iv = _safe(lambda: int(str(attr)), None)
        if iv is not None and iv < 0:
            bip_lookup[iv] = name
    _add_inventory_obs("doc.bip_lookup_size", _pv("ok", "Integer", len(bip_lookup)), "doc")

    org_map = {
        "views": _safe(lambda: BrowserOrganization.GetCurrentBrowserOrganizationForViews(doc), None),
        "sheets": _safe(lambda: BrowserOrganization.GetCurrentBrowserOrganizationForSheets(doc), None),
        "schedules": _safe(lambda: BrowserOrganization.GetCurrentBrowserOrganizationForSchedules(doc), None),
    }

    for category, org in org_map.items():
        if org is None:
            continue
        _browserorg_objs.append(org)

        org_id = _safe(lambda: org.Id, None)
        org_id_int = _safe(lambda: org_id.IntegerValue, None)
        if org_id_int is None:
            continue
        _add_inventory_obs("browserorg.org_id", _pv("ok", "Integer", org_id_int), category)

        org_ws_id_obj = _safe(lambda: org.WorksetId, None)
        org_ws_name, _org_ws_resolved = _resolve_workset(doc, org_ws_id_obj)
        org_ws_id_int = _safe(lambda: org_ws_id_obj.IntegerValue, None) if org_ws_id_obj is not None else None
        _add_inventory_obs(
            "browserorg.workset_id",
            _pv("ok" if org_ws_id_int is not None else "unreadable", "Integer", org_ws_id_int, display=org_ws_name),
            category,
        )

        # Sort criteria
        sp = _safe(lambda: org.SortingParameterId, None)
        sp_int = _safe(lambda: sp.IntegerValue, None)
        sp_name = bip_lookup.get(sp_int) if sp_int is not None else None
        so_int = _safe(lambda: _int_enum(org.SortingOrder), None)

        _add_inventory_obs(
            "browserorg.sorting_parameter_id",
            _pv("ok" if sp_int is not None else "unreadable", "Integer", sp_int,
                display=sp_name if sp_name else (str(sp_int) if sp_int is not None else None)),
            category,
        )
        _add_inventory_obs(
            "browserorg.sorting_order",
            _pv("ok" if so_int is not None else "unreadable", "Integer", so_int),
            category,
        )
        if sp_int is not None and sp_int > 0:
            sp_elem = _safe(lambda: doc.GetElement(ElementId(sp_int)), None)
            sp_elem_name = _safe(lambda: getattr(sp_elem, "Name", None), None) if sp_elem is not None else None
            if sp_elem_name:
                _add_inventory_obs(
                    "browserorg.sorting_parameter_element_name",
                    _pv("ok", "String", sp_elem_name),
                    category,
                )

        # Filter tab -- real read of stored state (kept); the old probe's
        # hand-rolled "filter_related_methods" list is dropped, since the
        # reflection sweep over BrowserOrganization discovers method
        # members (existence, not invocation) the same generic way it does
        # for every other domain.
        fp = _safe(lambda: list(org.GetParameters("Filter")), [])
        if fp:
            p = fp[0]
            has_value = _safe(lambda: bool(p.HasValue), None)
            _add_inventory_obs(
                "browserorg.filter_param_has_value",
                _pv("ok" if has_value is not None else "unreadable", "Integer",
                    int(has_value) if has_value is not None else None, display=str(has_value)),
                category,
            )

        # Walk the folder tree from org.Id as root
        _walk_tree(org, org_id_int, doc, bip_lookup, org_id_int, category)

    _add_inventory_obs(
        "browserorg.name_fallback_used_count",
        _pv("ok", "Integer", _name_fallback_used_count,
            display="FolderItemInfo.Name returned '???' this many times; resolved via "
                    "Definition.Name / BuiltInParameter label fallback instead."),
        "doc",
    )
    _add_inventory_obs(
        "browserorg.folder_items_walked_count",
        _pv("ok", "Integer", len(_folder_item_records)),
        "doc",
    )
else:
    _add_inventory_obs("doc.browserorganization_import", _pv("unsupported", "None", None, display="BrowserOrganization import failed"), "doc")

param_inventory = []
for k in sorted(param_index.keys()):
    e = param_index[k]
    param_inventory.append({
        "domain": "browser_organization",
        "param_key": k,
        "example": e["example"],
        "observed": {
            "storage_types": sorted(e["storage_types"]),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(e["observed_on_buckets"]),
        },
    })

optional_crosswalk = _folder_item_records

# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection,
# run over BOTH BrowserOrganization instances (one per category, up to 3)
# and every FolderItemInfo encountered during the walk. Identical engine
# to every other domain probe (copied verbatim, not imported -- each
# probe_*.py must remain a self-contained, paste-able Dynamo Python node).
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
            # on the allowlist above (see module-level design rationale in
            # every other probe_*.py).
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


_reflection_records = (
    _run_reflection_sweep(_browserorg_objs, "BrowserOrganization", "browser_organization")
    + _run_reflection_sweep(_all_folder_items, "FolderItemInfo", "browser_organization")
)

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "browser_organization",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "browser_organization",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "browser_organization",
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
            json.dump(_probe_wrap("browser_organization", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

# Dynamo Python (Revit) — Breadth Probe: view_filter_definitions (INVENTORY OUTPUT)
#
# DOMAIN = "view_filter_definitions"
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "view_filter_definitions",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "view_filter_definitions",
#     "records": optional_crosswalk         # ParameterFilterElement -> applying views/templates
#   }
# ]
#
# Inputs:
#   IN[0] max_filters_to_inspect (int)
#        Maximum number of ParameterFilterElement instances to inspect.
#        Default: 500
#
#   IN[1] per_category_sig_limit (int)
#        Sample at most N filters per distinct category-signature bucket
#        (sorted category ids). Default: 5
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
#   IN[4] max_rules_to_read_per_filter (int)
#        Hard safety cap on number of rules flattened per filter.
#        Default: 200
#
#   IN[5] enable_crosswalk (bool)
#        Whether to emit a ParameterFilterElement -> applying-views/templates
#        crosswalk (one row per filter: how many live views and how many
#        view templates have it in View.GetFilters(), plus a small name
#        sample). Same View.GetFilters() call probe_view_filter_applications.py
#        already uses, applied from the filter's side instead of the view's
#        side -- that probe's crosswalk tells you what a given view applies;
#        this one tells you whether a given filter is used by anything at
#        all (orphan/adoption signal). Appended as a new position rather
#        than inserted earlier so existing positional callers don't shift.
#        Default: False
#
#   IN[6] max_views_to_scan (int)
#        When crosswalk enabled, scan at most N views+templates for applied
#        filters. Default: 2000
#
# Reference pattern: probe_arrowheads.py :contentReference[oaicite:0]{index=0}


import clr
import os
import json
import hashlib
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector,
    ElementId,
    StorageType,
    UnitUtils,
    UnitTypeId,
    BuiltInParameter,
    Category,
    ParameterFilterElement,
    LogicalAndFilter,
    LogicalOrFilter,
    ElementParameterFilter,
    View
)

doc = DocumentManager.Instance.CurrentDBDocument

max_filters_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
per_category_sig_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 5
write_json = IN[2] if len(IN) > 2 and IN[2] is not None else False
out_path = IN[3] if len(IN) > 3 and IN[3] is not None else None
max_rules_to_read_per_filter = IN[4] if len(IN) > 4 and IN[4] is not None else 200
enable_crosswalk = IN[5] if len(IN) > 5 and IN[5] is not None else False
max_views_to_scan = IN[6] if len(IN) > 6 and IN[6] is not None else 2000


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _sha1(s):
    try:
        b = s.encode("utf-8")
        return hashlib.sha1(b).hexdigest()
    except:
        return None

def _as_param_payload(q, storage, raw, display, norm):
    return {
        "q": q,
        "storage": storage,
        "raw": raw,
        "display": display,
        "norm": norm
    }

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

def _observe(param_index, param_key, pv, bucket_label):
    if param_key not in param_index:
        param_index[param_key] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set()
        }

    entry = param_index[param_key]
    q = pv.get("q") or "unreadable"
    st = pv.get("storage")

    if st is not None:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    if bucket_label:
        entry["observed_on_buckets"].add(bucket_label)

    _maybe_set_example(entry, pv)

def _bucket_label_from_categories(cat_ids_sorted):
    # Stable breadth bucket label
    if not cat_ids_sorted:
        return "0|<none>"
    try:
        return "{}|{}".format(len(cat_ids_sorted), "|".join([str(i) for i in cat_ids_sorted[:25]]))
    except:
        return "{}|<unreadable>".format(len(cat_ids_sorted) if cat_ids_sorted else 0)

def _resolve_category_name(cat_id_int):
    # Category ids for view filters are category ids, not elements; use Category.GetCategory if possible.
    try:
        cat = Category.GetCategory(doc, ElementId(int(cat_id_int)))
        return _safe(lambda: cat.Name, None) if cat is not None else None
    except:
        return None


# -------------------------
# Filter-rule flattening (best-effort, version-tolerant)
# -------------------------

def _element_filter_kind(ef):
    if ef is None:
        return None
    try:
        return ef.GetType().Name
    except:
        return None

def _get_subfilters(ef):
    # LogicalAndFilter / LogicalOrFilter support GetFilters()
    if ef is None:
        return []
    try:
        if isinstance(ef, LogicalAndFilter) or isinstance(ef, LogicalOrFilter):
            subs = _safe(lambda: list(ef.GetFilters()), default=[])
            return subs if subs else []
    except:
        pass
    return []

def _get_rules_from_element_parameter_filter(epf):
    # ElementParameterFilter supports GetRules() in most modern APIs; fallback to reflection-style access.
    if epf is None:
        return []
    rules = _safe(lambda: list(epf.GetRules()), default=None)
    if rules is not None:
        return rules
    # fallback: try property names that sometimes exist
    for attr in ("Rules", "GetRules", "GetElementFilterRules"):
        try:
            v = getattr(epf, attr)
            if callable(v):
                rr = v()
                return list(rr) if rr is not None else []
            return list(v) if v is not None else []
        except:
            continue
    return []

def _rule_parameter_id(rule):
    # Try common methods/properties
    for attr in ("GetRuleParameter", "ParameterId", "GetParameterId"):
        try:
            v = getattr(rule, attr)
            if callable(v):
                pid = v()
            else:
                pid = v
            if isinstance(pid, ElementId):
                return _safe(lambda: pid.IntegerValue, None)
            # sometimes already an int
            if pid is not None:
                return int(pid)
        except:
            continue
    return None

def _rule_evaluator_name(rule):
    # Not always accessible; attempt best-effort.
    for attr in ("GetEvaluator", "Evaluator"):
        try:
            v = getattr(rule, attr)
            ev = v() if callable(v) else v
            if ev is None:
                continue
            return _safe(lambda: ev.GetType().Name, None) or _safe(lambda: str(ev), None)
        except:
            continue
    return None

def _rule_value_best_effort(rule):
    # Many rule types differ; attempt common properties first; else string fallback.
    for attr in ("RuleString", "StringValue", "RuleValue", "Value", "DoubleValue", "IntegerValue"):
        try:
            v = getattr(rule, attr)
            vv = v() if callable(v) else v
            if vv is not None:
                return vv
        except:
            continue
    # final fallback: string form
    return _safe(lambda: str(rule), None)

def _flatten_element_filter(ef, hard_cap):
    """
    Returns:
      logic: "and" | "or" | "single" | "unknown"
      rules: list of dicts (best-effort)
    """
    if ef is None:
        return ("missing", [])

    kind = _element_filter_kind(ef)

    # Logical container?
    if isinstance(ef, LogicalAndFilter):
        logic = "and"
        rules_out = []
        for sub in _get_subfilters(ef):
            lg, rr = _flatten_element_filter(sub, hard_cap)
            for r in rr:
                if len(rules_out) >= hard_cap:
                    break
                rules_out.append(r)
            if len(rules_out) >= hard_cap:
                break
        return (logic, rules_out)

    if isinstance(ef, LogicalOrFilter):
        logic = "or"
        rules_out = []
        for sub in _get_subfilters(ef):
            lg, rr = _flatten_element_filter(sub, hard_cap)
            for r in rr:
                if len(rules_out) >= hard_cap:
                    break
                rules_out.append(r)
            if len(rules_out) >= hard_cap:
                break
        return (logic, rules_out)

    # Parameter filter leaf?
    if isinstance(ef, ElementParameterFilter):
        rules = _get_rules_from_element_parameter_filter(ef)
        rules_out = []
        for rule in rules:
            if len(rules_out) >= hard_cap:
                break
            rtype = _safe(lambda: rule.GetType().Name, None)
            pid = _rule_parameter_id(rule)
            ev = _rule_evaluator_name(rule)
            val = _rule_value_best_effort(rule)
            rules_out.append({
                "rule.type": rtype,
                "rule.param_id": pid,
                "rule.evaluator": ev,
                "rule.value": val
            })
        return ("single", rules_out)

    # Unknown leaf type — keep kind for diagnostics, no rules
    return ("unknown:{}".format(kind), [])


# -------------------------
# Discovery + Sampling
# -------------------------

filters = _safe(
    lambda: list(FilteredElementCollector(doc).OfClass(ParameterFilterElement).ToElements()),
    default=[]
)

# Cap collector list early (then apply bucketing) to avoid pathological docs
try:
    mf = int(max_filters_to_inspect)
    if mf >= 0:
        filters = filters[:mf]
except:
    pass

selected = []
bucket_counts = {}  # category_sig -> count

for f in filters:
    cat_ids = _safe(lambda: list(f.GetCategories()), default=[])
    cat_ints = []
    for cid in cat_ids:
        try:
            if isinstance(cid, ElementId):
                cat_ints.append(int(cid.IntegerValue))
            else:
                cat_ints.append(int(cid))
        except:
            continue
    cat_ints_sorted = sorted(list(set(cat_ints)))
    cat_sig = "|".join([str(i) for i in cat_ints_sorted])

    c = bucket_counts.get(cat_sig, 0)

    if per_category_sig_limit is None:
        ok = True
    else:
        try:
            ok = c < int(per_category_sig_limit)
        except:
            ok = c < 5

    if ok:
        selected.append(f)
        bucket_counts[cat_sig] = c + 1

# Fallback: if bucketing excluded everything, take first few
if len(selected) == 0 and len(filters) > 0:
    selected = filters[:min(25, len(filters))]


# -------------------------
# Build inventory (synthetic param surface over selected filters)
# -------------------------

param_index = {}

for f in selected:
    # Bucket label for breadth
    cat_ids = _safe(lambda: list(f.GetCategories()), default=[])
    cat_ints = []
    for cid in cat_ids:
        try:
            cat_ints.append(int(cid.IntegerValue) if isinstance(cid, ElementId) else int(cid))
        except:
            continue
    cat_ints_sorted = sorted(list(set(cat_ints)))
    bucket_label = _bucket_label_from_categories(cat_ints_sorted)

    # vfd.id
    fid = _safe(lambda: f.Id.IntegerValue, None)
    _observe(param_index, "v.filter.id", _as_param_payload("ok", "Integer", fid, str(fid) if fid is not None else None, fid), bucket_label)

    # vfd.name (ParameterFilterElement.Name)
    nm = _safe(lambda: f.Name, None)
    if nm is None:
        _observe(param_index, "v.filter.name", _as_param_payload("missing", "String", None, None, None), bucket_label)
    else:
        _observe(param_index, "v.filter.name", _as_param_payload("ok", "String", nm, nm, nm), bucket_label)

    # vfd.categories.ids (stable string norm)
    if cat_ints_sorted is None:
        _observe(param_index, "v.filter.category_ids", _as_param_payload("unreadable", "String", None, None, None), bucket_label)
    else:
        raw_ids = cat_ints_sorted
        norm_ids = "|".join([str(i) for i in raw_ids])
        _observe(
            param_index,
            "v.filter.category_ids",
            _as_param_payload("ok", "String", raw_ids, norm_ids, norm_ids),
            bucket_label
        )

    # vfd.categories.names (best-effort)
    cat_names = []
    for ci in cat_ints_sorted:
        n = _resolve_category_name(ci)
        if n:
            cat_names.append(n)
    cat_names_sorted = sorted(list(set(cat_names))) if cat_names else []
    disp_names = "|".join(cat_names_sorted) if cat_names_sorted else None
    if disp_names is None:
        _observe(param_index, "v.filter.category_names", _as_param_payload("missing", "String", None, None, None), bucket_label)
    else:
        _observe(param_index, "v.filter.category_names", _as_param_payload("ok", "String", cat_names_sorted, disp_names, disp_names), bucket_label)

    # vfd.category_count
    cc = len(cat_ints_sorted) if cat_ints_sorted is not None else None
    if cc is None:
        _observe(param_index, "v.filter.category_count", _as_param_payload("unreadable", "Integer", None, None, None), bucket_label)
    else:
        _observe(param_index, "v.filter.category_count", _as_param_payload("ok", "Integer", cc, str(cc), cc), bucket_label)

    # vfd.logic + rules (flatten element filter)
    ef = _safe(lambda: f.GetElementFilter(), default=None)
    logic, rules = _flatten_element_filter(ef, int(max_rules_to_read_per_filter) if max_rules_to_read_per_filter is not None else 200)

    _observe(param_index, "v.filter.logic", _as_param_payload("ok", "String", logic, logic, logic), bucket_label)

    # vfd.rule_count
    rc = len(rules) if rules is not None else 0
    _observe(param_index, "v.filter.rule_count", _as_param_payload("ok", "Integer", rc, str(rc), rc), bucket_label)

    # vfd.rule_types (set -> stable string)
    rtypes = []
    if rules:
        for r in rules:
            rt = r.get("rule.type")
            if rt:
                rtypes.append(rt)
    rtypes_sorted = sorted(list(set(rtypes)))

    # zero-rule filters are a valid state, not "missing"
    if rc == 0:
        _observe(
            param_index,
            "v.filter.rule_types",
            _as_param_payload("ok", "String", "", "", ""),
            bucket_label
        )
    else:
        rtypes_disp = "|".join(rtypes_sorted) if rtypes_sorted else ""
        _observe(
            param_index,
            "v.filter.rule_types",
            _as_param_payload("ok", "String", rtypes_disp, rtypes_disp, rtypes_disp),
            bucket_label
        )

    # vfd.rule_param_ids (unique)
    rpids = []
    if rules:
        for r in rules:
            pid = r.get("rule.param_id")
            if pid is not None:
                try:
                    rpids.append(int(pid))
                except:
                    continue
    rpids_sorted = sorted(list(set(rpids)))

    # zero-rule filters are a valid state, not "missing"
    if rc == 0:
        _observe(
            param_index,
            "v.filter.rule_param_ids",
            _as_param_payload("ok", "String", "", "", ""),
            bucket_label
        )
    else:
        rpids_disp = "|".join([str(i) for i in rpids_sorted]) if rpids_sorted else ""
        _observe(
            param_index,
            "v.filter.rule_param_ids",
            _as_param_payload("ok", "String", rpids_disp, rpids_disp, rpids_disp),
            bucket_label
        )

    # vfd.rule_sig_hash (join-key candidate; stable signature over rules)
    # Signature uses: rule.type, rule.param_id, rule.evaluator, stringified rule.value
    sig_parts = []
    if rules:
        for r in rules:
            rt = r.get("rule.type")
            pid = r.get("rule.param_id")
            ev = r.get("rule.evaluator")
            vv = r.get("rule.value")
            sig_parts.append("{}|{}|{}|{}".format(
                str(rt) if rt is not None else "",
                str(pid) if pid is not None else "",
                str(ev) if ev is not None else "",
                str(vv) if vv is not None else ""
            ))
    sig_text = "||".join(sig_parts)
    sig_hash = _sha1(sig_text) if sig_text is not None else None
    if sig_hash is None:
        _observe(param_index, "v.filter.rule_sig_hash", _as_param_payload("unreadable", "String", None, None, None), bucket_label)
    else:
        _observe(param_index, "v.filter.rule_sig_hash", _as_param_payload("ok", "String", sig_hash, sig_hash, sig_hash), bucket_label)


# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "view_filter_definitions",
        "param_key": pk,
        "selected_filter_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
        }
    })


# -------------------------
# Assemble labeled output payload
# -------------------------


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

_METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
# see the identity check in _reflect_contract below for why this must never be
# comparable-by-value to a real Revit return.

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
            return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
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
    if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
        # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
        # unique object(), never a string, specifically so a genuine reflected
        # property or allowlisted-method return whose real value happens to be
        # the literal text "<method not invoked>" cannot collide with this
        # placeholder and get misclassified/dropped (flagged in PR #398 review:
        # an earlier version of this check compared by value against a string
        # constant, which had exactly that collision risk). Checked before
        # isinstance(raw_v, str) specifically so it never reaches that branch.
        return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
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
        saw_item = False
        for item in raw_v:
            saw_item = True
            if not hasattr(item, "IntegerValue"):
                raise TypeError("non-ElementId item in collection")
            ids.append(int(item.IntegerValue))
        if not saw_item:
            # An empty collection is vacuously "every item has .IntegerValue"
            # -- there's nothing to fail the check against, so item-by-item
            # duck-typing alone can never tell an empty ElementId collection
            # (GetMonitoredLinkElementIds returning [] because a type has no
            # monitored links) apart from an empty collection of anything
            # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
            # IList<Subelement>, both returning [] because that instance
            # happens to have zero). A CLR generic-type reflection check
            # (raw_v.GetType().GetGenericArguments()) was tried here and
            # found not to reliably discriminate types against a live
            # Revit/pythonnet session (still produced the same false
            # positives), so it was dropped rather than kept as an
            # unreliable safety net. Per this project's fail-soft principle
            # (never silently collapse distinct states), an empty collection
            # of unconfirmed item type gets its own explicit q value instead
            # of defaulting to "ok" (would reintroduce this exact bug) or
            # bare "unsupported" (would make it indistinguishable from a
            # totally opaque complex-object failure). storage stays "None"
            # (not "ElementIdList") so find_crosswalk_candidates.py's
            # _is_elementid_typed() correctly does not treat this as a
            # reference candidate.
            return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
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
                    "type_label": type_label, "example": None, "example_error": None,
                    "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
                }
            e = idx[key]
            if not ok:
                e["error_count"] += 1
                if e["example_error"] is None and err:
                    e["example_error"] = err
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
            "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
            "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
        })
    return records

_reflection_records_0 = _run_reflection_sweep(selected, "ParameterFilterElement", "view_filter_definitions")
_reflection_records = _reflection_records_0

# -------------------------
# Crosswalk (optional): ParameterFilterElement -> applying views/templates.
# Reuses the exact View.GetFilters() call probe_view_filter_applications.py
# already uses, from the filter's side instead of the view's side. One row
# per discovered filter (from `filters`, the full pre-bucket-sample list --
# every definition gets a crosswalk row, not just the reflection-sampled
# subset), aggregated usage counts rather than a raw per-view join, since
# "is this filter used by anything" is the directly useful governance
# question (orphan/purge-candidate signal).
# -------------------------

filter_usage = {}  # filter_id_int -> {"live_view_count": int, "template_count": int, "sample_names": [...]}
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
        # Guarded: some view types can throw on GetFilters (same guard
        # probe_view_filter_applications.py uses).
        fids = _safe(lambda: list(v.GetFilters()), default=None)
        if not fids:
            continue
        vname = _safe(lambda: v.Name, None)
        for fid_obj in fids:
            fid = _safe(lambda: fid_obj.IntegerValue, None)
            if fid is None:
                continue
            entry = filter_usage.setdefault(fid, {"live_view_count": 0, "template_count": 0, "sample_names": []})
            if is_template:
                entry["template_count"] += 1
            else:
                entry["live_view_count"] += 1
            if vname and len(entry["sample_names"]) < 5:
                entry["sample_names"].append(vname)

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


for f in filters:
    fid = _safe(lambda: f.Id.IntegerValue, None)
    if fid is None:
        continue
    fname = _safe(lambda: f.Name, None)
    f_ws_id_obj = _safe(lambda: f.WorksetId, None)
    f_ws_name, _f_ws_resolved = _resolve_workset(doc, f_ws_id_obj)
    f_ws_id_int = _safe(lambda: f_ws_id_obj.IntegerValue, None) if f_ws_id_obj is not None else None
    usage = filter_usage.get(fid, {"live_view_count": 0, "template_count": 0, "sample_names": []})
    total = usage["live_view_count"] + usage["template_count"]
    optional_crosswalk.append({
        "filter.id": fid,
        "filter.name": fname,
        "filter.workset_id": f_ws_id_int,
        "filter.workset_name": f_ws_name,
        "filter.is_applied_anywhere": total > 0,
        "applied_live_view_count": usage["live_view_count"],
        "applied_template_count": usage["template_count"],
        "sample_applied_names": usage["sample_names"],
    })

OUT_payload = [
    {
        "kind": "reflection",
        "domain": "view_filter_definitions",
        "records": _reflection_records
    },
    {
        "kind": "inventory",
        "domain": "view_filter_definitions",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "view_filter_definitions",
        "records": optional_crosswalk
    }
]


# -------------------------
# Optional: write to JSON
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
            json.dump(_probe_wrap("view_filter_definitions", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload

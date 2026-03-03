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
#     "records": []                         # no natural crosswalk emitted here
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
#        Filename is fixed as: probe_view_filter_definitions_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.
#
#   IN[4] max_rules_to_read_per_filter (int)
#        Hard safety cap on number of rules flattened per filter.
#        Default: 200
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
    ElementParameterFilter
)

doc = DocumentManager.Instance.CurrentDBDocument

max_filters_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
per_category_sig_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 5
write_json = IN[2] if len(IN) > 2 and IN[2] is not None else False
out_path = IN[3] if len(IN) > 3 and IN[3] is not None else None
max_rules_to_read_per_filter = IN[4] if len(IN) > 4 and IN[4] is not None else 200


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

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "view_filter_definitions",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "view_filter_definitions",
        "records": []
    }
]


# -------------------------
# Optional: write to JSON
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
        fixed_name = "probe_view_filter_definitions_{}.json".format(date_stamp)

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
